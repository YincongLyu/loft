#include "transform_manager.h"
#include "binlog.h"

#include "format/ddl_generated.h"
#include "format/dml_generated.h"

#include "events/control_events.h"
#include "events/rows_event.h"
#include "events/statement_events.h"
#include "events/write_event.h"

#include "utils/base64.h"
#include "sql/mysql_fields.h"

#include "common/logging.h"
#include "common/macros.h"
#include "data_handler.h"

#include <ctime>
#include <sstream>
#include <stdexcept>

#include <functional>
#include <fstream>
#include <iostream>
#include <map>

using pib = std::pair<int, bool>;

// ss >> std::get_time(&timeStruct, "%Y-%m-%d %H:%M:%S.%f");
// get_time 不支持 解析微妙，只能到 秒级精度，故 拆开成两部分
// - std::mktime：默认会将时间解析为本地时间，这样会导致时区偏移问题
// - timegm：这个函数会将时间解释为 UTC
static inline uint64 _stringToTimestamp(const std::string &timeString)
{
  std::tm     timeStruct = {};
  std::string datetimePart, microsecondPart;

  // 分离日期和微秒部分
  size_t dotPos = timeString.find('.');
  if (dotPos != std::string::npos) {
    datetimePart    = timeString.substr(0, dotPos);
    microsecondPart = timeString.substr(dotPos + 1);
  } else {
    datetimePart    = timeString;
    microsecondPart = "0";
  }

  // 解析日期时间部分
  std::istringstream ss(datetimePart);
  ss >> std::get_time(&timeStruct, "%Y-%m-%d %H:%M:%S");

  if (ss.fail()) {
    throw std::runtime_error("Failed to parse time string");
  }

  // 使用 timegm 将 tm 转换为 UTC 时间戳
  time_t timeEpoch      = timegm(&timeStruct);
  auto   timeSinceEpoch = std::chrono::system_clock::from_time_t(timeEpoch);

  // 减去 8 小时的偏移
  timeSinceEpoch -= std::chrono::hours(8);

  // 转换微秒部分
  int  microseconds              = std::stoi(microsecondPart);
  auto additionalMicroseconds    = std::chrono::microseconds(microseconds);
  auto timePointWithMicroseconds = timeSinceEpoch + additionalMicroseconds;

  // 转换为自 epoch 起的微秒数
  return std::chrono::duration_cast<std::chrono::microseconds>(timePointWithMicroseconds.time_since_epoch()).count();
}

void LogFormatTransformManager::transformDDL(const DDL *ddl, MYSQL_BIN_LOG *binLog)
{

  auto ddlType = ddl->ddl_type();

  auto dbName = ddl->db_name();
  auto ddlSql = ddl->ddl_sql();

  auto immediateCommitTs = ddl->msg_time();
  auto originalCommitTs  = ddl->tx_time();

  // 1. 构造 GTID event
  auto lastCommit = ddl->last_commit();
  auto txSeq      = ddl->tx_seq();

  auto i_ts = _stringToTimestamp(immediateCommitTs->c_str());
  auto o_ts = _stringToTimestamp(originalCommitTs->c_str());

  auto gtidEvent = std::make_unique<Gtid_event>(
      lastCommit, txSeq, true, o_ts, i_ts, ORIGINAL_SERVER_VERSION, IMMEDIATE_SERVER_VERSION);

  // 2. 构造 Query event
  const char *query_arg   = ddlSql->data();
  const char *catalog_arg = nullptr;
  const char *db_arg      = nullptr;
  if (dbName != nullptr) {
    db_arg = dbName->c_str();
  }
  catalog_arg = db_arg;  // binlog v4里，catalog_name 会初始化为 0，但要和 db_name 一样

  uint32_t query_length = strlen(query_arg);
  LOG_INFO("query_: %s, query_len: %d", query_arg, query_length);

  uint64 thread_id_arg                = THREAD_ID;
  uint32 sql_mode_arg                 = 0;  // 随意
  uint32 auto_increment_increment_arg = 0;  // 随意
  uint32 auto_increment_offset_arg    = 0;  // 随意
  uint32 number                       = 0;  // 一定要
  uint64 table_map_for_update_arg     = 0;  // 随意
  int    errcode                      = ERROR_CODE;

  auto queryEvent = std::make_unique<Query_event>(query_arg,
      catalog_arg,
      db_arg,
      txSeq,
      query_length,
      thread_id_arg,
      sql_mode_arg,
      auto_increment_increment_arg,
      auto_increment_offset_arg,
      number,
      table_map_for_update_arg,
      errcode,
      i_ts);

  // ******* print debug info **************************
  if (ddlType == nullptr) {  // drop db
    LOG_INFO("sql_type: drop db | create/drop procedure/function");
  } else {
    std::string sql_type = ddlType->c_str();
    if (sql_type == "CREATE TABLE") {
      if (db_arg == nullptr) {  // create db
        LOG_INFO("sql_type: create db");
      } else {  // create table
        LOG_INFO("sql_type: create table");
      }
    } else if (sql_type == "DROP TABLE") {
      LOG_INFO("sql_type: drop table");
    }
  }
  // ******************************************************************

  binLog->write_event_to_binlog(gtidEvent.get());
  binLog->write_event_to_binlog(queryEvent.get());
}

inline enum_field_types LogFormatTransformManager::ConvertStringType(std::string_view type_str)
{
  auto it = type_map.find(type_str);

  if (it != type_map.end()) {
    return it->second;
  } else {
    return MYSQL_TYPE_INVALID;
  }
}

void LogFormatTransformManager::processRowData(const ::flatbuffers::Vector<::flatbuffers::Offset<loft::kvPair>> &fields,
    Rows_event *row, const std::unordered_map<std::string, int> &field_map,
    const std::vector<mysql::FieldRef> &field_vec, bool is_before)
{
  row->setBefore(is_before);

  std::map<int, bool>           rows_pair;
  std::map<int, const kvPair *> dataMap;  // 存不为 null 的 kvPair

  // 收集字段信息
  for (auto item : fields) {
    int field_idx        = field_map.at(item->key()->c_str());
    rows_pair[field_idx] = (item->value_type() == DataMeta_NONE);
    if (item->value_type() != DataMeta_NONE) {
      dataMap[field_idx] = item;
    }
  }

  // 设置null标记
  std::vector<int>  rows;
  std::vector<bool> rows_null;
  for (const auto &[idx, flag] : rows_pair) {
    rows.push_back(idx);
    rows_null.push_back(flag);
  }

  if (is_before) {
    row->set_rows_before(rows);
    row->set_null_before(rows_null);
  } else {
    row->set_rows_after(rows);
    row->set_null_after(rows_null);
  }

  // 处理数据
  for (const auto &[idx, kv] : dataMap) {
    if (auto handler = DataHandlerFactory::getHandler(kv->value_type())) {
      handler->processData(kv, field_vec[idx - 1].get(), row);
    }
  }
}

void LogFormatTransformManager::transformDML(const DML *dml, MYSQL_BIN_LOG *binLog)
{
  //////////**************** gtid event start ***************************

  auto lastCommit        = dml->last_commit();
  auto txSeq             = dml->tx_seq();
  auto immediateCommitTs = dml->msg_time();
  auto originalCommitTs  = dml->tx_time();
  auto i_ts              = _stringToTimestamp(immediateCommitTs->c_str());
  auto o_ts              = _stringToTimestamp(originalCommitTs->c_str());

  auto ge = std::make_unique<Gtid_event>(
      lastCommit, txSeq, true, o_ts, i_ts, ORIGINAL_SERVER_VERSION, IMMEDIATE_SERVER_VERSION);

  //////////****************** gtid event end *******************************

  //////////****************** query event start ****************************
  const char *query_arg   = DML_QUERY_STR;  // row-based 的 DML 固定内容是 BEGIN
  auto        dbName      = dml->db_name();
  const char *db_arg      = dbName->c_str();
  const char *catalog_arg = db_arg;  // 在binlog v4中，目录名称通常被设置为与事件相关的数据库的名称

  uint32_t query_length = strlen(query_arg);
  LOG_INFO("query_: %s, query_len: %d", query_arg, query_length);

  unsigned long      thread_id_arg                = 10000;
  unsigned long long sql_mode_arg                 = 0;  // 随意
  unsigned long      auto_increment_increment_arg = 1;  // 随意
  unsigned long      auto_increment_offset_arg    = 1;  // 随意
  unsigned int       number                       = 0;  // 0 means 'en-US'
  unsigned long long table_map_for_update_arg     = 0;  // 随意
  int                errcode                      = 0;

  auto qe = std::make_unique<Query_event>(query_arg,
      catalog_arg,
      db_arg,
      INVALID_XID,
      query_length,
      thread_id_arg,
      sql_mode_arg,
      auto_increment_increment_arg,
      auto_increment_offset_arg,
      number,
      table_map_for_update_arg,
      errcode,
      i_ts);
  //////////****************** query event end ******************************

  //////////****************** table map event start ************************

  auto        table   = dml->table_();
  const char *tbl_arg = table->c_str();
  auto        fields  = dml->fields();

  std::unordered_map<std::string, int> field_map;  // [field_name, field_idx]
  std::vector<mysql::FieldRef>         field_vec;
  size_t                               null_bit = 0;
  //    TYPELIB *interval = new TYPELIB;
  int interval_count = 0;
  int fieldIdx       = 0;  // 下标
  for (auto field : *fields) {
    auto field_name   = field->name();
    auto fieldMeta    = field->meta();
    auto field_length = fieldMeta->length();
    bool is_unsigned  = fieldMeta->is_unsigned();
    bool is_nullable  = fieldMeta->nullable();
    auto data_type    = fieldMeta->data_type();  // 根据 这里的类型，构建 对应的 Field 对象
    auto decimals     = fieldMeta->precision();

    enum_field_types field_type = ConvertStringType(data_type->c_str());
    if (field_type == MYSQL_TYPE_ENUM || field_type == MYSQL_TYPE_SET) {
      interval_count = field_length;
    }
    if (is_nullable) {
      null_bit = fieldIdx;
    }
    // 工厂函数
    auto field_obj = mysql::make_field(
        field_name->c_str(), field_length, is_unsigned, is_nullable, null_bit, field_type, interval_count, decimals);

    field_vec.emplace_back(field_obj);
    field_map.insert({field_name->c_str(), ++fieldIdx});
  }
  // TODO 需要根据 create table 时，记录 table_id, 这是全局的
  Table_id      tid(DML_TABLE_ID);  // 暂时随便写一个，实际上要做一个 连续的 id 分配器
  unsigned long colcnt = field_vec.size();
  // field_vec 内部的元素是共享的
  auto table_map_event =
      std::make_unique<Table_map_event>(tid, colcnt, db_arg, strlen(db_arg), tbl_arg, strlen(tbl_arg), field_vec, i_ts);

  LOG_INFO("construct table map event end...");

  //////////****************** table map event end *************************

  //////////****************** rows event start ****************************
  auto           opType    = dml->op_type();
  Log_event_type rows_type = UNKNOWN_EVENT;
  if (strcmp(opType->c_str(), "I") == 0) {
    LOG_INFO("INSERT sql");
    rows_type = Log_event_type::WRITE_ROWS_EVENT;
  } else if (strcmp(opType->c_str(), "U") == 0) {
    LOG_INFO("UPDATE sql");
    rows_type = Log_event_type::UPDATE_ROWS_EVENT;
  } else if (strcmp(opType->c_str(), "D") == 0) {
    LOG_INFO("DELETE sql");
    rows_type = Log_event_type::DELETE_ROWS_EVENT;
  } else {
    LOG_ERROR("unknown opType: %s", opType->c_str());
  }

  auto row = std::make_unique<Rows_event>(tid, colcnt, 1, rows_type, i_ts);  // 初始化 一个 rows_event 对象

  if (auto keys = dml->keys()) {
    processRowData(*keys, row.get(), field_map, field_vec, true);
  }
  if (auto newData = dml->new_data()) {
    processRowData(*newData, row.get(), field_map, field_vec, false);
  }

  //////////****************** rows event end ****************************

  //////////****************** xid event start ******************************

  auto xe = std::make_unique<Xid_event>(txSeq, i_ts);
  LOG_INFO("construct xid event end...");

  //////////****************** xid event end ******************************

  binLog->write_event_to_binlog(ge.get());
  binLog->write_event_to_binlog(qe.get());
  binLog->write_event_to_binlog(table_map_event.get());
  binLog->write_event_to_binlog(row.get());
  binLog->write_event_to_binlog(xe.get());
}
