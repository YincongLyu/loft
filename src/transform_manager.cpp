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

inline uint64_t LogFormatTransformManager::stringToTimestamp(const std::string& timeString) {
  std::tm timeStruct = {};

  // 直接使用指针操作，避免字符串拷贝
  const char* str = timeString.c_str();
  const char* p = str;

  // 直接解析年月日时分秒，避免使用istringstream
  timeStruct.tm_year = (p[0] - '0') * 1000 + (p[1] - '0') * 100 +
                       (p[2] - '0') * 10 + (p[3] - '0') - 1900;
  timeStruct.tm_mon = (p[5] - '0') * 10 + (p[6] - '0') - 1;
  timeStruct.tm_mday = (p[8] - '0') * 10 + (p[9] - '0');
  timeStruct.tm_hour = (p[11] - '0') * 10 + (p[12] - '0');
  timeStruct.tm_min = (p[14] - '0') * 10 + (p[15] - '0');
  timeStruct.tm_sec = (p[17] - '0') * 10 + (p[18] - '0');

  // 检查格式是否正确
  if (p[4] != '-' || p[7] != '-' || p[10] != ' ' ||
      p[13] != ':' || p[16] != ':') {
    throw std::runtime_error("Invalid time format");
  }

  // 解析微秒部分
  int microseconds = 0;
  if (timeString.length() > 19 && p[19] == '.') {
    p += 20;  // 移到小数点后第一位
    int multiplier = 100000;  // 从最高位开始
    while (*p >= '0' && *p <= '9' && multiplier > 0) {
      microseconds += (*p - '0') * multiplier;
      multiplier /= 10;
      ++p;
    }
  }

  time_t timeEpoch = timegm(&timeStruct);
  auto timeSinceEpoch = std::chrono::system_clock::from_time_t(timeEpoch);

  // 减去8小时偏移
  timeSinceEpoch -= std::chrono::hours(8);

  // 添加微秒
  auto timePointWithMicroseconds = timeSinceEpoch + std::chrono::microseconds(microseconds);

  return std::chrono::duration_cast<std::chrono::microseconds>(
      timePointWithMicroseconds.time_since_epoch()).count();
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

  auto i_ts = stringToTimestamp(immediateCommitTs->c_str());
  auto o_ts = stringToTimestamp(originalCommitTs->c_str());

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

void LogFormatTransformManager::processRowData(
    const ::flatbuffers::Vector<::flatbuffers::Offset<loft::kvPair>>& data,
    Rows_event* row,
    const std::unordered_map<std::string, int>& field_map,
    const std::vector<mysql::FieldRef>& field_vec,
    bool is_before) {

  row->setBefore(is_before);

  // 使用数组来收集数据，下标对应field的序号
  const size_t max_field_size = field_vec.size() + 1; // +1因为field_idx从1开始
  std::vector<uint8> field_present(max_field_size, 0);
  std::vector<int> rows;
  std::vector<uint8> rows_null;
  std::vector<const loft::kvPair*> ordered_data(max_field_size, nullptr);

  // 第一次遍历：将数据放入对应位置
  for (size_t i = 0; i < data.size(); ++i) {
    auto item = data[i];
    int field_idx = field_map.at(item->key()->c_str());
    field_present[field_idx] = 1;
    ordered_data[field_idx] = item;
  }

  // 收集实际存在的字段数据
  rows.reserve(data.size());
  rows_null.reserve(data.size());

  // 按顺序处理存在的字段
  for (size_t field_idx = 1; field_idx < max_field_size; ++field_idx) {
    if (field_present[field_idx]) {
      auto item = ordered_data[field_idx];
      rows.push_back(field_idx);
      bool is_null = (item->value_type() == DataMeta_NONE);
      rows_null.push_back(is_null ? 1 : 0);
    }
  }

  // 设置rows和rows_null
  if (is_before) {
    row->set_rows_before(std::move(rows));
    row->set_null_before(std::move(rows_null));
  } else {
    row->set_rows_after(std::move(rows));
    row->set_null_after(std::move(rows_null));
  }

  // 按顺序处理非空数据
  for (size_t field_idx = 1; field_idx < max_field_size; ++field_idx) {
    if (field_present[field_idx]) {
      auto item = ordered_data[field_idx];
      if (item->value_type() != DataMeta_NONE) {
        if (auto handler = DataHandlerFactory::getHandler(item->value_type())) {
          handler->processData(item, field_vec[field_idx - 1].get(), row);
        }
      }
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
  auto i_ts              = stringToTimestamp(immediateCommitTs->c_str());
  auto o_ts              = stringToTimestamp(originalCommitTs->c_str());

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

std::vector<std::unique_ptr<AbstractEvent>> LogFormatTransformManager::transformDDL(const DDL *ddl)
{

  auto ddlType = ddl->ddl_type();

  auto dbName = ddl->db_name();
  auto ddlSql = ddl->ddl_sql();

  auto immediateCommitTs = ddl->msg_time();
  auto originalCommitTs  = ddl->tx_time();

  // 1. 构造 GTID event
  auto lastCommit = ddl->last_commit();
  auto txSeq      = ddl->tx_seq();

  auto i_ts = stringToTimestamp(immediateCommitTs->c_str());
  auto o_ts = stringToTimestamp(originalCommitTs->c_str());

  std::unique_ptr<AbstractEvent> gtidEvent = std::make_unique<Gtid_event>(
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

  std::vector<std::unique_ptr<AbstractEvent>> events;
  events.push_back(std::move(gtidEvent));
  events.push_back(std::move(queryEvent));
  return events;
}
std::vector<std::unique_ptr<AbstractEvent>> LogFormatTransformManager::transformDML(const DML *dml)
{
  auto lastCommit        = dml->last_commit();
  auto txSeq             = dml->tx_seq();
  auto immediateCommitTs = dml->msg_time();
  auto originalCommitTs  = dml->tx_time();
  auto i_ts              = stringToTimestamp(immediateCommitTs->c_str());
  auto o_ts              = stringToTimestamp(originalCommitTs->c_str());

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
  std::vector<std::unique_ptr<AbstractEvent>> events;
  events.push_back(std::move(ge));
  events.push_back(std::move(qe));
  events.push_back(std::move(table_map_event));
  events.push_back(std::move(row));
  events.push_back(std::move(xe));
  return events;
}
