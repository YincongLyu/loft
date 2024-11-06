#include "file_manager.h"
#include "binlog.h"

#include "control_events.h"
#include "ddl_generated.h"
#include "dml_generated.h"
#include "mysql_fields.h"
#include "rows_event.h"
#include "statement_events.h"

#include "logging.h"
#include "macros.h"

#include <chrono>
#include <ctime>
#include <iomanip>
#include <sstream>
#include <stdexcept>

#include <fstream>
#include <iostream>

namespace loft {
// ss >> std::get_time(&timeStruct, "%Y-%m-%d %H:%M:%S.%f");
// get_time 不支持 解析微妙，只能到 秒级精度，故 拆开成两部分
// - std::mktime：默认会将时间解析为本地时间，这样会导致时区偏移问题
// - timegm：这个函数会将时间解释为 UTC
unsigned long long int stringToTimestamp(const std::string &timeString) {
    std::tm timeStruct = {};
    std::string datetimePart, microsecondPart;

    // Split the datetime string into two parts: "2024-08-01 14:32:41" and
    // "000054"
    size_t dotPos = timeString.find('.');
    if (dotPos != std::string::npos) {
        datetimePart = timeString.substr(0, dotPos);
        microsecondPart = timeString.substr(dotPos + 1);
    } else {
        datetimePart = timeString;
        microsecondPart = "0";
    }

    std::istringstream ss(datetimePart);
    ss >> std::get_time(&timeStruct, "%Y-%m-%d %H:%M:%S");

    if (ss.fail()) {
        throw std::runtime_error("Failed to parse time string");
    }

    // Use timegm to ensure we interpret the time as UTC
    time_t timeEpoch = timegm(&timeStruct);
    auto timeSinceEpoch = std::chrono::system_clock::from_time_t(timeEpoch);

    // Convert microseconds to duration
    int microseconds = std::stoi(microsecondPart);
    auto additionalMicroseconds = std::chrono::microseconds(microseconds);
    auto timePointWithMicroseconds = timeSinceEpoch + additionalMicroseconds;

    // Convert to microseconds
    return std::chrono::duration_cast<std::chrono::microseconds>(
               timePointWithMicroseconds.time_since_epoch()
    )
        .count();
}

LogFormatTransformManager::LogFormatTransformManager(const std::string &filepath
)
    : filepath_(filepath) {
}

auto LogFormatTransformManager::readSQLN(int nowTestCase
) -> std::unique_ptr<char[]> {
    int curSqlStart = nowTestCase * intOffset_;
    for (int i = 0; i < nowTestCase - 1; i++) {
        curSqlStart += loft::SQL_SIZE_ARRAY[i];
    }
    int curSqlLen = loft::SQL_SIZE_ARRAY[nowTestCase - 1];

    std::ifstream file(filepath_, std::ios::binary);
    if (!file) {
        std::cerr << "无法打开文件：" << filepath_ << std::endl;
        return {};
    }

    // 将文件指针移动到指定的偏移量
    file.seekg(curSqlStart, std::ios::beg);

    // 创建一个 unique_ptr 来存储读取的字节
    auto binaryData = std::make_unique<char[]>(curSqlLen);
    // 从当前文件指针位置读取 len 个字节到 buffer 数组
    file.read(binaryData.get(), curSqlLen);

    if (file.gcount() == curSqlLen) {
        return binaryData;
    } else {
        std::cerr << "读取失败或已到文件末尾" << std::endl;
        return {};
    }
}

auto LogFormatTransformManager::readFileAsBinary(const std::string &filePath
) -> std::pair<std::unique_ptr<char[]>, unsigned long long> {
    // std::ios::ate 定位到文件末尾，以便获取文件大小
    std::ifstream file(filePath, std::ios::binary | std::ios::ate);
    if (!file.is_open()) {
        throw std::runtime_error("Failed to open file");
    }

    // 获取文件大小，使用 unsigned long long 确保足够的容量
    // file.tellg() 返回类型是
    // std::streamsize，通常是一个带符号整数类型，具体实现可能为 long 或 long
    // long
    unsigned long long size = static_cast<unsigned long long>(file.tellg());
    if (size == 0) {
        throw std::runtime_error("File is empty");
    }

    file.seekg(0, std::ios::beg); // 回到文件开头

    // 分配缓冲区并读取文件内容
    std::unique_ptr<char[]> buffer(new char[size]);
    if (!file.read(buffer.get(), size)) {
        throw std::runtime_error("Failed to read file");
    }

    // 返回缓冲区和文件大小
    return {std::move(buffer), size};
}

void LogFormatTransformManager::transform(
    const char *data, unsigned long long file_sz
) {
    return;
}

void LogFormatTransformManager::transformDDL(
    const DDL *ddl, MYSQL_BIN_LOG *binLog
) {
    LOFT_ASSERT(
        std::strcmp(ddl->op_type()->c_str(), "DDL") == 0,
        "this is not a ddl sql"
    );

    auto ddlType = ddl->ddl_type();

    auto dbName = ddl->db_name();
    auto ddlSql = ddl->ddl_sql();

    auto immediateCommitTs = ddl->msg_time();
    auto originalCommitTs = ddl->tx_time();

    // 1. 构造 GTID event
    auto lastCommit = ddl->last_commit();
    auto txSeq = ddl->tx_seq();

    unsigned long long int i_ts = stringToTimestamp(immediateCommitTs->c_str());
    unsigned long long int o_ts = stringToTimestamp(originalCommitTs->c_str());

    std::cout << originalCommitTs->c_str() << " " << o_ts << std::endl;

    Gtid_event *ge = new Gtid_event(
        lastCommit, txSeq, true, o_ts, i_ts, original_server_version_,
        immediate_server_version_
    );

    // 2. 构造 Query event
    const char *query_arg = ddlSql->data();
    const char *catalog_arg = nullptr;
    const char *db_arg = nullptr;
    if (dbName != nullptr) {
        db_arg = dbName->c_str();
    }
    catalog_arg =
        db_arg; // binlog v4里，catalog_name 会初始化为 0，但要和 db_name 一样

    uint32_t query_length = strlen(query_arg);
    LOG_INFO("query_: %s, query_len: %d", query_arg, query_length);

    unsigned long thread_id_arg = 10000;
    unsigned long long sql_mode_arg = 0;             // 随意
    unsigned long auto_increment_increment_arg = 0;  // 随意
    unsigned long auto_increment_offset_arg = 0;     // 随意
    unsigned int number = 0;                         // 一定要
    unsigned long long table_map_for_update_arg = 0; // 随意
    int errcode = 0;

    Query_event *qe = new Query_event(
        query_arg, catalog_arg, db_arg, txSeq, query_length, thread_id_arg,
        sql_mode_arg, auto_increment_increment_arg, auto_increment_offset_arg,
        number, table_map_for_update_arg, errcode
    );

    // ******* print debug info **************************
    if (ddlType == nullptr) { // drop db
        LOG_INFO("sql_type: drop db");
    } else {
        std::string sql_type = ddlType->c_str();
        if (sql_type == "CREATE TABLE") {
            if (db_arg == nullptr) { // create db
                LOG_INFO("sql_type: create db");
            } else { // create table
                LOG_INFO("sql_type: create table");
            }
        } else if (sql_type == "DROP TABLE") {
            LOG_INFO("sql_type: drop table");
        }
    }
    // ******************************************************************

    Format_description_event fde(4, "8.0.26");
    binLog->write_event_to_binlog(&fde);

    binLog->write_event_to_binlog(ge);
    binLog->write_event_to_binlog(qe);
}

enum_field_types ConvertStringType(const char *type_str) {
    static const std::unordered_map<std::string, enum_field_types> type_map = {

        {  "SMALLINT",       MYSQL_TYPE_SHORT}, // 2 byte
        {     "SHORT",       MYSQL_TYPE_SHORT}, // 2 byte
        { "MEDIUMINT",       MYSQL_TYPE_INT24}, // 3 byte
        {       "INT",        MYSQL_TYPE_LONG}, // 4 byte
        {    "BIGINT",    MYSQL_TYPE_LONGLONG}, // 8 byte

        {     "FLOAT",       MYSQL_TYPE_FLOAT},
        {    "DOUBLE",      MYSQL_TYPE_DOUBLE},
        {   "DECIMAL",  MYSQL_TYPE_NEWDECIMAL},

        {      "NULL",        MYSQL_TYPE_NULL},
        {      "CHAR",      MYSQL_TYPE_STRING},
        {   "VARCHAR",     MYSQL_TYPE_VARCHAR},

        {  "TINYTEXT",   MYSQL_TYPE_TINY_BLOB},
        {      "TEXT",        MYSQL_TYPE_BLOB},
        {"MEDIUMTEXT", MYSQL_TYPE_MEDIUM_BLOB},
        {  "LONGTEXT",   MYSQL_TYPE_LONG_BLOB},

        {  "TINYBLOB",   MYSQL_TYPE_TINY_BLOB},
        {      "BLOB",        MYSQL_TYPE_BLOB},
        {"MEDIUMBLOB", MYSQL_TYPE_MEDIUM_BLOB},
        {  "LONGBLOB",   MYSQL_TYPE_LONG_BLOB},

        { "TIMESTAMP",   MYSQL_TYPE_TIMESTAMP},
        {      "DATE",        MYSQL_TYPE_DATE},
        {      "TIME",        MYSQL_TYPE_TIME},
        {  "DATETIME",    MYSQL_TYPE_DATETIME},
        {      "YEAR",        MYSQL_TYPE_YEAR},

        {       "BIT",         MYSQL_TYPE_BIT},
        {      "ENUM",        MYSQL_TYPE_ENUM},
        {       "SET",         MYSQL_TYPE_SET},

        {      "JSON",        MYSQL_TYPE_JSON},
    };

    auto it = type_map.find(type_str);
    if (it != type_map.end()) {
        return it->second;
    } else {
        return MYSQL_TYPE_INVALID; // Return invalid type if not found
    }
}

void LogFormatTransformManager::transformDML(
    const DML *dml, MYSQL_BIN_LOG *binLog
) {
    //////////****************** gtid event
    ///*************************************

    auto lastCommit = dml->last_commit();
    auto txSeq = dml->tx_seq();
    auto immediateCommitTs = dml->msg_time();
    auto originalCommitTs = dml->tx_time();
    unsigned long long int i_ts = stringToTimestamp(immediateCommitTs->c_str());
    unsigned long long int o_ts = stringToTimestamp(originalCommitTs->c_str());

    Gtid_event *ge = new Gtid_event(
        lastCommit, txSeq, true, o_ts, i_ts, original_server_version_,
        immediate_server_version_
    );

    //////////****************** gtid event end *******************************

    //////////****************** query event start ****************************
    const char *query_arg = "BEGIN"; // row-based 的 DML 固定内容是 BEGIN
    auto dbName = dml->db_name();
    const char *db_arg = strdup(dbName->c_str()); // null-terminaled类型的字符串
    const char *catalog_arg =
        db_arg; // 在binlog v4中，目录名称通常被设置为与事件相关的数据库的名称

    uint32_t query_length = strlen(query_arg);
    LOG_INFO("query_: %s, query_len: %d", query_arg, query_length);

    unsigned long thread_id_arg = 10000;
    unsigned long long sql_mode_arg = 0;             // 随意
    unsigned long auto_increment_increment_arg = 1;  // 随意
    unsigned long auto_increment_offset_arg = 1;     // 随意
    unsigned int number = 0;                         // 0 means 'en-US'
    unsigned long long table_map_for_update_arg = 0; // 随意
    int errcode = 0;

    Query_event *qe = new Query_event(
        query_arg, catalog_arg, db_arg, INVALID_XID, query_length,
        thread_id_arg, sql_mode_arg, auto_increment_increment_arg,
        auto_increment_offset_arg, number, table_map_for_update_arg, errcode
    );
    //////////****************** query event end ******************************

    //////////****************** table map event start ************************

    auto table = dml->table_();
    const char *tbl_arg = table->c_str();
    auto fields = dml->fields();

    std::vector<mysql::Field *> field_vec;
    size_t null_bit = 0;
    TYPELIB *interval = new TYPELIB;
    int idx = 0;
    for (auto field : *fields) {
        auto field_name = field->name();
        auto fieldMeta = field->meta();
        auto field_length = fieldMeta->length();
        bool is_unsigned = fieldMeta->is_unsigned();
        bool is_nullable = fieldMeta->nullable();
        auto data_type =
            fieldMeta->data_type(); // 根据 这里的类型，构建 对应的 Field 对象
        auto decimals = fieldMeta->precision();

        enum_field_types field_type = ConvertStringType(data_type->c_str());
        if (field_type == MYSQL_TYPE_ENUM || field_type == MYSQL_TYPE_SET) {
            interval->count = field_length;
        }

        if (is_nullable) {
            null_bit = idx;
        }

        mysql::Field *field_obj = mysql::make_field(
            field_name->c_str(), field_length, is_unsigned, is_nullable,
            null_bit, field_type, interval, decimals
        );

        field_vec.emplace_back(field_obj);
    }

    Table_id tid(13); // 暂时随便写一个，实际上要做一个 连续的 id 分配器
    unsigned long colcnt = field_vec.size();
    Table_map_event *table_map_event = new Table_map_event(
        tid, colcnt, db_arg, strlen(db_arg), tbl_arg, strlen(tbl_arg), field_vec
    );

    //////////****************** table map event end *************************

    //////////****************** rows event start ****************************
    // insert 没有 keys 要判断一下
    auto keys = dml->keys();

    if (keys) {
        LOG_INFO("current sql is update or delete");
    }

    auto newData = dml->new_data();
    auto opType = dml->op_type();

    //////////****************** rows event end ****************************

    //////////****************** xid event start ******************************

    Xid_event *xe = new Xid_event(txSeq);

    //////////****************** xid event end ******************************

    //    Format_description_event fde(4, "8.0.26");
    //    binLog->write_event_to_binlog(&fde);

    //    binLog->write_event_to_binlog(ge);

    // TODO 这两个 query log event 有问题！mysqlbinlog 解释不出来
    binLog->write_event_to_binlog(qe);
    //    binLog->write_event_to_binlog(table_map_event);
    //    binLog->write_event_to_binlog(rows_event);
    //    binLog->write_event_to_binlog(xe);
}

} // namespace loft
