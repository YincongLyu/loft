#include "file_manager.h"
#include "binlog.h"

#include "sql/base64.h"
#include "control_events.h"
#include "ddl_generated.h"
#include "dml_generated.h"
#include "mysql_fields.h"
#include "rows_event.h"
#include "statement_events.h"
#include "write_event.h"

#include "logging.h"
#include "macros.h"

#include <chrono>
#include <ctime>
#include <iomanip>
#include <sstream>
#include <stdexcept>

#include <fstream>
#include <iostream>
#include <map>

namespace loft {
using pib = std::pair<int, bool>;

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

std::string formatDoubleToFixedWidth(double number, int length, int frac) {
    // 转换为字符串，并设置固定的小数位数
    std::ostringstream oss;
    oss << std::fixed << std::setprecision(frac) << number;
    std::string str = oss.str();

    // 找到小数点的位置
    auto decimalPos = str.find('.');

    // 整数部分的宽度
    int intPartWidth =
        (decimalPos == std::string::npos) ? str.length() : decimalPos;

    // 判断整数部分是否超出要求的宽度
    if (intPartWidth > length - frac - 1) {
        return "";
    }

    // 按照总宽度截断
    if (str.length() > length) {
        str = str.substr(0, length);
    }

    return str;
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

    //    std::cout << originalCommitTs->c_str() << " " << o_ts << std::endl;

    auto ge = std::make_unique<Gtid_event>(
        lastCommit, txSeq, true, o_ts, i_ts, ORIGINAL_SERVER_VERSION,
        IMMEDIATE_SERVER_VERSION
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

    unsigned long thread_id_arg = THREAD_ID;
    unsigned long long sql_mode_arg = 0;             // 随意
    unsigned long auto_increment_increment_arg = 0;  // 随意
    unsigned long auto_increment_offset_arg = 0;     // 随意
    unsigned int number = 0;                         // 一定要
    unsigned long long table_map_for_update_arg = 0; // 随意
    int errcode = ERROR_CODE;

    auto qe = std::make_unique<Query_event>(
        query_arg, catalog_arg, db_arg, txSeq, query_length, thread_id_arg,
        sql_mode_arg, auto_increment_increment_arg, auto_increment_offset_arg,
        number, table_map_for_update_arg, errcode
    );

    // ******* print debug info **************************
    if (ddlType == nullptr) { // drop db
        LOG_INFO("sql_type: drop db | create/drop procedure/function");
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

    binLog->write_event_to_binlog(ge.get());
    binLog->write_event_to_binlog(qe.get());
}

enum_field_types
LogFormatTransformManager::ConvertStringType(const char *type_str) {
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
    //////////**************** gtid event start ***************************

    auto lastCommit = dml->last_commit();
    auto txSeq = dml->tx_seq();
    auto immediateCommitTs = dml->msg_time();
    auto originalCommitTs = dml->tx_time();
    unsigned long long int i_ts = stringToTimestamp(immediateCommitTs->c_str());
    unsigned long long int o_ts = stringToTimestamp(originalCommitTs->c_str());

    auto ge = std::make_unique<Gtid_event>(
        lastCommit, txSeq, true, o_ts, i_ts, ORIGINAL_SERVER_VERSION,
        IMMEDIATE_SERVER_VERSION
    );

    //////////****************** gtid event end *******************************

    //////////****************** query event start ****************************
    const char *query_arg = DML_QUERY_STR; // row-based 的 DML 固定内容是 BEGIN
    auto dbName = dml->db_name();
    const char *db_arg = dbName->c_str();
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

    auto qe = std::make_unique<Query_event>(
        query_arg, catalog_arg, db_arg, INVALID_XID, query_length,
        thread_id_arg, sql_mode_arg, auto_increment_increment_arg,
        auto_increment_offset_arg, number, table_map_for_update_arg, errcode
    );
    //////////****************** query event end ******************************

    //////////****************** table map event start ************************

    auto table = dml->table_();
    const char *tbl_arg = table->c_str();
    auto fields = dml->fields();

    std::unordered_map<std::string, int> field_map; // [field_name, field_idx]
    std::vector<mysql::FieldRef> field_vec;
    size_t null_bit = 0;
    //    TYPELIB *interval = new TYPELIB;
    int interval_count = 0;
    int fieldIdx = 0; // 下标
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
            interval_count = field_length;
        }
        if (is_nullable) {
            null_bit = fieldIdx;
        }
        // 工厂函数
        auto field_obj = mysql::make_field(
            field_name->c_str(), field_length, is_unsigned, is_nullable,
            null_bit, field_type, interval_count, decimals
        );

        field_vec.emplace_back(field_obj);
        field_map.insert({field_name->c_str(), ++fieldIdx});
    }
    // TODO 需要根据 create table 时，记录 table_id, 这是全局的
    Table_id tid(DML_TABLE_ID
    ); // 暂时随便写一个，实际上要做一个 连续的 id 分配器
    unsigned long colcnt = field_vec.size();
    // field_vec 内部的元素是共享的
    auto table_map_event = std::make_unique<Table_map_event>(
        tid, colcnt, db_arg, strlen(db_arg), tbl_arg, strlen(tbl_arg), field_vec
    );

    LOG_INFO("construct table map event end...");

    //////////****************** table map event end *************************

    //////////****************** rows event start ****************************
    auto opType = dml->op_type();
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

    auto row = std::make_unique<Rows_event>(
        tid, colcnt, 1, rows_type
    ); // 初始化 一个 rows_event 对象

    // TODO refactor 改造成函数指针
    // insert 没有 keys 要判断一下
    auto keys = dml->keys();

    if (keys) {
        std::map<int, bool> before_rows_pair;
        std::map<int, const kvPair *> conditionDataMap; // [field_idx, [k, v]]

        LOG_INFO("current sql is update or delete");

        for (auto conditionData : *keys) {
            auto key = conditionData->key();
            int field_map_id = field_map[key->c_str()];
            auto valueType = conditionData->value_type();

            if (valueType != DataMeta_NONE) {
                before_rows_pair.emplace(field_map_id, false);
                conditionDataMap[field_map_id] =
                    conditionData; // conditionDataMap
            } else {
                before_rows_pair.emplace(field_map_id, true);
            }
        }

        std::vector<int> before_rows;
        std::vector<bool> before_rows_null;
        for (auto &[idx, flag] : before_rows_pair) {
            before_rows.emplace_back(idx);
            before_rows_null.emplace_back(flag);
        }
        row->set_rows_before(before_rows);
        row->set_null_before(before_rows_null);

        for (auto &[idx, kv] : conditionDataMap) {
            auto field = field_vec[idx - 1].get();
            switch (kv->value_type()) {
                case DataMeta_NONE:
                    break;
                case DataMeta_LongVal: {
                    // bit
                    long integer_value = kv->value_as_LongVal()->value();
                    if (field->type() == MYSQL_TYPE_BIT) {
                        row->write_data_before(
                            &integer_value, field->type(), field->pack_length(),
                            0, 0, 0
                        );
                    } else if (field->type() == MYSQL_TYPE_YEAR) {
                        // year, 存的时候需要主动减去 1900
                        integer_value = integer_value - 1900;
                        row->write_data_before(
                            &integer_value, field->type(), 0, 0, 0, 0
                        );
                    } else {
                        // short, mediumint, int, bitint: 2, 3, 4, 8 整数没有
                        // strlen enum, set
                        row->write_data_before(
                            &integer_value, field->type(), field->pack_length(),
                            0, 0, 0
                        );
                    }

                    break;
                }
                case DataMeta_DoubleVal: {
                    // float: 4 字节, double 8 字节,
                    // 传入的时候需要手动处理精度问题
                    double double_value = kv->value_as_DoubleVal()->value();

                    std::string num = formatDoubleToFixedWidth(
                        double_value, field->get_width(), field->decimals()
                    );
                    LOFT_ASSERT(
                        num != "", "double number format must be valid"
                    );
                    double_value = std::stof(num);

                    if (field->type() == MYSQL_TYPE_FLOAT) {
                        float float_value = std::stof(num);
                        row->write_data_before(
                            &float_value, field->type(), field->pack_length(),
                            0, 0, 0
                        );
                    } else {
                        double_value = std::stod(num);
                        row->write_data_before(
                            &double_value, field->type(), field->pack_length(),
                            0, 0, 0
                        );
                    }
                    break;
                }
                case DataMeta_StringVal: {
                    LOG_INFO("field type: %d", field->type());
                    // decimal 传入的也是字符串类型
                    const char *str =
                        kv->value_as_StringVal()->value()->c_str();

                    if (field->type() == MYSQL_TYPE_NEWDECIMAL) {
                        double double_value = std::stod(str
                        ); // 此时 double 可能会加上更多的小数部分，但可以忽略
                        LOG_INFO(
                            "current field type is decimal, double_value: %lf",
                            double_value
                        );
                        row->write_data_before(
                            &double_value, field->type(), 0, 0,
                            field->pack_length(), field->decimals()
                        );

                    } else {
                        const char *dst = (char *)malloc(
                            base64_needed_decoded_length(strlen(str))
                        );
                        int64_t dst_len = base64_decode(
                            str, strlen(str), (void *)dst, nullptr, 0
                        );

                        row->write_data_before(
                            dst, field->type(), field->pack_length(), dst_len,
                            0, 0
                        );
                    }

                    break;
                }
            }
        }
    }

    auto newData = dml->new_data();
    // delete 没有 newData 要判断一下
    if (newData) {
        std::map<int, bool> after_rows_pair;
        std::map<int, const kvPair *> newDataMap; // [field_idx, [k, v]]

        for (auto setData : *newData) {
            auto key = setData->key();
            int field_map_id = field_map[key->c_str()];
            auto valueType = setData->value_type();

            if (valueType != DataMeta_NONE) {
                after_rows_pair.emplace(field_map_id, false);

                newDataMap[field_map_id] = setData; // newDataMap
            } else {
                after_rows_pair.emplace(field_map_id, true);
            }
        }
        // 还要再遍历一次，把 after_rows, after_rows_null 写进去
        std::vector<int> after_rows;
        std::vector<bool> after_rows_null;

        for (auto &[idx, flag] : after_rows_pair) {
            after_rows.emplace_back(idx);
            after_rows_null.emplace_back(flag);
        }
        row->set_rows_after(after_rows);
        row->set_null_after(after_rows_null);

        // 顺序遍历，写 row, 这次只看 value
        for (auto &[idx, kv] : newDataMap) {
            auto field = field_vec[idx - 1].get();
            switch (kv->value_type()) {
                case DataMeta_NONE:
                    break;
                case DataMeta_LongVal: {
                    // bit
                    long integer_value = kv->value_as_LongVal()->value();
                    if (field->type() == MYSQL_TYPE_BIT) {
                        row->write_data_after(
                            &integer_value, field->type(), field->pack_length(),
                            0, 0, 0
                        );
                    } else if (field->type() == MYSQL_TYPE_YEAR) {
                        // year, 存的时候需要主动减去 1900
                        integer_value = integer_value - 1900;
                        row->write_data_after(
                            &integer_value, field->type(), 0, 0, 0, 0
                        );
                    } else {
                        // short, mediumint, int, bitint: 2, 3, 4, 8 整数没有
                        // strlen enum, set
                        row->write_data_after(
                            &integer_value, field->type(), field->pack_length(),
                            0, 0, 0
                        );
                    }

                    break;
                }
                case DataMeta_DoubleVal: {
                    // float: 4 字节, double 8 字节,
                    // 传入的时候需要手动处理精度问题
                    double double_value = kv->value_as_DoubleVal()->value();

                    std::string num = formatDoubleToFixedWidth(
                        double_value, field->get_width(), field->decimals()
                    );
                    LOFT_ASSERT(
                        num != "", "double number format must be valid"
                    );
                    double_value = std::stof(num);

                    if (field->type() == MYSQL_TYPE_FLOAT) {
                        float float_value = std::stof(num);
                        row->write_data_after(
                            &float_value, field->type(), field->pack_length(),
                            0, 0, 0
                        );
                    } else {
                        double_value = std::stod(num);
                        row->write_data_after(
                            &double_value, field->type(), field->pack_length(),
                            0, 0, 0
                        );
                    }

                    break;
                }
                case DataMeta_StringVal: {
                    LOG_INFO("field type: %d", field->type());
                    // decimal 传入的也是字符串类型
                    const char *str =
                        kv->value_as_StringVal()->value()->c_str();

                    if (field->type() == MYSQL_TYPE_NEWDECIMAL) {
                        double double_value = std::stod(str
                        ); // 此时 double 可能会加上更多的小数部分，但可以忽略
                        LOG_INFO(
                            "current field type is decimal, double_value: %lf",
                            double_value
                        );
                        row->write_data_after(
                            &double_value, field->type(), 0, 0,
                            field->pack_length(), field->decimals()
                        );

                    } else {
                        const char *dst = (char *)malloc(
                            base64_needed_decoded_length(strlen(str))
                        );
                        int64_t dst_len = base64_decode(
                            str, strlen(str), (void *)dst, nullptr, 0
                        );

                        row->write_data_after(
                            dst, field->type(), field->pack_length(), dst_len,
                            0, 0
                        );
                    }

                    break;
                }
            }
        }
    }

    //////////****************** rows event end ****************************

    //////////****************** xid event start ******************************

    auto xe = std::make_unique<Xid_event>(txSeq);
    LOG_INFO("construct xid event end...");

    //////////****************** xid event end ******************************

    binLog->write_event_to_binlog(ge.get());
    binLog->write_event_to_binlog(qe.get());
    binLog->write_event_to_binlog(table_map_event.get());
    binLog->write_event_to_binlog(row.get());
    binLog->write_event_to_binlog(xe.get());
}

} // namespace loft
