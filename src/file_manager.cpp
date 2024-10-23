#include "file_manager.h"
#include "binlog.h"
#include "control_events.h"
#include "ddl_generated.h"
#include "statement_events.h"

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
unsigned long long int stringToTimestamp(const std::string& timeString) {
    std::tm timeStruct = {};
    std::string datetimePart, microsecondPart;

    // Split the datetime string into two parts: "2024-08-01 14:32:41" and "000054"
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
    return std::chrono::duration_cast<std::chrono::microseconds>(timePointWithMicroseconds.time_since_epoch()).count();
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

void LogFormatTransformManager::transform(const char* data, unsigned long long file_sz) {
    return;
}

void LogFormatTransformManager::transformDDL(const DDL *ddl, MYSQL_BIN_LOG *binLog) {
    assert(std::strcmp(ddl->op_type()->c_str(), "DDL") == 0);

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

    Gtid_event* ge = new Gtid_event(lastCommit, txSeq, true,
                                    o_ts, i_ts,
                                    original_server_version_, immediate_server_version_);

    // 2. 构造 Query event
    const char *query_arg = ddlSql->c_str();
    const char *catalog_arg = nullptr;
    const char *db_arg = nullptr;
    if (dbName != nullptr) {
        db_arg = dbName->c_str();
    }
    uint32_t query_length = strlen(query_arg);
    unsigned long thread_id_arg = 10000;
    unsigned long long sql_mode_arg = 0; // 随意
    unsigned long auto_increment_increment_arg = 0; // 随意
    unsigned long auto_increment_offset_arg = 0; // 随意
    unsigned int number = 0;  // 一定要
    unsigned long long table_map_for_update_arg = 0; // 随意
    int errcode = 0;

    Query_event* qe = new Query_event(query_arg, catalog_arg, db_arg, txSeq, query_length, thread_id_arg, sql_mode_arg,
                                      auto_increment_increment_arg, auto_increment_offset_arg, number, table_map_for_update_arg, errcode);

// ******* print debug info **************************
    if (ddlType == nullptr) { // drop db
        std::cout << "drop db" << std::endl;
    } else {
        std::string sql_type = ddlType->c_str();
        if (sql_type == "CREATE TABLE") {
            if (db_arg == nullptr) { // create db
                std::cout << "create db" << std::endl;
            } else { // create table
                std::cout << "create table" << std::endl;
            }
        } else if (sql_type == "DROP TABLE") {
            std::cout << "drop table" << std::endl;
        }
    }
// ******************************************************************

    binLog->write_event_to_binlog(ge);
    binLog->write_event_to_binlog(qe);

}

void LogFormatTransformManager::transformDML(const DML *dml, MYSQL_BIN_LOG *binLog) {



}

} // namespace loft
