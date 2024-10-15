#include "file_manager.h"
#include "ddl_generated.h"
#include "control_events.h"
#include <fstream>
#include <iostream>

namespace loft {

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

auto LogFormatTransformManager::readFileAsBinary(const std::string& filePath)
    -> std::pair<std::unique_ptr<char[]>, unsigned long long> {
    // std::ios::ate 定位到文件末尾，以便获取文件大小
    std::ifstream file(filePath, std::ios::binary | std::ios::ate);
    if (!file.is_open()) {
        throw std::runtime_error("Failed to open file");
    }

    // 获取文件大小，使用 unsigned long long 确保足够的容量
    // file.tellg() 返回类型是 std::streamsize，通常是一个带符号整数类型，具体实现可能为 long 或 long long
    unsigned long long size = static_cast<unsigned long long>(file.tellg());
    if (size == 0) {
        throw std::runtime_error("File is empty");
    }

    file.seekg(0, std::ios::beg);  // 回到文件开头

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

void LogFormatTransformManager::transformDDL(const DDL* ddl) {
    assert(std::strcmp(ddl->op_type()->c_str(), "DDL") == 0);
    
    auto ddlType = ddl->ddl_type();
    auto dbName = ddl->db_name();

    // TODO 填充字段
    // init GTID event + Statement event

    // auto gtid_event = new binary_log::Gtid_event();
    if (ddlType == nullptr) { // drop db

    } else {
        std::string sql_type = ddlType->c_str();
        if (sql_type == "CREATE TABLE") {
            if (dbName == nullptr) {  // create db
                // set dbName = 'mysql';
                std::cout << "create db" << std::endl;
            } else {  // create table

            }
        } else if (sql_type == "DROP TABLE") {

        }
    }
 
}


} // namespace loft
