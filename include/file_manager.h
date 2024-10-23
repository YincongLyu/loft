// file_manager.h
#ifndef FILE_MANAGER_H
#define FILE_MANAGER_H

#include "binlog.h"
#include "constants.h"
#include "table_id.h"
#include "ddl_generated.h"
#include "dml_generated.h"
#include <cstring>
#include <memory>
#include <unordered_map>

namespace loft {

class MyReader {
  public:
    MyReader(const char *buffer, unsigned long long length)
        : buffer_(buffer)
        , ptr_(buffer)
        , limit_(length) {}

    template<typename T>
    T read(unsigned char bytes = sizeof(T)) {
        if (ptr_ + bytes > buffer_ + limit_) {
            throw std::out_of_range("Attempt to read beyond buffer limit");
        }
        T value = 0;
        ::memcpy((char *)&value, ptr_, bytes);
        ptr_ = ptr_ + bytes;
        return (bytes > 1) ? letoh(value) : value;
    }

    template<typename T>
    void memcpy(T destination, size_t length) {
        if (ptr_ + length > buffer_ + limit_) {
            throw std::out_of_range("Attempt to copy beyond buffer limit");
        }
        ::memcpy(destination, ptr_, length);
        ptr_ = ptr_ + length;
    }

    void forward(size_t length) {
        if (ptr_ + length > buffer_ + limit_) {
            throw std::out_of_range("Attempt to forward beyond buffer limit");
        }
        ptr_ = ptr_ + length;
    }



    unsigned long long position() {
        return ptr_ >= buffer_ ? ptr_ - buffer_ : limit_;
    }

    template<typename T>
    T letoh(T value) {
        if constexpr (std::is_same_v<T, uint16_t>) {
            return le16toh(value);
        } else if constexpr (std::is_same_v<T, int32_t>) {
            return le32toh(value);
        } else if constexpr (std::is_same_v<T, uint32_t>) {
            return le32toh(value);
        } else if constexpr (std::is_same_v<T, int64_t> || std::is_same_v<T, uint64_t>) {
            return le64toh(value);
        } else {
            throw std::invalid_argument("Unsupported type for letoh");
        }
    }

  private:
    const char *buffer_;
    const char *ptr_;
    unsigned long long limit_;
};

class LogFormatTransformManager {
  public:
    // 构造函数，初始化文件路径
    LogFormatTransformManager(const std::string &filepath = "./data");

    /*
        读取 data 中的 第 nowTestCase 个 SQL 语句，[1, 10]
    */
    auto readSQLN(int nowTestCase) -> std::unique_ptr<char[]>;

    /*
       读文件
    */
    auto readFileAsBinary(const std::string &filePath = "./data")
        -> std::pair<std::unique_ptr<char[]>, unsigned long long>;

    // 完整转化一个 data 文件中的所有的 sql
    void transform(const char *data, unsigned long long file_sz);

    // 组装 3 个 event
    void transformDDL(const DDL *ddl, MYSQL_BIN_LOG *binLog);

    // 组装 5 个 event
    void transformDML(const DML *dml, MYSQL_BIN_LOG *binLog);

  private:
    std::string filepath_;                   // 文件路径，默认是 ./data
    const int intOffset_ = loft::INT_OFFSET; // 头部，前 4 个字节存长度
    uint32_t original_server_version_ = 80026; // 配置项读入
    uint32_t immediate_server_version_ = 80026;

    // <table_name, table_id> 对应，在执行多条 DML 时，能确定正在 操作同一张表
    std::unordered_map<std::string, Table_id> tableName2TableId_;
};

} // namespace loft

#endif // FILE_MANAGER_H
