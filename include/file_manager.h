// file_manager.h
#ifndef FILE_MANAGER_H
#define FILE_MANAGER_H

#include "constants.h"
#include "ddl_generated.h"
#include "dml_generated.h"
#include <memory>
#include <cstring>

namespace loft {

class MyReader {
 public:

  MyReader(const char *buffer, unsigned long long length)
      : m_buffer(buffer),
        m_ptr(buffer),
        m_length(length),
        m_limit(length) {}

template <typename T>
T read(unsigned char bytes = sizeof(T)) {
    if (m_ptr + bytes > m_buffer + m_limit) {
        throw std::out_of_range("Attempt to read beyond buffer limit");
    }
    T value = 0;
    ::memcpy((char *)&value, m_ptr, bytes);
    m_ptr = m_ptr + bytes;
    return (bytes > 1) ? letoh(value) : value;
}

template <typename T>
void memcpy(T destination, size_t length) {
    if (m_ptr + length > m_buffer + m_limit) {
        throw std::out_of_range("Attempt to copy beyond buffer limit");
    }
    ::memcpy(destination, m_ptr, length);
    m_ptr = m_ptr + length;
}


  unsigned long long position() {
    return m_ptr >= m_buffer ? m_ptr - m_buffer : m_limit;
  }

  uint16_t letoh(uint16_t value) { return le16toh(value); }

  int32_t letoh(int32_t value) { return le32toh(value); }

  uint32_t letoh(uint32_t value) { return le32toh(value); }

  int64_t letoh(int64_t value) { return le64toh(value); }

  uint64_t letoh(uint64_t value) { return le64toh(value); }

 private:
  const char *m_buffer;
  const char *m_ptr;
  unsigned long long m_length;
  unsigned long long m_limit;
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
    auto readFileAsBinary(const std::string& filePath = "./data") -> 
      std::pair<std::unique_ptr<char[]>, unsigned long long>;


    // 完整转化一个 data 文件中的所有的 sql
    void transform(const char* data, unsigned long long file_sz);

    // 组装 3 个 event
    void transformDDL(const DDL* ddl);
    
    // 组装 5 个 event
    void transformDML(const DML* dml);

  private:
    std::string filepath_;                   // 文件路径，默认是 ./data
    const int intOffset_ = loft::INT_OFFSET; // 头部，前 4 个字节存长度
};

} // namespace loft

#endif // FILE_MANAGER_H
