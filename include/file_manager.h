// file_manager.h
#ifndef FILE_MANAGER_H
#define FILE_MANAGER_H

#include "constants.h"
#include <memory>

namespace loft {

class LogFormatTransformManager {
  public:
    // 构造函数，初始化文件路径
    LogFormatTransformManager(const std::string &filepath = "./data");

    /*
        读取 data 中的 第 nowTestCase 个 SQL 语句，[1, 10]
    */
    std::unique_ptr<char[]> readSQLN(int nowTestCase);

  private:
    std::string filepath_;                   // 文件路径，默认是 ./data
    const int intOffset_ = loft::INT_OFFSET; // 头部，前 4 个字节存长度
};

} // namespace loft

#endif // FILE_MANAGER_H
