#include "file_manager.h"
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

} // namespace loft
