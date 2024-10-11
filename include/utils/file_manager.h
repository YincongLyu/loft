// utils/file_manager.h
#ifndef FILE_MANAGER_H
#define FILE_MANAGER_H

#include "constants.h"
#include <fstream>
#include <vector>
#include <iostream>


namespace loft {

namespace utils {
    const int intOffset = loft::INT_OFFSET;
    // const int constants[] = loft::SQL_SIZE_ARRAY;

    std::vector<char> readSQLN(int nowTestCase) {
        int curSqlStart = nowTestCase * intOffset;
        for (int i = 0; i < nowTestCase - 1; i++) {
            curSqlStart += loft::SQL_SIZE_ARRAY[i];
        }
        int curSqlLen = loft::SQL_SIZE_ARRAY[nowTestCase - 1];
        std::ifstream file("./data", std::ios::binary);
        if (!file) {
            std::cerr << "无法打开文件。" << std::endl;
            return {};
        }

        // 将文件指针移动到指定的偏移量
        file.seekg(curSqlStart, std::ios::beg);

        // 创建一个vector来存储读取的字节
        std::vector<char> binaryData(curSqlLen);
        // 从当前文件指针位置读取len个字节到buffer数组
        file.read(binaryData.data(), curSqlLen);

        if (file.gcount() == curSqlLen) {
            return binaryData;
        } else {
            std::cerr << "读取失败或文件末尾已到。" << std::endl;
            return {};
        }
    }

} // namespace utils

} // namespace loft


#endif // FILE_MANAGER_H