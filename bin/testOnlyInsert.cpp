//
// Created by Coonger on 2024/11/8.
//
#include "log_file.h"
#include "common/init_setting.h"
#include <vector>
#include "events/write_event.h"

int main() {

    std::string filename = "/home/yincong/loft/data";
    // 1. 创建一个 LogFileManager 对象，获得 3 个必要对象, 一定是这样子的调用过程
    auto logFileManager = std::make_unique<LogFileManager>();
    logFileManager->init(DEFAULT_BINLOG_FILE_DIR, DEFAULT_BINLOG_FILE_NAME_PREFIX, DEFAULT_BINLOG_FILE_SIZE);

    auto fileReader = logFileManager->get_file_reader();
    // reader 的成员变量等到 open 之后再初始化

    auto readFileStartTime = std::chrono::high_resolution_clock::now(); // 记录开始时间

    fileReader->open(filename.c_str());
    auto [data, fileSize ] = fileReader->readFromFile(filename);
    auto bufferReader = std::make_unique<BufferReader>(data.get(), fileSize);

    auto readFileEndTime = std::chrono::high_resolution_clock::now(); // 记录文件读取结束时间
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(readFileEndTime - readFileStartTime).count();
    LOG_DEBUG("read file time: %ld ms", duration);

    // 指定要提取的字节范围，例如从第10个字节到第20个字节
//    int DDL3Len = 256 + 328 + 448 + 12;
//    int DML2000Len = 652 * 20000; // 超过 1024 * 1024 大小，模拟自动切换写 下一个文件
//    std::vector<char> extractedData1(data.get(), data.get() + DDL3Len);
//    std::vector<char> extractedData2(data.get() + DDL3Len + 420, data.get() + DDL3Len + 420 + DML2000Len);
//
//    // 创建新文件并写入提取的数据
//    std::ofstream newFile1("data1", std::ios::binary);
//    newFile1.write(extractedData1.data(), extractedData1.size());
//    newFile1.close();
//
//    std::ofstream newFile2("data2", std::ios::binary);
//    newFile2.write(extractedData2.data(), extractedData2.size());
//    newFile2.close();

    // log_files_ 的最后一个文件的下一个文件名，默认写新文件
    auto fileWriter = logFileManager->get_file_writer();
    logFileManager->last_file(*fileWriter); // fileWrite 自动写下一个文件了，而且也打开了文件流了

    auto fileTransformManager = logFileManager->get_transform_manager();
//    std::vector<bool> is_ddl{true, true, true, true, false, false};

    // 跳过前 4 条DDL
    for (int k = 0; k < 4; k++) {
        auto sql_len = bufferReader->read<uint32_t>();
        bufferReader->forward(sql_len);
    }

    auto startTime = std::chrono::high_resolution_clock::now(); // 记录开始时间
    int DMLEPOCH = 703435;
    for (int k = 0; k < DMLEPOCH; k++) {
      auto insert_len = bufferReader->read<uint32_t>();

      std::vector<unsigned char> buf(insert_len);
      bufferReader->memcpy<unsigned char *>(buf.data(), insert_len);

      const DML *dml = GetDML(buf.data());

      LOFT_ASSERT(dml, "Failed to parse DML object.");
      // Use the`  raw pointer directly
      fileTransformManager->transformDML(dml, fileWriter->get_binlog());
    }

    auto dmlEndTime = std::chrono::high_resolution_clock::now(); // 记录结束时间
    duration = std::chrono::duration_cast<std::chrono::milliseconds>(dmlEndTime - readFileEndTime).count();
    LOG_DEBUG("DML read execution time: %ld ms", duration);

    LOG_DEBUG("cnt execution time: %ld", cnt);
    LOG_DEBUG("insert data_to_binary() execution time: %ld ms", sum);

//    int EPOCH = 6;
//    for (int k = 0; k < EPOCH; k++) {
//        auto sql_len = bufferReader->read<uint32_t>();
//        LOG_DEBUG("sql_len[%d]=%d", k, sql_len);
//
//        std::vector<unsigned char> buf(sql_len);
//        bufferReader->memcpy<unsigned char *>(buf.data(), sql_len);
//
//        if (is_ddl[k]) {
//            const DDL *ddl = GetDDL(buf.data());
//            fileTransformManager->transformDDL(ddl, fileWriter->get_binlog());
//        } else {
//            const DML *dml = GetDML(buf.data());
//            fileTransformManager->transformDML(dml, fileWriter->get_binlog());
//        }
//
//    }

}