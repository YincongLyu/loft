//
// Created by Coonger on 2024/11/12.
//
#include <gtest/gtest.h>
#include "log_file.h"
#include "iostream"
#include "buffer_reader.h"

/**
 * @brief 验证 接口一 init() 接口是否正确设置：binlog 写入的目录，binlog 文件前缀名，binlog 文件大小
 */
TEST(LOG_FILE_TEST, INIT_TEST) {
    auto logFileManager = std::make_unique<LogFileManager>();

    EXPECT_EQ( logFileManager->init(DEFAULT_BINLOG_FILE_DIR, DEFAULT_BINLOG_FILE_NAME_PREFIX, DEFAULT_BINLOG_FILE_SIZE), RC::SUCCESS);
    EXPECT_STREQ(logFileManager->get_directory(), "/home/yincong/collectBin/");
    EXPECT_STREQ(logFileManager->get_file_prefix(), "ON");
    EXPECT_EQ(logFileManager->get_file_max_size(), 20971520);

    auto files = logFileManager->get_log_files();

    EXPECT_EQ(files.size(), 2);
    for (auto &file : files) {
        std::cout << file.second << std::endl;
    }

}

/**
 * @brief [Only] 内部测试，统计 directory 目录下，有多少个 binlog 文件
 */
TEST(LOG_FILE_TEST, LIST_FILE_TEST) {

  auto logFileManager = std::make_unique<LogFileManager>();

  logFileManager->init(DEFAULT_BINLOG_FILE_DIR, DEFAULT_BINLOG_FILE_NAME_PREFIX, DEFAULT_BINLOG_FILE_SIZE);

  auto files = logFileManager->get_log_files();

  EXPECT_EQ(files.size(), 2);
  for (auto &file : files) {
    std::cout << file.second << std::endl;
  }

}

/**
 * @brief [Only] 内部测试，将 binlog 拆分成 2 个文件，一个是 DDL，一个是 DML
 */
TEST(LOG_FILE_TEST1, DISABLED_Extract_DML_FILE) {
  std::string splitFilename = "/home/yincong/loft/testDataDir/data";
  std::string dmlFilename = "/home/yincong/loft/testDataDir/data2";

  auto logFileManager = std::make_unique<LogFileManager>();
  logFileManager->init(DEFAULT_BINLOG_FILE_DIR, DEFAULT_BINLOG_FILE_NAME_PREFIX, DEFAULT_BINLOG_FILE_SIZE);

  auto fileReader = logFileManager->get_file_reader();
  fileReader->open(splitFilename.c_str());
  auto [data, fileSize ] = fileReader->readFromFile(splitFilename);
  auto bufferReader = std::make_unique<BufferReader>(data.get(), fileSize);

  // 跳过前 4 条DDL
  for (int k = 0; k < 4; k++) {
      auto sql_len = bufferReader->read<uint32_t>();
      bufferReader->forward(sql_len);
  }

  auto nowPos = bufferReader->position();
  std::ofstream outFile(dmlFilename, std::ios::binary);
  outFile.write(data.get() + nowPos, fileSize - nowPos);

  outFile.close();

}

/**
 * @brief [Only] 内部测试，转换 2 个 binlog 文件，第一个入参是一个 文件
 */
TEST(LOG_FILE_TEST1, TRANSFORM_TEST) {
    std::string ddlFilename = "/home/yincong/loft/testDataDir/data1";
    std::string dmlFilename = "/home/yincong/loft/testDataDir/data2";
    // 1. 创建一个 LogFileManager 对象，获得 3 个必要对象, 一定是这样子的调用过程
    auto logFileManager = std::make_unique<LogFileManager>();
    logFileManager->init(DEFAULT_BINLOG_FILE_DIR, DEFAULT_BINLOG_FILE_NAME_PREFIX, DEFAULT_BINLOG_FILE_SIZE);

    // 1. 测试 ddl sql
    logFileManager->transform(ddlFilename.c_str(), true);
    LOG_DEBUG("DDL transform done");

    // 2. 测试 dml sql
    logFileManager->transform(dmlFilename.c_str(), false);
    LOG_DEBUG("DML transform done");

}

/**
 * @brief [Only] 内部测试，统计 用例一 DML sql 有多少条
 */
TEST(LOG_FILE_TEST1, InsertDMLSqlCnt) {
  // 1. 新建一个 binlog 文件，开启写功能
  std::string filename = "/home/yincong/loft/testDataDir/data";
  auto logFileManager = std::make_unique<LogFileManager>();
  logFileManager->init(DEFAULT_BINLOG_FILE_DIR, DEFAULT_BINLOG_FILE_NAME_PREFIX, DEFAULT_BINLOG_FILE_SIZE);

  auto fileReader = logFileManager->get_file_reader();
  fileReader->open(filename.c_str());
  auto [data, fileSize] = fileReader->readFromFile(filename);
  auto bufferReader     = std::make_unique<BufferReader>(data.get(), fileSize);

  // 2. 打开最后一个 binlog 文件，准备写
  auto fileWriter = logFileManager->get_file_writer();
  logFileManager->last_file(*fileWriter);

  // 跳过前 4 条
  uint32 sql_len;
  int SKIP_CNT = 4;
  for (int k = 0; k < SKIP_CNT; k++) {
    sql_len = bufferReader->read<uint32_t>();
    bufferReader->forward(sql_len);
  }

  int dml_cnt = 0;
  while (bufferReader->valid()) {
    dml_cnt++;
    sql_len = bufferReader->read<uint32_t>();;
    bufferReader->forward(sql_len);
  }

  LOG_DEBUG("dml_cnt: %d", dml_cnt);

}