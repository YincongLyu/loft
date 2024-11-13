//
// Created by Coonger on 2024/11/12.
//
#include <gtest/gtest.h>
#include "log_file.h"
#include "iostream"

TEST(LOG_FILE_TEST, INIT_TEST) {

    auto logFileManager = std::make_unique<LogFileManager>();

    EXPECT_EQ( logFileManager->init(DEFAULT_BINLOG_FILE_DIR, DEFAULT_BINLOG_FILE_NAME_PREFIX, DEFAULT_BINLOG_FILE_SIZE), RC::SUCCESS);

    auto files = logFileManager->get_log_files();

    EXPECT_EQ(files.size(), 2);
    for (auto &file : files) {
        std::cout << file.second << std::endl;
    }

}


TEST(LOG_FILE_TEST, TRANSFORM_TEST) {
    std::string ddlFilename = "/home/yincong/loft/data1";
    std::string dmlFilename = "/home/yincong/loft/data2";
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