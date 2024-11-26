//
// Created by Coonger on 2024/11/8.
//
#include <vector>

#include "common/init_setting.h"
#include "events/write_event.h"

#include "log_file.h"
#include "buffer_reader.h"

int main()
{

  std::string filename = "/home/yincong/loft/testDataDir/data1";
  // 1. 创建一个 LogFileManager 对象，获得 3 个必要对象, 一定是这样子的调用过程
  auto logFileManager = std::make_unique<LogFileManager>();
  logFileManager->init(DEFAULT_BINLOG_FILE_DIR, DEFAULT_BINLOG_FILE_NAME_PREFIX, DEFAULT_BINLOG_FILE_SIZE);

  auto fileReader = logFileManager->get_file_reader();
  // reader 的成员变量等到 open 之后再初始化

  auto readFileStartTime = std::chrono::high_resolution_clock::now();  // 记录开始时间

  fileReader->open(filename.c_str());
  auto [data, fileSize] = fileReader->readFromFile(filename);
  auto bufferReader     = std::make_unique<BufferReader>(data.get(), fileSize);

  auto readFileEndTime = std::chrono::high_resolution_clock::now();  // 记录文件读取结束时间
  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(readFileEndTime - readFileStartTime).count();
  LOG_DEBUG("read file time: %ld ms", duration);

  // log_files_ 的最后一个文件的下一个文件名，默认写新文件
  auto fileWriter = logFileManager->get_file_writer();
  logFileManager->last_file(*fileWriter);  // fileWrite 自动写下一个文件了，而且也打开了文件流了


  std::vector<std::future<RC>> futures;  // 存储所有的future

  // 处理DDL
  int DDLEPOCH = 3;
  for (int k = 0; k < DDLEPOCH; k++) {
    auto sql_len = bufferReader->read<uint32_t>();
    std::vector<unsigned char> buf(sql_len);
    bufferReader->memcpy<unsigned char*>(buf.data(), sql_len);
    futures.push_back(logFileManager->transformAsync(std::move(buf), true));
  }

  // 跳过第四条
  bufferReader->forward(bufferReader->read<uint32_t>());

  // 处理DML
  int DMLEPOCH = 703435;
  for (int k = 0; k < DMLEPOCH; k++) {
    auto sql_len = bufferReader->read<uint32_t>();
    std::vector<unsigned char> buf(sql_len);
    bufferReader->memcpy<unsigned char*>(buf.data(), sql_len);
    futures.push_back(logFileManager->transformAsync(std::move(buf), false));
  }

  // 等待所有提交任务完成，只保证所有任务都投放到了 ring_buffer_里，并没有保证 转换完成和写入到文件中
  for (auto& future : futures) {
    RC result = future.get();
    if (result != RC::SUCCESS) {
      LOG_ERROR("Transform task failed");
    }
  }

  auto startTime = std::chrono::high_resolution_clock::now();  // 记录开始时间

  // 中途查询进度
  LOG_DEBUG("test show process......");
  logFileManager->log_progress();

  // main 函数最后部分，添加显式等待，如果等待 转换的任务执行完，就不用显示调用
//  logFileManager->wait_for_completion(); // 确保所有任务完成
//  logFileManager->shutdown();            // 显式关闭资源

  // 测试 API 3, 查询 ON.000001 文件的 scn，seq，ckp
  uint64 scn = 0;
  uint32 seq = 0;
  std::string ckp;
  logFileManager->get_last_status_from_filename("ON.000001", scn, seq, ckp);
  LOG_DEBUG("scn: %lu, seq: %u, ckp: %s", scn, seq, ckp.c_str());

  auto dmlEndTime = std::chrono::high_resolution_clock::now();  // 记录结束时间
  duration        = std::chrono::duration_cast<std::chrono::milliseconds>(dmlEndTime - readFileEndTime).count();
  LOG_DEBUG("DML transform execution time: %ld ms", duration);

}