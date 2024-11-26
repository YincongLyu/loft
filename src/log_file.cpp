//
// Created by Coonger on 2024/11/10.
//

#include <fcntl.h>   // ::open
#include <charconv>  // std::from_chars
#include <string_view>  // std::string_view
#include <cstring> // std::strcmp

#include "log_file.h"
#include "buffer_reader.h"

auto RedoLogFileReader::open(const char *filename) -> RC {
    filename_ = filename;
    fd_ = ::open(filename, O_RDONLY);
    if (fd_ < 0) {
        LOG_ERROR("open file failed. filename=%s, error=%s", filename, strerror(errno));
        return RC::FILE_OPEN;
    }

    LOG_INFO("open file success. filename=%s, fd=%d", filename, fd_);
    return RC::SUCCESS;
}

auto RedoLogFileReader::close() -> RC {
    if (fd_ < 0) {
        return RC::FILE_NOT_OPENED;
    }

    ::close(fd_);
    fd_ = -1;
    return RC::SUCCESS;
}

auto RedoLogFileReader::readFromFile(const std::string &fileName)
    -> std::pair<std::unique_ptr<char[]>, size_t> {
    FILE *file = fopen(fileName.c_str(), "rb");
    if (file == nullptr) {
        std::cerr << "Failed to open file " << fileName << std::endl;
        return {nullptr, 0};  // 返回空指针和大小为0
    }

    const size_t bufferSize = IO_SIZE;  // 每次读取4KB数据
    char buffer[bufferSize];
    size_t readSize = 0;
    size_t oneRead = 0;

    // 动态缓冲区大小控制，通过unique_ptr管理data
    std::unique_ptr<char[]> data;
    size_t dataCapacity = 0;

    // 循环读取文件内容
    while (!feof(file)) {
        memset(buffer, 0, sizeof(buffer));
        oneRead = fread(buffer, 1, sizeof(buffer), file);
        if (ferror(file)) {
            std::cerr << "Failed to read data from " << fileName << std::endl;
            fclose(file);
            return {nullptr, 0};
        }

        // 如果当前读取大小超过 data 的容量，重新分配
        if (readSize + oneRead > dataCapacity) {
            dataCapacity = (readSize + oneRead) * 2;
            std::unique_ptr<char[]> newData(new char[dataCapacity]);

            if (data) {
                memcpy(newData.get(), data.get(), readSize);
            }
            data = std::move(newData);
        }

        memcpy(data.get() + readSize, buffer, oneRead);
        readSize += oneRead;
    }

    fclose(file);

    // 调整最终大小，使其准确匹配已读取的数据量
    std::unique_ptr<char[]> result(new char[readSize + 1]);
    memcpy(result.get(), data.get(), readSize);
    result[readSize] = '\0';

    return {std::move(result), readSize};
}
///////////////////////////////////////////////////
// fileWriter 的 open 和 close ，选择直接操作 文件流，而不是 fd
RC BinLogFileWriter::open(const char *filename, size_t max_file_size)
{
    filename_ = filename;
    // 这里仅是 初始化了文件信息，还没有 open 文件流
    RC ret;
    bin_log_ = std::make_unique<MYSQL_BIN_LOG>(filename, max_file_size, ret);
    // 确保 open 失败时返回错误，而不是继续运行
    if (ret != RC::SUCCESS || bin_log_ == nullptr) {
      LOG_ERROR("Failed to create binlog file: %s", filename);
      bin_log_.reset();  // 确保指针清空
      return RC::FILE_OPEN;
    }
    // 直接返回 当前文件的 可写位置，相当于继续写
    return bin_log_->open(); // 正确返回 RC::SUCCESS
}

RC BinLogFileWriter::close()
{
    // 在 next_file 里调用，由于会先调用 close，所以这里可以直接返回
    // 只有外部第一次调用 open，才会初始化 bin_log_
    if (bin_log_ == nullptr) {
        LOG_DEBUG("At first time revoke last_file or next file");
        return RC::FILE_NOT_OPENED;
    }

    return bin_log_->close(); // 正确返回  RC::SUCCESS;
}

RC BinLogFileWriter::write(AbstractEvent &event) { return bin_log_->write_event_to_binlog(&event) ? RC::SUCCESS : RC::IOERR_EVENT_WRITE; }

///////////////////////////////////////////////////

RC LogFileManager::init(const char *directory, const char *file_name_prefix, uint64_t max_file_size_per_file) {

    directory_ = std::filesystem::absolute(std::filesystem::path(directory));
    file_prefix_ = file_name_prefix;
    max_file_size_per_file_ = max_file_size_per_file;

    // 检查目录是否存在，不存在就创建出来
    if (!std::filesystem::is_directory(directory_)) {
        LOG_INFO("directory is not exist. directory=%s", directory_.c_str());

        std::error_code ec;
        bool ret = std::filesystem::create_directories(directory_, ec);
        if (!ret) {
            LOG_ERROR("create directory failed. directory=%s, error=%s", directory_.c_str(), ec.message().c_str());
            return RC::FILE_CREATE;
        }
    }

    // 列出所有的日志文件
    for (const std::filesystem::directory_entry &dir_entry : std::filesystem::directory_iterator(directory_)) {
        if (!dir_entry.is_regular_file()) {
            continue;
        }

        std::string filename = dir_entry.path().filename().string();
        // TODO
        uint32_t fileno = 0;
        RC rc = get_fileno_from_filename(filename, fileno);
        if (LOFT_FAIL(rc)) {
            LOG_INFO("invalid log file name. filename=%s", filename.c_str());
            continue;
        }

        if (log_files_.find(fileno) != log_files_.end()) {
            LOG_INFO("duplicate log file. filename1=%s, filename2=%s",
                      filename.c_str(), log_files_.find(fileno)->second.filename().c_str());
            continue;
        }

        log_files_.emplace(fileno, dir_entry.path());
    }

    LOG_INFO("init log file manager success. directory=%s, log files=%d",
             directory_.c_str(), static_cast<int>(log_files_.size()));


    // 获得索引文件 句柄
    std::filesystem::path index_path = directory_ / (file_prefix_ + index_suffix_);
    index_fd_ = ::open(index_path.c_str(), O_RDWR | O_CREAT | O_APPEND, 0644);
    if (index_fd_ < 0) {
      LOG_ERROR("open file failed. filename=%s, error=%s", index_path.c_str(), strerror(errno));
      return RC::FILE_OPEN;
    }

    return RC::SUCCESS;
}

RC LogFileManager::get_fileno_from_filename(
    const std::string &filename, uint32_t &fileno
) {

    if (!filename.starts_with(file_prefix_)) {
        LOG_INFO("invalid log file name: cannot calc lsn. filename=%s, error=%s",
                  filename.c_str(), strerror(static_cast<int>(result.ec)));
        return RC::INVALID_ARGUMENT;
    }

    std::string_view lsn_str(filename.data() + strlen(file_prefix_) + 1, filename.length() - strlen(file_prefix_) - 1);
    std::from_chars_result result = std::from_chars(lsn_str.data(), lsn_str.data() + lsn_str.size(), fileno);
    if (result.ec != std::errc()) {
        LOG_INFO("invalid log file name: cannot calc lsn. filename=%s, error=%s",
                  filename.c_str(), strerror(static_cast<int>(result.ec)));
        return RC::INVALID_ARGUMENT;
    }

    return RC::SUCCESS;
}


RC LogFileManager::transform(const char *filename, bool is_ddl) {
    // reader 的成员变量等到 open 之后再初始化
    file_reader_->open(filename);

    auto [data, fileSize ] = file_reader_->readFromFile(filename);
    auto bufferReader = std::make_unique<BufferReader>(data.get(), fileSize);

    // 每次调用 transform 都默认从目录下最后一个文件继续写
    this->last_file(*file_writer_.get());
    LOG_DEBUG("last file write pos=%lu", file_writer_->get_binlog()->get_bytes_written());

    // 但这个其实很难控制，我们应该控制每个 文件可以处理 xxx 条 sql，保证总体在 限定bytes就可以，不然每次都要用 if 判断 是否需要切换文件
    if (is_ddl) {
        while (bufferReader->valid()) {
            auto sql_len = bufferReader->read<uint32_t>();

            // 2.2. 进入转换流程，先初始化一片 内存空间， copy 出来
            std::vector<unsigned char> buf(sql_len);
            bufferReader->memcpy<unsigned char *>(buf.data(), sql_len);
            // FIXME
            // 在这里每次转换一条 sql， 找时机切换到 next_file! 如果在这里每次判断，非常耗时
            if (!file_writer_->get_binlog()->remain_bytes_safe()) {
                this->next_file(*file_writer_.get());
            }
            const DDL *ddl = GetDDL(buf.data());
            transform_manager_->transformDDL(ddl, file_writer_->get_binlog());
        }
    } else {
        int dml_count = 0;
        while (bufferReader->valid()) {
            auto sql_len = bufferReader->read<uint32_t>();

            std::vector<unsigned char> buf(sql_len);
            bufferReader->memcpy<unsigned char *>(buf.data(), sql_len);
            // 在 1048 427 切换了，1024 * 1024 = 1048 576，写完之后 1048 488 差一点就超出了
            if (!file_writer_->get_binlog()->remain_bytes_safe()) {
                this->next_file(*file_writer_.get());
                LOG_DEBUG("write dml cnt=%d: revoke rotate", dml_count);
            }

            const DML *dml = GetDML(buf.data());
            transform_manager_->transformDML(dml, file_writer_->get_binlog());
            dml_count++;
//            if (dml_count % 500 == 0)
//                LOG_DEBUG("%lu", file_writer_->get_binlog()->get_bytes_written());
        }
//        LOG_DEBUG("done, dml count=%d", dml_count);
    }

    return RC::SUCCESS;
}

/**
 * @brief 同步 调用 transform()
 */
RC LogFileManager::transform(std::vector<unsigned char> &&buf, bool is_ddl) {

  // 1. 此处实现一个堆积，放到 ring_buffer_ 里，后续的执行线程取任务
  Task task(std::move(buf), is_ddl);

  // 2. 后面就开始线程从 TaskQueue 里拿取任务
  if (!ring_buffer_->write(std::move(task))) {
    LOG_ERROR("Failed to write task to ring buffer, should wait to write.");
    return RC::SPEED_LIMIT;
  }

  return RC::SUCCESS;

}

/**
 * @brief 异步调用，移动拷贝数据，批处理
 * @param buf
 * @param is_ddl
 * @return
 */
std::future<RC> LogFileManager::transformAsync(std::vector<unsigned char>&& buf, bool is_ddl) {
  auto promise = std::make_shared<std::promise<RC>>();
  auto future = promise->get_future();

  try {
    // 直接移动 buf，避免拷贝
    Task task(std::move(buf), is_ddl);

    if (!ring_buffer_->write(std::move(task))) { // 这里也使用移动语义
      promise->set_value(RC::SPEED_LIMIT);
      return future;
    }

    size_t current_pending = ++pending_tasks_;
    // 只有当积累了足够的任务或者是DDL任务时才通知消费者
    if (current_pending >= BATCH_SIZE) {
      task_cond_.notify_one(); // 这里 notifiy 的是谁呢？谁在等 task_cond_？
    }

    promise->set_value(RC::SUCCESS);
  } catch (const std::exception& e) {
    promise->set_exception(std::current_exception());
  }

  return future;
}

std::future<RC> LogFileManager::transformAsync2(std::vector<unsigned char>&& buf, bool is_ddl) {
  auto promise = std::make_shared<std::promise<RC>>();
  auto future = promise->get_future();

  // 将任务直接放入预加载任务队列
  preload_tasks({Task(std::move(buf), is_ddl)});

  // 不再显式通知 `process_tasks`
  promise->set_value(RC::SUCCESS);
  return future;
}


LogFileManager::LogFileManager()
    : file_reader_(std::make_unique<RedoLogFileReader>()),
    file_writer_(std::make_unique<BinLogFileWriter>()),
    transform_manager_(std::make_unique<LogFormatTransformManager>()),
    ring_buffer_(std::make_shared<TaskQueue<Task>>(10000)) {

  // 启动任务收集线程
//  task_collector_threads_.emplace_back(&LogFileManager::process_tasks2, this);


  // 初始化线程池
//  init_thread_pool(1,4);
//
//  // 启动专门的写入线程
//  writer_thread_ = std::thread([this] {
//    result_queue_.process_writes(file_writer_.get(), this);
//  });
    // 其他初始化操作可以放在这里，比如加载已有日志文件的索引，设置初始状态等
}

RC LogFileManager::last_file(BinLogFileWriter &file_writer) {
    if (log_files_.empty()) {
        return next_file(file_writer);
    }

    file_writer.close();

    auto last_file_item = log_files_.rbegin();
    return file_writer.open(last_file_item->second.c_str(), max_file_size_per_file_);
}

RC LogFileManager::next_file(BinLogFileWriter &file_writer) {

    // 最小从 1 开始
    uint32_t fileno = log_files_.empty() ? 1 : log_files_.rbegin()->first + 1;

    last_file_no_ = fileno;

    std::ostringstream oss;
    oss << std::setw(6) << std::setfill('0') << fileno;
    file_suffix_ = oss.str();

    std::string nextFilename = file_prefix_ + std::string(file_dot_) + file_suffix_;
    std::filesystem::path next_file_path = directory_ / nextFilename;

    if (!log_files_.empty()) {
        auto nowFilename = log_files_.rbegin()->second.string();
        // 在上一个文件中，写入一个 rotate event 再关闭
        auto rotateEvent = std::make_unique<Rotate_event>(nowFilename, nowFilename.length(),
                                                          Rotate_event::DUP_NAME, 4);
        assert(rotateEvent != nullptr);
        file_writer.get_binlog()->write_event_to_binlog(rotateEvent.get());

        file_writer.close();
    }

    log_files_.emplace(fileno, next_file_path);
    // 写入索引文件
    write_filename2index(nextFilename);

    LOG_DEBUG("[==rotate file==]next file name = %s", next_file_path.c_str());

    return file_writer.open(next_file_path.c_str(), max_file_size_per_file_);
}

LogFileManager::~LogFileManager() {
  try {
    // 4. 最后关闭 index_fd_
    if (index_fd_ >= 0) {
      ::close(index_fd_);
      index_fd_ = -1;
    }
  } catch (const std::exception& e) {
    LOG_ERROR("Exception in ~LogFileManager: %s", e.what());
  }

}

RC LogFileManager::write_filename2index(std::string &filename) {

  filename += "\n";  // 添加换行符
  ssize_t write_len = write(index_fd_, filename.c_str(), filename.length());
  if (write_len != static_cast<ssize_t>(filename.length())) {
    LOG_ERROR("Failed to write to index file, expected %zu bytes, wrote %zd bytes, error: %s",
                 filename.length(), write_len, strerror(errno));
    return RC::IOERR_WRITE;
  }

  return RC::SUCCESS;
}

RC LogFileManager::get_last_status_from_filename(
    const std::string &filename, uint64 &scn, uint32 &seq, std::string &ckp)
{
  uint32 file_no;
  RC rc = get_fileno_from_filename(filename, file_no);
  if (rc != RC::SUCCESS) {
    return rc;
  }

  // 获取文件编号对应的 checkpoint
  auto iter = file_ckp_.find(file_no);
  if (iter == file_ckp_.end()) {
    return RC::FILE_NOT_EXIST;  // 文件不存在
  }
  ckp = iter->second;

  // 解析 ckp，格式应为 "trxSeq-seq-scn"
  std::string delimiter = "-";
  size_t pos = 0;
  size_t count = 0;
  uint64 numbers[3] = {0};  // 用于存储解析的数字

  std::string input = ckp;
  while ((pos = input.find(delimiter)) != std::string::npos && count < 3) {
    std::string token = input.substr(0, pos);

    try {
      numbers[count] = std::stoull(token);  // 使用 stoull 解析为 uint64
    } catch (const std::exception &e) {
      LOG_ERROR("Failed to parse checkpoint: %s", e.what());
      return RC::INVALID_ARGUMENT;
    }

    input.erase(0, pos + delimiter.length());
    count++;
  }

  // 最后一个数字
  if (count == 2 && !input.empty()) {
    try {
      numbers[count] = std::stoull(input);  // 解析最后一个数字
    } catch (const std::exception &e) {
      LOG_ERROR("Failed to parse final checkpoint value: %s", e.what());
      return RC::INVALID_ARGUMENT;
    }
  }

  if (count != 2) {
    LOG_ERROR("Invalid checkpoint format: %s", ckp.c_str());
    return RC::INVALID_ARGUMENT;  // 检查是否解析了足够的字段
  }

  // 分配解析后的值
  seq = static_cast<uint32>(numbers[1]);  // `seq` 是 uint32
  scn = numbers[2];                       // `scn` 是 uint64

  return RC::SUCCESS;
}

/**
 * @brief 仅用作 任务收集 线程
 */
void LogFileManager::process_tasks() {
  std::vector<Task> batch_tasks;
  batch_tasks.reserve(BATCH_SIZE);

  while (!stop_flag_) {  // 使用stop_flag_替代之前的stop_threads_
    {
      std::unique_lock<std::mutex> lock(task_mutex_);
      task_cond_.wait_for(lock,
          std::chrono::milliseconds(100),  // 添加超时以便定期检查stop_flag_
          [this] {
            return stop_flag_ || pending_tasks_ >= BATCH_SIZE;
          });

      if (stop_flag_ && pending_tasks_ == 0) break;

      size_t tasks_to_read = std::min(pending_tasks_.load(), BATCH_SIZE);
      for (size_t i = 0; i < tasks_to_read; ++i) {
        Task task;
        if (ring_buffer_->read(task)) {
          batch_tasks.push_back(std::move(task));
          --pending_tasks_;
        }
      }
    }

    if (!batch_tasks.empty()) {
      auto processor = std::make_unique<BatchProcessor>(
          this,
          std::move(batch_tasks),
          batch_sequence_++);
      batch_queue_.add_batch(std::move(processor));
      batch_tasks.clear();
    }
  }
}

void LogFileManager::process_tasks2() {
  std::vector<Task> batch_tasks;
  batch_tasks.reserve(BATCH_SIZE);

  while (!stop_flag_) {
    try {
      {
        std::unique_lock<std::mutex> lock(preload_mutex_);
        task_cond_.wait(lock, [this] {
          return preloading_done_ || stop_flag_;
        });

        if (stop_flag_ && preloaded_tasks_.empty()) break;

        // 从预加载任务队列中取出一批任务
        for (int i = 0; i < BATCH_SIZE && !preloaded_tasks_.empty(); ++i) {
          batch_tasks.push_back(std::move(preloaded_tasks_.back()));
          preloaded_tasks_.pop_back();
        }
      }

      if (!batch_tasks.empty()) {
        auto processor = std::make_unique<BatchProcessor>(
            this, std::move(batch_tasks), batch_sequence_++);
        batch_queue_.add_batch(std::move(processor));
        batch_tasks.clear();
      }
    } catch (const std::exception& e) {
      LOG_ERROR("Exception in process_tasks: %s", e.what());
    }
  }
}


void LogFileManager::init_thread_pool(int core_size, int max_size) {
  thread_pool_ = std::make_unique<ThreadPoolExecutor>();
  thread_pool_->init("LogProcessor", core_size, max_size, 1000);

  batch_queue_.stop_flag_ = &stop_flag_;  // 设置BatchQueue的stop_flag_指针
  result_queue_.stop_flag_ = &stop_flag_;  // 设置ResultQueue的stop_flag_指针

  thread_pool_->execute([this] { batch_queue_.process_batches(); });

}

void LogFileManager::shutdown() {
  if (!stop_flag_) {

    LOG_DEBUG("use thread max num: %d", thread_pool_->largest_pool_size());
    stop_flag_ = true;

    // 通知所有等待线程
//    task_cond_.notify_all();
//    batch_queue_.cv_.notify_all();
    result_queue_.cv_.notify_one();

    for (auto& thread : task_collector_threads_) {
      if (thread.joinable()) thread.join();
    }

    while (!batch_queue_.queue_.empty()) {
      LOG_DEBUG("Waiting for batch_queue_ to be empty...");
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    // 等待所有写入任务完成
    while (!result_queue_.pending_results_.empty()) {
      LOG_DEBUG("Waiting for pending_results_ to be empty...");
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

//    if (thread_pool_) {
//      thread_pool_->shutdown();
//      thread_pool_->await_termination();
//    }
    if (writer_thread_.joinable()) writer_thread_.join();

    auto endTime = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - start_time_).count();
    LOG_DEBUG("====Total execution time: %ld ms", duration);

    LOG_INFO("All threads joined, progress:");
    LOG_DEBUG("show progress in shutdown......");
    log_progress();
  }
}
void LogFileManager::wait_for_completion() {
  LOG_DEBUG("Waiting for all write tasks to complete...");

  // 等待所有任务进入批次队列
//  while (pending_tasks_ > 0) {
////    LOG_DEBUG("Pending tasks: %zu", pending_tasks_.load());
//    std::this_thread::sleep_for(std::chrono::milliseconds(10)); // 等待短暂时间后重试
//  }
//
//  // 等待所有批次任务完成
//  while (!batch_queue_.queue_.empty()) {
////    LOG_DEBUG("Batch queue not empty. Remaining batches: %zu", batch_queue_.queue_.size());
//    std::this_thread::sleep_for(std::chrono::milliseconds(10)); // 等待短暂时间后重试
//  }

  // 等待写入队列完成
  {
    std::unique_lock<std::mutex> lock(result_queue_.mutex_);
    while (!result_queue_.pending_results_.empty()) {
//      LOG_DEBUG("Pending results in result queue: %zu", result_queue_.pending_results_.size());
      result_queue_.cv_.wait_for(lock, std::chrono::milliseconds(10));
    }
  }

  LOG_DEBUG("All tasks and writes completed.");
}
void LogFileManager::preload_tasks(const std::vector<Task> &tasks) {
  try {
    {
//      std::lock_guard<std::mutex> lock(preload_mutex_);
      preloaded_tasks_ = tasks;  // 复制所有任务
    }
//    preloading_done_ = true;       // 标记预加载完成
//    task_cond_.notify_all();       // 通知 `process_tasks` 开始处理
  } catch (const std::exception& e) {
    LOG_ERROR("Exception during preload_tasks: %s", e.what());
    throw;  // 重新抛出异常
  }

  std::vector<Task> batch_tasks;
  batch_tasks.reserve(BATCH_SIZE);
  while (!preloaded_tasks_.empty()) {
    // 从预加载任务队列中取出一批任务
    for (int i = 0; i < BATCH_SIZE && !preloaded_tasks_.empty(); ++i) {
      batch_tasks.push_back(std::move(preloaded_tasks_.back()));
      preloaded_tasks_.pop_back();
    }
    // 准备好 preloaded_tasks_ 后马上就可以初始化 batch_queue_，从这里开始计时
    if (!batch_tasks.empty()) {
      auto processor = std::make_unique<BatchProcessor>(
          this, std::move(batch_tasks), batch_sequence_++);
      batch_queue_.add_batch(std::move(processor));
      batch_tasks.clear();
    }
  }

  // 1. 开始计时点移到这里
  start_time_ = std::chrono::high_resolution_clock::now();
  // 2. 接着启动 转换 + 写入 工作线程
  init_thread_pool(2,4);

  // 启动专门的写入线程
  writer_thread_ = std::thread([this] {
    result_queue_.process_writes(file_writer_.get(), this);
  });

}
