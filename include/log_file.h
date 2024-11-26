//
// Created by Coonger on 2024/11/4.
//

#pragma once

#include <cstring>       // std::string
#include <filesystem>    // std::filesystem::path>
#include <system_error>  // std::error_code
#include <utility>       // std::pair
#include <map>           // std::map
#include <future>
#include <queue>

#include "transform_manager.h"
#include "binlog.h"
#include "events/abstract_event.h"
#include "common/init_setting.h"
#include "common/rc.h"
#include "common/task_queue.h"

#include "common/thread_pool_executor.h"
using namespace common;

namespace loft {

/**
 * @brief 负责处理一个日志文件，包括读取和写入
 */
class RedoLogFileReader
{
public:
  RedoLogFileReader()  = default;
  ~RedoLogFileReader() {
    close();
  }

  auto open(const char *filename) -> RC;
  auto close() -> RC;
  auto readFromFile(const std::string &fileName) -> std::pair<std::unique_ptr<char[]>, size_t>;

private:
  int         fd_ = -1;
  std::string filename_;
};

/**
 * @brief 负责写入一个日志文件， 【封装 我写的 MYSQL_BIN_LOG 类】
 */
class BinLogFileWriter
{
public:
  BinLogFileWriter()  = default;
  ~BinLogFileWriter() {
    close();
  }

  /**
   * @brief 打开一个日志文件
   * @param filename 日志文件名
   */
  RC open(const char *filename, size_t max_file_size);

  /// @brief 关闭当前文件
  RC close();

  /// @brief 写入一条 event
  RC write(AbstractEvent &event);

  /**
   * @brief 文件是否已经写满。按照剩余空间来判断
   */
  bool full() const;

  //    string to_string() const;

  const char *filename() const { return filename_.c_str(); }

  auto get_binlog() -> MYSQL_BIN_LOG * { return bin_log_.get(); }

private:
  std::string filename_;  /// 日志文件名
                          //    int fd_ = -1;      /// 日志文件描述符
  int last_ckp_ = 0;      /// 写入的最后一条SQL的scn, seq, ckp

  std::unique_ptr<MYSQL_BIN_LOG> bin_log_;  /// 封装的 MYSQL_BIN_LOG 类
};

/**
 * @brief 管理所有的 binlog 日志文件, 【封装我的 mgr 类】
 * @details binlog 日志文件都在某个目录下，使用固定的前缀 作为文件名如
 * ON.000001。 每个 binlog 日志文件有最大字节数要求
 */
class LogFileManager
{
public:
  LogFileManager();
  ~LogFileManager();

  // TODO 此处 实现 3 个 必要接口

  /// 接口一：
  /**
   * @brief 初始化写入 binlog 文件的目录路径 和 文件大小
   *
   * @param directory 日志文件目录
   * @param file_name_prefix 日志文件前缀
   * @param max_file_size_per_file 一个文件的最大字节数
   */
  RC init(const char *directory, const char *file_name_prefix, uint64 max_file_size_per_file);

  RC transform(const char *file_name, bool is_ddl);
  /// 接口二：
  /**
   * @brief binlog格式转换 并写入文件
   * @param buf 待转换的一条 sql
   * @param is_ddl 是否是 ddl 语句
   * @return
   */
  RC transform(std::vector<unsigned char> &&buf, bool is_ddl);
  std::future<RC> transformAsync(std::vector<unsigned char> &&buf, bool is_ddl);
  std::future<RC> transformAsync2(std::vector<unsigned char> &&buf, bool is_ddl);

      /// 接口三：
  /**
   * @brief 从文件名称的后缀中获取这是第几个 binlog 文件，文件索引信息保存在log_files_里
   * @details 如果日志文件名不符合要求，就返回失败。实际上返回 3 个
   * scn、seq、ckp 字段，只用保存 ckp，在每次转换前，存入到 file_ckp_里
   */
  RC get_last_status_from_filename(const std::string &filename, uint64 &scn, uint32 &seq, std::string &ckp);

  /// ****************** binlog 文件的管理 ***************
  /**
   * @brief 从文件名中获取 文件编号
   * @param filename
   * @param fileno
   */
  RC get_fileno_from_filename(const std::string &filename, uint32 &fileno);

  /**
   * @brief 获取最新的一个日志文件名
   * @details
   * 如果当前有文件就获取最后一个日志文件，否则创建一个日志文件，也就是第一个日志文件
   */
  RC last_file(BinLogFileWriter &file_writer);

  /**
   * @brief 获取一个新的日志文件名
   * @details
   * 获取下一个日志文件名。通常是上一个日志文件写满了，通过这个接口生成下一个日志文件
   */
  RC next_file(BinLogFileWriter &file_writer);

  /**
   * @brief 写binlog索引文件
   * @param filename
   */
  RC write_filename2index(std::string &filename);

  /**
   * @brief 后台单独开启一个线程，专门清理 binlog 文件，如达到设置的时间 1 min，就清理 30% 的 binlog 文件
   * @details 清理 log_files_ 和 file_ckp_，防止膨胀，remove 文件
   */
  RC clean_files();

  auto get_directory() -> const char * { return directory_.c_str(); }
  auto get_file_prefix() -> const char * { return file_prefix_; }
  auto get_file_max_size() -> size_t { return max_file_size_per_file_; }

  auto get_log_files() -> std::map<uint32, std::filesystem::path> & { return log_files_; }
  auto get_file_reader() -> RedoLogFileReader * { return file_reader_.get(); }
  auto get_file_writer() -> BinLogFileWriter * { return file_writer_.get(); }
  auto get_transform_manager() -> LogFormatTransformManager * { return transform_manager_.get(); }

  /**
   * @brief 等待 BatchQueue 和 ResultQueue 的任务都完成
   */
  void wait_for_completion();

  class BatchProcessor : public Runnable {
  public:
    BatchProcessor(LogFileManager* manager, std::vector<Task>&& tasks, size_t sequence)
        : manager_(manager), tasks_(std::move(tasks)), batch_sequence_(sequence) {}

    void run() override {
      auto result = std::make_unique<BatchResult>(batch_sequence_);

      for (const auto& task : tasks_) {
        if (task.is_ddl_) {
          const DDL* ddl = GetDDL(task.data_.data());
          manager_->file_ckp_[manager_->last_file_no_] =
              ddl->check_point()->c_str();

          // 转换但不直接写入文件
          auto events = manager_->get_transform_manager()->transformDDL(ddl);
          for (auto &event : events) {
            result->transformed_data.push_back(transform_to_buffer(event.get()));
          }

        } else {
          const DML* dml = GetDML(task.data_.data());
          manager_->file_ckp_[manager_->last_file_no_] =
              dml->check_point()->c_str();

          // 转换但不直接写入文件
          auto events = manager_->get_transform_manager()->transformDML(dml);
          for (auto &event : events) {
            result->transformed_data.push_back(transform_to_buffer(event.get()));
          }
        }
      }

      // 将结果加入写入队列
      manager_->result_queue_.add_result(std::move(result));

      manager_->processed_tasks_ += tasks_.size();
    }

  private:
    // 将转换后的数据存入内存
    std::vector<uchar> transform_to_buffer(AbstractEvent* event) {
      std::vector<uchar> buffer(LOG_EVENT_HEADER_LEN + event->get_data_size(), 0);
      // 将event写入buffer
      event->write_to_buffer(buffer.data());
      return buffer;
    }

  private:
    LogFileManager* manager_;
    std::vector<Task> tasks_;
    size_t batch_sequence_;  // 批次序号，用于确保顺序执行
  };

  // BatchQueue修改为允许并发处理
  struct BatchQueue {
    std::mutex mutex_;
    std::condition_variable cv_;
    std::queue<std::unique_ptr<BatchProcessor>> queue_;
    std::atomic<bool>* stop_flag_;

    void add_batch(std::unique_ptr<BatchProcessor> batch) {
      std::lock_guard<std::mutex> lock(mutex_);
      queue_.push(std::move(batch));
      cv_.notify_all();  // 通知所有等待的线程
    }

    void process_batches() {
      while (!(*stop_flag_)) {
        std::unique_ptr<BatchProcessor> batch;
        {
          std::unique_lock<std::mutex> lock(mutex_);
          if (cv_.wait_for(lock,
                  std::chrono::milliseconds(100),
                  [this] { return *stop_flag_ || !queue_.empty(); })) {
            if (*stop_flag_ && queue_.empty()) {
              break;
            }
            if (!queue_.empty()) {
              batch = std::move(queue_.front());
              queue_.pop();
            }
          }
        }
        if (batch) {
          batch->run();
        }
      }

      // 主动清空剩余的任务
      while (!queue_.empty()) {
        auto batch = std::move(queue_.front());
        queue_.pop();
        batch->run();
      }

    }
  };

  // 用于存储转换后的数据
  struct BatchResult {
    size_t sequence;
    std::vector<std::vector<uchar>> transformed_data;  // 每个event转换后的数据

    BatchResult(size_t seq) : sequence(seq) {}
  };

  // 管理已转换完成待写入的结果队列
  struct ResultQueue {
    std::mutex mutex_;
    std::condition_variable cv_;
    std::unordered_map<size_t, std::unique_ptr<BatchResult>> pending_results_;
    size_t next_write_sequence_{0};
    std::atomic<bool>* stop_flag_;


    void add_result(std::unique_ptr<BatchResult> result) {
      std::lock_guard<std::mutex> lock(mutex_);
      pending_results_[result->sequence] = std::move(result);
      // 只有当下一个期望序号的结果到达时才通知
      if (pending_results_.count(next_write_sequence_) > 0) {
        cv_.notify_one();
      }
    }

    // 专门的文件写入线程
    void process_writes(BinLogFileWriter* writer, LogFileManager* manager) {
      while (!(*stop_flag_)) {
        std::unique_ptr<BatchResult> result;
        {
          std::unique_lock<std::mutex> lock(mutex_);
          if (cv_.wait_for(lock,
                  std::chrono::milliseconds(100),
                  [this] {
                    return *stop_flag_ ||
                           pending_results_.count(next_write_sequence_) > 0;
                  })) {
            if (*stop_flag_ && pending_results_.empty()) {
              break;
            }
            if (pending_results_.count(next_write_sequence_) > 0) {
              result = std::move(pending_results_[next_write_sequence_]);
              pending_results_.erase(next_write_sequence_);
              next_write_sequence_++;
            }
          }
        }

        if (result) { // 检查是否要切换文件
          manager->written_tasks_ += result->transformed_data.size();

          // 按顺序写入文件
          std::lock_guard<std::mutex> write_lock(manager->writer_mutex_);
          for (auto& data : result->transformed_data) {
            // 切换文件
            if (!writer->get_binlog()->remain_bytes_safe()) {
              manager->next_file(*writer);
            }

            // 写入实际数据, 填充 common_header 中的 log_pos 字段
            if (writer->get_binlog() == nullptr) {
              LOG_ERROR("bin_log_ is not initialized.");
              return;
            }

            uint64_t current_pos = writer->get_binlog()->get_bytes_written();
            uint64_t next_pos = current_pos + data.size();
            int4store(data.data() + LOG_POS_OFFSET, next_pos);

            writer->get_binlog()->write(data.data(), data.size());
          }
        }
      }
    }

  };
  /**
   * @brief 追踪处理进度
   */
  void log_progress() {
    LOG_DEBUG("Pending tasks: %zu, Processed SQL num: %zu, Written Events num: %zu",
                 pending_tasks_.load(),
                 processed_tasks_.load(),
                 written_tasks_.load());
  }

  void shutdown();

  void preload_tasks(const std::vector<Task>& tasks); // 预加载任务
  size_t get_processed_sql_num() const {
    return processed_tasks_.load(std::memory_order_relaxed);
  }


private:
  void process_tasks();
  void process_tasks2();
  void init_thread_pool(int core_size, int max_size);


private:
  const char *file_prefix_ = DEFAULT_BINLOG_FILE_NAME_PREFIX;
  const char *file_dot_    = ".";
  std::string file_suffix_;  // 这会是一个递增的后缀数字

  std::string index_suffix_ = ".index";
  int index_fd_ = -1; // init()后，就打开 index 文件

  std::filesystem::path directory_              = DEFAULT_BINLOG_FILE_DIR;   /// 日志文件存放的目录
  size_t                max_file_size_per_file_ = DEFAULT_BINLOG_FILE_SIZE;  /// 一个文件的最大字节数

  std::map<uint32, std::filesystem::path> log_files_;  /// file_no 和 日志文件名 的映射
  std::map<uint32, std::string> file_ckp_; /// file_no 和 ckp 的映射
  uint32 last_file_no_ = 0;

  std::unique_ptr<RedoLogFileReader>         file_reader_;

  // 1. 生产者
  std::shared_ptr<TaskQueue<Task>>   ring_buffer_;
  std::condition_variable task_cond_; // event_trigger 通知
  std::mutex task_mutex_;
  std::vector<std::thread> task_collector_threads_;  // 用于运行process_tasks的线程
  static constexpr size_t BATCH_SIZE = 2000; // 批量处理的大小
  std::atomic<size_t> pending_tasks_{0}; // 跟踪待处理任务数量

  std::atomic<bool> stop_flag_{false};  // 用于控制线程停止
  // 2. 转换计算
  std::unique_ptr<LogFormatTransformManager> transform_manager_;
  std::unique_ptr<ThreadPoolExecutor> thread_pool_;
  BatchQueue batch_queue_;
  std::atomic<size_t> batch_sequence_{0};

  // 3. 共享的文件写入器
  std::unique_ptr<BinLogFileWriter> file_writer_;
  std::mutex writer_mutex_;  // 保护文件写入
  ResultQueue result_queue_;
  std::thread writer_thread_;  // 专门的写入线程

  // 追踪进度
  std::atomic<size_t> processed_tasks_{0};
  std::atomic<size_t> written_tasks_{0};

  // 预加载 version
  std::vector<Task> preloaded_tasks_;   // 预加载的 SQL 任务队列
  std::atomic<bool> preloading_done_;  // 标志是否完成预加载
  std::mutex preload_mutex_;

  std::chrono::time_point<std::chrono::high_resolution_clock> start_time_;
};


}  // namespace loft
