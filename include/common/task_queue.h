//
// Created by Coonger on 2024/11/21.
//

#include <string>
#include <vector>

#include <condition_variable>
#include <mutex>
#include <vector>

#include "type_def.h"

//struct Task {
//  std::string data_; // 存储任务数据
//  bool is_ddl_;      // 是否为 DDL 任务
//
//  // 添加默认构造函数
//  Task() : data_(), is_ddl_(false) {}
//
//  Task(std::string d, bool ddl) : data_(std::move(d)), is_ddl_(ddl) {}
//};

struct Task {
  std::vector<unsigned char> data_; // 直接使用 vector 存储原始数据
  bool is_ddl_;

  Task() : is_ddl_(false) {}

  // 使用移动语义
  Task(std::vector<unsigned char>&& d, bool ddl)
      : data_(std::move(d)), is_ddl_(ddl) {}
};


/**
 * @brief 生产者任务队列
 */
template <typename T>
class TaskQueue {
public:
  explicit TaskQueue(size_t capacity) : capacity_(capacity), head_(0), tail_(0), size_(0), buffer_(capacity) {}

  // 写入任务
//  bool write(const T &task) {
//    std::unique_lock<std::mutex> lock(mutex_);
//    cond_not_full_.wait(lock, [this] { return size_ < capacity_; }); // 等待有空位
//
//    buffer_[tail_] = task;
//    tail_ = (tail_ + 1) % capacity_;
//    ++size_;
//
//    cond_not_empty_.notify_one(); // 通知有新任务
//    return true;
//  }

  // 添加移动版本的写入方法
  bool write(T&& task) {
    std::unique_lock<std::mutex> lock(mutex_);
    cond_not_full_.wait(lock, [this] { return size_ < capacity_; });

    buffer_[tail_] = std::move(task);
    tail_ = (tail_ + 1) % capacity_;
    ++size_;

    cond_not_empty_.notify_one();
    return true;
  }

  // 读取任务
  bool read(T &task) {
    std::unique_lock<std::mutex> lock(mutex_);
    cond_not_empty_.wait(lock, [this] { return size_ > 0; }); // 等待有任务

    task = buffer_[head_];
    head_ = (head_ + 1) % capacity_;
    --size_;

    cond_not_full_.notify_one(); // 通知有空位
    return true;
  }

private:
  size_t capacity_;
  size_t head_;
  size_t tail_;
  size_t size_;
  std::vector<T> buffer_;

  std::mutex mutex_;
  std::condition_variable cond_not_empty_;
  std::condition_variable cond_not_full_;
};
