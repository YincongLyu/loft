//
// Created by Coonger on 2024/11/21.
//

#pragma once

#include "common/queue.h"
#include <mutex>
#include <queue>

namespace common {

/**
 * @brief 一个十分简单的线程安全的任务队列
 * @tparam T 任务数据类型。
 */
template <typename T>
class SimpleQueue : public Queue<T>
{
public:
  using value_type = T;

public:
  SimpleQueue() : Queue<T>() {}
  virtual ~SimpleQueue() {}

  //! @copydoc Queue::emplace
  int push(value_type &&value) override;
  //! @copydoc Queue::pop
  int pop(value_type &value) override;
  //! @copydoc Queue::size
  int size() const override;

private:
  std::mutex             mutex_;
  std::queue<value_type> queue_;
};

template <typename T>
int SimpleQueue<T>::push(T &&value)
{
  std::lock_guard<std::mutex> lock(mutex_);
  queue_.push(std::move(value));
  return 0;
}

template <typename T>
int SimpleQueue<T>::pop(T &value)
{
  std::lock_guard<std::mutex> lock(mutex_);
  if (queue_.empty()) {
    return -1;
  }

  value = std::move(queue_.front());
  queue_.pop();
  return 0;
}

template <typename T>
int SimpleQueue<T>::size() const
{
  return queue_.size();
}

}  // namespace common
