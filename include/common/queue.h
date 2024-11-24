//
// Created by Coonger on 2024/11/21.
//

#pragma once

namespace common {

/**
 * @brief 任务队列
 */

/**
 * @brief 任务队列接口
 * @ingroup Queue
 * @tparam T 任务数据类型。
 */
template <typename T>
class Queue
{
public:
  using value_type = T;

public:
  Queue()          = default;
  virtual ~Queue() = default;

  /**
   * @brief 在队列中放一个任务
   *
   * @param value 任务数据
   * @return int 成功返回0
   */
  virtual int push(value_type &&value) = 0;

  /**
   * @brief 从队列中取出一个任务
   *
   * @param value 任务数据
   * @return int 成功返回0。如果队列为空，也不是成功的
   */
  virtual int pop(value_type &value) = 0;

  /**
   * @brief 当前队列中任务的数量
   *
   * @return int 对列中任务的数量
   */
  virtual int size() const = 0;
};

}  // namespace common
