//
// Created by Coonger on 2024/11/21.
//

#pragma once

#include <functional>

namespace common {

/**
 * @brief 可执行对象接口
 */
class Runnable
{
public:
  Runnable()          = default;
  virtual ~Runnable() = default;

  virtual void run() = 0;
};

/**
 * @brief 可执行对象适配器，方便使用lambda表达式
 * @ingroup ThreadPool
 */
class RunnableAdaptor : public Runnable
{
public:
  RunnableAdaptor(std::function<void()> callable) : callable_(callable) {}

  void run() override { callable_(); }

private:
  std::function<void()> callable_;
};


}  // namespace common
