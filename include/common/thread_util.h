//
// Created by Coonger on 2024/11/21.
//

/**
 * @brief refer from: https://github.com/oceanbase/miniob/blob/main/src/common/thread/thread_util.h
 */
#pragma once

namespace common {

/**
 * @brief 设置当前线程的名字
 * @details 设置当前线程的名字可以帮助调试多线程程序，比如在gdb或者 top -H命令可以看到线程名字。
 * pthread_setname_np在Linux和Mac上实现不同。Linux上可以指定线程号设置名称，但是Mac上不行。
 * @param name 线程的名字。按照linux手册中描述，包括\0在内，不要超过16个字符
 * @return int 设置成功返回0
 */
int thread_set_name(const char *name);

}  // namespace common
