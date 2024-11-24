//
// Created by Coonger on 2024/11/2.
//

#pragma once

#include <ctime>
#include <sys/time.h>

#include <cstdio>
#include <cstdlib>
#include <cstring>

#define LOG_LOG_TIME_FORMAT "%Y-%m-%d %H:%M:%S"

#define GET_TIME                    \
  time_t t       = ::time(nullptr); \
  tm    *curTime = localtime(&t);   \
  char   time_str[32];              \
  ::strftime(time_str, 32, LOG_LOG_TIME_FORMAT, curTime);

#define TIME time_str

#define DEBUG(format, ...) printf(format, ##__VA_ARGS__)

#define __SHORT_FILE__ (strrchr(__FILE__, '/') ? (strrchr(__FILE__, '/') + 1) : __FILE__)

// 定义日志级别

#define LOG_LEVEL_OFF (0)
#define LOG_LEVEL_FATAL (1)
#define LOG_LEVEL_ERROR (2)
#define LOG_LEVEL_INFO (100)
#define LOG_LEVEL_DEBUG (4)

#define level LOG_LEVEL_DEBUG

#if level >= LOG_LEVEL_FATAL
#define LOG_FATAL(format, ...)                                                                        \
  do {                                                                                                \
    GET_TIME                                                                                          \
    DEBUG("\033[;31m[FATAL] %s %s:%d: " format "\n\033[0m", TIME, __FILE__, __LINE__, ##__VA_ARGS__); \
    fflush(stdout);                                                                                   \
    abort();                                                                                          \
  } while (0)
#else
#define LOG_FATAL(format, ...)
#endif

#if level >= LOG_LEVEL_ERROR
#define LOG_ERROR(format, ...)                                                                        \
  do {                                                                                                \
    GET_TIME                                                                                          \
    DEBUG("\033[;31m[ERROR] %s %s:%d: " format "\n\033[0m", TIME, __FILE__, __LINE__, ##__VA_ARGS__); \
  } while (0)
#else
#define LOG_ERROR(format, ...)
#endif

#if level >= LOG_LEVEL_INFO
#define LOG_INFO(format, ...)                                                                               \
  do {                                                                                                      \
    GET_TIME                                                                                                \
    DEBUG("\033[;34m[INFO]  %s %s:%d: " format "\n\033[0m", TIME, __SHORT_FILE__, __LINE__, ##__VA_ARGS__); \
  } while (0)
#else
#define LOG_INFO(format, ...)
#endif

#if level >= LOG_LEVEL_DEBUG
#define LOG_DEBUG(format, ...)                                                                              \
  do {                                                                                                      \
    GET_TIME                                                                                                \
    DEBUG("\033[;33m[DEBUG] %s %s:%d: " format "\n\033[0m", TIME, __SHORT_FILE__, __LINE__, ##__VA_ARGS__); \
  } while (0)
#else
#define LOG_DEBUG(format, ...)
#endif
