//
// Created by Coonger on 2024/11/2.
//
#pragma once

#include <string>
#include <unordered_map>

#define MAGIC_NUM_SIZE 4
// 19 + (2 + 50 + 4 + 1 + 41)
#define FDE_SIZE 117
// 19 + (8 + 9)
#define ROTATE_SIZE 36

// *** binlog file write configuration ***
#define DEFAULT_BINLOG_FILE_DIR "/home/yincong/collectBin/"
#define DEFAULT_BINLOG_FILE_NAME_PREFIX "ON"
#define DEFAULT_BINLOG_FILE_SIZE (1024 * 1024)
// 200 byte 是一个安全数
#define WRITE_THRESHOLD  200
#define BINLOG_FILE_WRITE_SAFE_SIZE (BINLOG_FILE_SIZE - WRITE_THRESHOLD)
#define BINLOG_FILE_TTL 30s

#define THREAD_NUM 1

// arbitrary
#define DML_TABLE_ID 13


// *** common header ***
#define SERVER_ID 100

// *** fde event ***
#define BINLOG_VERSION 4
#define SERVER_VERSION_STR "8.0.32-debug"

// *** gtid event ***
#define ORIGINAL_SERVER_VERSION 80032
#define IMMEDIATE_SERVER_VERSION 80032

// ***  query event ****
#define USER       ""
#define HOST       "127.0.0.1"
#define THREAD_ID  10000
#define EXEC_TIME  2
#define ERROR_CODE 0

#define DML_QUERY_STR "BEGIN"
#define TIME_ZONE "SYSTEM"

// IO size 一般规定为 4k，适合现代 OS disk 的读写
constexpr const size_t IO_SIZE{4096};





