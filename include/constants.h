// constants.h
#ifndef CONSTANTS_H
#define CONSTANTS_H
#include <bits/types.h>
#include <cassert>
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <unistd.h>

namespace loft {

using uint8_t = __uint8_t;
using uint16_t = __uint16_t;

using int32_t = signed int;
using uint32_t = __uint32_t;

using int64_t = signed long int;
using uint64_t = __uint64_t;

using my_off_t = unsigned long long;
using uchar = unsigned char;

typedef int File; /* File descriptor */

/*
  Io buffer size; Must be a power of 2 and a multiple of 512. May be
  smaller what the disk page size. This influences the speed of the
  isam btree library. eg to big to slow.
*/
constexpr const size_t IO_SIZE{4096};
#define binlog_checksum_options BINLOG_CHECKSUM_ALG_CRC32

#define BINLOG_VERSION    4
#define ST_SERVER_VER_LEN 50

/**
    start with magic number
*/
#define BINLOG_MAGIC        "\xfe\x62\x69\x6e"
#define BINLOG_MAGIC_SIZE   4
#define BIN_LOG_HEADER_SIZE 4U
#define BINLOG_CHECKSUM_LEN 4

/** start event post-header (for v3 and v4) */
#define ST_BINLOG_VER_OFFSET        0
#define ST_SERVER_VER_OFFSET        2
#define ST_CREATED_OFFSET           (ST_SERVER_VER_OFFSET + ST_SERVER_VER_LEN)
#define ST_COMMON_HEADER_LEN_OFFSET (ST_CREATED_OFFSET + 4)
/**
    event common-header offset
*/

#define EVENT_TYPE_OFFSET    4
#define SERVER_ID_OFFSET     5
#define EVENT_LEN_OFFSET     9
#define LOG_POS_OFFSET       13
#define FLAGS_OFFSET         17
#define LOG_EVENT_HEADER_LEN 19U /* the fixed header length */

/* General constants*/
// copy from include/my_io.h
#define FN_LEN     256 /* Max file name len */
#define FN_HEADLEN 253 /* Max length of filepart of file name */
#define FN_REFLEN  512 /* Max length of full path-name */

#define SYSTEM_CHARSET_MBMAXLEN 3
#define NAME_CHAR_LEN           64 /* Field/table name length */
#define NAME_LEN                (NAME_CHAR_LEN * SYSTEM_CHARSET_MBMAXLEN)

#define MYSQL_DATA_HOME "./";

constexpr int INT_OFFSET = 4;
constexpr int SQL_SIZE_ARRAY[] = {248,  744,  3304, 3208, 3208,
                                  2040, 2040, 1968, 264,  224};

} // namespace loft

#endif // CONSTANTS_H
