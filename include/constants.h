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

#define MAX_SIZE_LOG_EVENT_STATUS                                          \
    (1U + 4 /* type, flags2 */ + 1U + 8 /* type, sql_mode */ + 1U + 1      \
     + 255 /* type, length, catalog */ + 1U                                \
     + 4 /* type, auto_increment */ + 1U + 6 /* type, charset */ + 1U + 1  \
     + MAX_TIME_ZONE_NAME_LENGTH /* type, length, time_zone */ + 1U        \
     + 2 /* type, lc_time_names_number */ + 1U                             \
     + 2 /* type, charset_database_number */ + 1U                          \
     + 8 /* type, table_map_for_update */ + 1U + 1                         \
     + 32 * 3 /* type, user_len, user */ + 1 + 255 /* host_len, host */    \
     + 1U + 1                                                              \
     + (MAX_DBS_IN_EVENT_MTS * (1 + NAME_LEN)) /* type, db_1, db_2, ... */ \
     + 1U + 3 /* type, microseconds */ + 1U                                \
     + 1 /* type, explicit_def..ts*/ + 1U + 8 /* type, xid of DDL */ + 1U  \
     + 2 /* type, default_collation_for_utf8mb4_number */ + 1U             \
     + 1 /* sql_require_primary_key */ + 1U                                \
     + 1 /* type, default_table_encryption */)

/**
  Maximum length of time zone name that we support (Time zone name is
  char(64) in db). mysqlbinlog needs it.
*/
#define MAX_TIME_ZONE_NAME_LENGTH (NAME_LEN + 1)

/**
   When the actual number of databases exceeds MAX_DBS_IN_EVENT_MTS
   the value of OVER_MAX_DBS_IN_EVENT_MTS is is put into the
   mts_accessed_dbs status.
*/
#define OVER_MAX_DBS_IN_EVENT_MTS 254

/**
   Query-log-event 最大的可以更改的 dbs 数量
*/
#define MAX_DBS_IN_EVENT_MTS 16
const uint64_t INVALID_XID = 0xffffffffffffffffULL;

/**
    start with magic number
*/
#define BINLOG_MAGIC        "\xfe\x62\x69\x6e"
#define BINLOG_MAGIC_SIZE   4
#define BIN_LOG_HEADER_SIZE 4U
#define BINLOG_CHECKSUM_LEN 4
#define BINLOG_CHECKSUM_ALG_DESC_LEN 1 /* 1 byte checksum alg descriptor */

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

struct MYSQL_LEX_CSTRING {
    const char *str;
    size_t length;
};
typedef struct MYSQL_LEX_CSTRING LEX_CSTRING;

constexpr int INT_OFFSET = 4;
constexpr size_t SQL_SIZE_ARRAY[] = {248,  744,  3304, 3208, 3208,
                                     2040, 2040, 1968, 264,  224};

} // namespace loft

#endif // CONSTANTS_H
