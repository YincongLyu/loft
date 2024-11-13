//
// Created by Coonger on 2024/11/13.
//
#pragma once

#include <unordered_map>

#include "common/type_def.h"
#include "sql/mysql_fields.h"

/******************************************************************************
                     Event Common Footer
******************************************************************************/
#define binlog_checksum_options BINLOG_CHECKSUM_ALG_CRC32

/******************************************************************************
                     Format-Description-Event
******************************************************************************/
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


/******************************************************************************
                     Query-log-event
******************************************************************************/

#define MAX_DBS_IN_EVENT_MTS 16 // 最大的可以更改的 dbs 数量
const uint64 INVALID_XID = 0xffffffffffffffffULL; // 最大事务号


/******************************************************************************
                     Magic number
******************************************************************************/
#define BINLOG_MAGIC                 "\xfe\x62\x69\x6e" // binlog文件起始 4 个 byte 是 magic number
#define BINLOG_MAGIC_SIZE            4
#define BIN_LOG_HEADER_SIZE          4U
#define BINLOG_CHECKSUM_LEN          4
#define BINLOG_CHECKSUM_ALG_DESC_LEN 1 /* 1 byte checksum alg descriptor */


/******************************************************************************
                     Event Common Header
******************************************************************************/

/** start event post-header (for v3 and v4) */
#define ST_BINLOG_VER_OFFSET        0
#define ST_SERVER_VER_OFFSET        2
#define ST_CREATED_OFFSET           (ST_SERVER_VER_OFFSET + ST_SERVER_VER_LEN)
#define ST_COMMON_HEADER_LEN_OFFSET (ST_CREATED_OFFSET + 4)

#define EVENT_TYPE_OFFSET    4
#define SERVER_ID_OFFSET     5
#define EVENT_LEN_OFFSET     9
#define LOG_POS_OFFSET       13
#define FLAGS_OFFSET         17
#define LOG_EVENT_HEADER_LEN 19U /* the fixed header length */

/******************************************************************************
          File General constants | refer from include/my_io.h
******************************************************************************/

#define FN_LEN     256 /* Max file name len */
#define FN_HEADLEN 253 /* Max length of filepart of file name */
#define FN_REFLEN  512 /* Max length of full path-name */

#define SYSTEM_CHARSET_MBMAXLEN 3
#define NAME_CHAR_LEN           64 /* Field/table name length */
#define NAME_LEN                (NAME_CHAR_LEN * SYSTEM_CHARSET_MBMAXLEN)

/******************************************************************************
          decimal.cpp
******************************************************************************/

#define DIG_PER_DEC1    9
#define E_DEC_OK        0
#define E_DEC_TRUNCATED 1
#define E_DEC_OVERFLOW  2
#define E_DEC_DIV_ZERO  4
#define E_DEC_BAD_NUM   8
#define E_DEC_OOM       16
#define ROUND_UP(X)     (((X) + DIG_PER_DEC1 - 1) / DIG_PER_DEC1)

#define EXTRA_ROW_INFO_LEN_OFFSET        0
#define EXTRA_ROW_INFO_FORMAT_OFFSET     1
#define EXTRA_ROW_INFO_HEADER_LENGTH     2
#define EXTRA_ROW_INFO_MAX_PAYLOAD       (255 - EXTRA_ROW_INFO_HEADER_LENGTH)
#define ROWS_MAPID_OFFSET                0
#define ROWS_FLAGS_OFFSET                6
#define ROWS_VHLEN_OFFSET                8
#define EXTRA_ROW_INFO_TYPECODE_LENGTH   1
#define EXTRA_ROW_PART_INFO_VALUE_LENGTH 2

static const std::unordered_map<std::string, enum_field_types> type_map = {

    {  "SMALLINT",       MYSQL_TYPE_SHORT}, // 2 byte
    {     "SHORT",       MYSQL_TYPE_SHORT}, // 2 byte
    { "MEDIUMINT",       MYSQL_TYPE_INT24}, // 3 byte
    {       "INT",        MYSQL_TYPE_LONG}, // 4 byte
    {    "BIGINT",    MYSQL_TYPE_LONGLONG}, // 8 byte

    {     "FLOAT",       MYSQL_TYPE_FLOAT},
    {    "DOUBLE",      MYSQL_TYPE_DOUBLE},
    {   "DECIMAL",  MYSQL_TYPE_NEWDECIMAL},

    {      "NULL",        MYSQL_TYPE_NULL},
    {      "CHAR",      MYSQL_TYPE_STRING},
    {   "VARCHAR",     MYSQL_TYPE_VARCHAR},

    {  "TINYTEXT",   MYSQL_TYPE_TINY_BLOB},
    {      "TEXT",        MYSQL_TYPE_BLOB},
    {"MEDIUMTEXT", MYSQL_TYPE_MEDIUM_BLOB},
    {  "LONGTEXT",   MYSQL_TYPE_LONG_BLOB},

    {  "TINYBLOB",   MYSQL_TYPE_TINY_BLOB},
    {      "BLOB",        MYSQL_TYPE_BLOB},
    {"MEDIUMBLOB", MYSQL_TYPE_MEDIUM_BLOB},
    {  "LONGBLOB",   MYSQL_TYPE_LONG_BLOB},

    { "TIMESTAMP",   MYSQL_TYPE_TIMESTAMP},
    {      "DATE",        MYSQL_TYPE_DATE},
    {      "TIME",        MYSQL_TYPE_TIME},
    {  "DATETIME",    MYSQL_TYPE_DATETIME},
    {      "YEAR",        MYSQL_TYPE_YEAR},

    {       "BIT",         MYSQL_TYPE_BIT},
    {      "ENUM",        MYSQL_TYPE_ENUM},
    {       "SET",         MYSQL_TYPE_SET},

    {      "JSON",        MYSQL_TYPE_JSON},
};