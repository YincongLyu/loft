
/**
    refer from: mysql/include/field_types.h
*/
#pragma once
#include <unordered_map>
#include <string_view>
/**
  Column types for MySQL
*/
enum enum_field_types
{
  MYSQL_TYPE_DECIMAL,
  MYSQL_TYPE_TINY,
  MYSQL_TYPE_SHORT,
  MYSQL_TYPE_LONG,
  MYSQL_TYPE_FLOAT,
  MYSQL_TYPE_DOUBLE,
  MYSQL_TYPE_NULL,
  MYSQL_TYPE_TIMESTAMP,
  MYSQL_TYPE_LONGLONG,
  MYSQL_TYPE_INT24,
  MYSQL_TYPE_DATE,
  MYSQL_TYPE_TIME,
  MYSQL_TYPE_DATETIME,
  MYSQL_TYPE_YEAR,
  MYSQL_TYPE_NEWDATE, /**< Internal to MySQL. Not used in protocol */
  MYSQL_TYPE_VARCHAR,
  MYSQL_TYPE_BIT,
  MYSQL_TYPE_TIMESTAMP2,
  MYSQL_TYPE_DATETIME2,   /**< Internal to MySQL. Not used in protocol */
  MYSQL_TYPE_TIME2,       /**< Internal to MySQL. Not used in protocol */
  MYSQL_TYPE_TYPED_ARRAY, /**< Used for replication only */
  MYSQL_TYPE_INVALID     = 243,
  MYSQL_TYPE_BOOL        = 244, /**< Currently just a placeholder */
  MYSQL_TYPE_JSON        = 245,
  MYSQL_TYPE_NEWDECIMAL  = 246,
  MYSQL_TYPE_ENUM        = 247,
  MYSQL_TYPE_SET         = 248,
  MYSQL_TYPE_TINY_BLOB   = 249,
  MYSQL_TYPE_MEDIUM_BLOB = 250,
  MYSQL_TYPE_LONG_BLOB   = 251,
  MYSQL_TYPE_BLOB        = 252,
  MYSQL_TYPE_VAR_STRING  = 253,
  MYSQL_TYPE_STRING      = 254,
  MYSQL_TYPE_GEOMETRY    = 255
};

// 定义映射关系
const std::unordered_map<std::string_view, enum_field_types> type_map = {{"SMALLINT", MYSQL_TYPE_SHORT},
    {"SHORT", MYSQL_TYPE_SHORT},
    {"MEDIUMINT", MYSQL_TYPE_INT24},
    {"INT", MYSQL_TYPE_LONG},
    {"BIGINT", MYSQL_TYPE_LONGLONG},
    {"FLOAT", MYSQL_TYPE_FLOAT},
    {"DOUBLE", MYSQL_TYPE_DOUBLE},
    {"DECIMAL", MYSQL_TYPE_NEWDECIMAL},
    {"NULL", MYSQL_TYPE_NULL},
    {"CHAR", MYSQL_TYPE_STRING},
    {"VARCHAR", MYSQL_TYPE_VARCHAR},
    {"TINYTEXT", MYSQL_TYPE_TINY_BLOB},
    {"TEXT", MYSQL_TYPE_BLOB},
    {"MEDIUMTEXT", MYSQL_TYPE_MEDIUM_BLOB},
    {"LONGTEXT", MYSQL_TYPE_LONG_BLOB},
    {"TINYBLOB", MYSQL_TYPE_TINY_BLOB},
    {"BLOB", MYSQL_TYPE_BLOB},
    {"MEDIUMBLOB", MYSQL_TYPE_MEDIUM_BLOB},
    {"LONGBLOB", MYSQL_TYPE_LONG_BLOB},
    {"TIMESTAMP", MYSQL_TYPE_TIMESTAMP},
    {"DATE", MYSQL_TYPE_DATE},
    {"TIME", MYSQL_TYPE_TIME},
    {"DATETIME", MYSQL_TYPE_DATETIME},
    {"YEAR", MYSQL_TYPE_YEAR},
    {"BIT", MYSQL_TYPE_BIT},
    {"ENUM", MYSQL_TYPE_ENUM},
    {"SET", MYSQL_TYPE_SET},
    {"JSON", MYSQL_TYPE_JSON}};
