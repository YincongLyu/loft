
#pragma once

#include "field_types.h"

/**
  @file field_common_properties.h

  @brief This file contains basic method for field types.

  @note copy from: mysql-src
*/

/**
   Tests if field type is an integer

   @param type Field type, as returned by field->type()

   @returns true if integer type, false otherwise
 */
inline bool is_integer_type(enum_field_types type) {
    switch (type) {
        case MYSQL_TYPE_TINY:
        case MYSQL_TYPE_SHORT:
        case MYSQL_TYPE_INT24:
        case MYSQL_TYPE_LONG:
        case MYSQL_TYPE_LONGLONG:
            return true;
        default:
            return false;
    }
}

/**
  Tests if field type is a numeric type

  @param type Field type, as returned by field->type()

  @returns true if numeric type, false otherwise
*/
inline bool is_numeric_type(enum_field_types type) {
    switch (type) {
        case MYSQL_TYPE_TINY:
        case MYSQL_TYPE_SHORT:
        case MYSQL_TYPE_INT24:
        case MYSQL_TYPE_LONG:
        case MYSQL_TYPE_LONGLONG:
        case MYSQL_TYPE_FLOAT:
        case MYSQL_TYPE_DOUBLE:
        case MYSQL_TYPE_DECIMAL:
        case MYSQL_TYPE_NEWDECIMAL:
            return true;
        default:
            return false;
    }
}

/**
  Tests if field type is a string type

  @param type Field type, as returned by field->type()

  @returns true if string type, false otherwise
*/
inline bool is_string_type(enum_field_types type) {
    switch (type) {
        case MYSQL_TYPE_VARCHAR:
        case MYSQL_TYPE_VAR_STRING:
        case MYSQL_TYPE_STRING:
        case MYSQL_TYPE_TINY_BLOB:
        case MYSQL_TYPE_MEDIUM_BLOB:
        case MYSQL_TYPE_LONG_BLOB:
        case MYSQL_TYPE_BLOB:
        case MYSQL_TYPE_ENUM:
        case MYSQL_TYPE_SET:
        case MYSQL_TYPE_JSON:
            return true;
        default:
            return false;
    }
}

/**
  Tests if field type is temporal, i.e. represents
  DATE, TIME, DATETIME, TIMESTAMP or YEAR types in SQL.

  @param type    Field type, as returned by field->type().
  @retval true   If field type is temporal
  @retval false  If field type is not temporal
*/
inline bool is_temporal_type(enum_field_types type) {
    switch (type) {
        case MYSQL_TYPE_TIME:
        case MYSQL_TYPE_DATETIME:
        case MYSQL_TYPE_TIMESTAMP:
        case MYSQL_TYPE_DATE:
        case MYSQL_TYPE_NEWDATE:
        case MYSQL_TYPE_YEAR:
            return true;
        default:
            return false;
    }
}

/**
  Tests if field type is temporal and has time part,
  i.e. represents TIME, DATETIME or TIMESTAMP types in SQL.

  @param type    Field type, as returned by field->type().
  @retval true   If field type is temporal type with time part.
  @retval false  If field type is not temporal type with time part.
*/
inline bool is_temporal_type_with_time(enum_field_types type) {
    switch (type) {
        case MYSQL_TYPE_TIME:
        case MYSQL_TYPE_DATETIME:
        case MYSQL_TYPE_TIMESTAMP:
            return true;
        default:
            return false;
    }
}

/**
  Tests if field type is temporal and has date part,
  i.e. represents DATE, DATETIME or TIMESTAMP types in SQL.

  @param type    Field type, as returned by field->type().
  @retval true   If field type is temporal type with date part.
  @retval false  If field type is not temporal type with date part.
*/
inline bool is_temporal_type_with_date(enum_field_types type) {
    // A type which is_temporal_type() but not is_temporal_type_with_date() ?
    assert(type != MYSQL_TYPE_NEWDATE);
    switch (type) {
        case MYSQL_TYPE_DATE:
        case MYSQL_TYPE_DATETIME:
        case MYSQL_TYPE_TIMESTAMP:
            return true;
        default:
            return false;
    }
}

/**
  Tests if field type is temporal and has date and time parts,
  i.e. represents DATETIME or TIMESTAMP types in SQL.

  @param type    Field type, as returned by field->type().
  @retval true   If field type is temporal type with date and time parts.
  @retval false  If field type is not temporal type with date and time parts.
*/
inline bool is_temporal_type_with_date_and_time(enum_field_types type) {
    switch (type) {
        case MYSQL_TYPE_DATETIME:
        case MYSQL_TYPE_TIMESTAMP:
            return true;
        default:
            return false;
    }
}

/**
   Recognizer for concrete data type (called real_type for some reason),
   returning true if it is one of the TIMESTAMP types.
*/
inline bool is_timestamp_type(enum_field_types type) {
    return type == MYSQL_TYPE_TIMESTAMP || type == MYSQL_TYPE_TIMESTAMP2;
}

/**
  Test if the field type contains information on being signed/unsigned.
  This includes numeric but also YEAR that still contains sign modifiers
  even if ignored.

  @param type Field type, as returned by field->type()

  @returns true if the type contains info on being signed/unsigned
*/
inline bool has_signedess_information_type(enum_field_types type) {
    switch (type) {
        case MYSQL_TYPE_TINY:
        case MYSQL_TYPE_SHORT:
        case MYSQL_TYPE_INT24:
        case MYSQL_TYPE_LONG:
        case MYSQL_TYPE_LONGLONG:
        case MYSQL_TYPE_YEAR:
        case MYSQL_TYPE_FLOAT:
        case MYSQL_TYPE_DOUBLE:
        case MYSQL_TYPE_DECIMAL:
        case MYSQL_TYPE_NEWDECIMAL:
            return true;
        default:
            return false;
    }
}

static constexpr int DECIMAL_MAX_SCALE{30};
static constexpr int DECIMAL_NOT_SPECIFIED{DECIMAL_MAX_SCALE + 1};
/** -838:59:59 */
constexpr const int MAX_TIME_WIDTH{10};
/** YYYY-MM-DD HH:MM:SS */
constexpr const int MAX_DATETIME_WIDTH{19};

/** maximum length of buffer in our big digits (uint32). */
static constexpr int DECIMAL_BUFF_LENGTH{9};
/** the number of digits that my_decimal can possibly contain */
static constexpr int DECIMAL_MAX_POSSIBLE_PRECISION{DECIMAL_BUFF_LENGTH * 9};

constexpr const int DATETIME_MAX_DECIMALS = 6;
static constexpr int DECIMAL_MAX_PRECISION{
    DECIMAL_MAX_POSSIBLE_PRECISION - 8 * 2
};

/**
  maximum guaranteed precision of number in decimal digits (number of our
  digits * number of decimal digits in one our big digit - number of decimal
  digits in one our big digit decreased by 1 (because we always put decimal
  point on the border of our big digits))
*/
/*
  Inside an in-memory data record, memory pointers to pieces of the
  record (like BLOBs) are stored in their native byte order and in
  this amount of bytes.
*/
#define portable_sizeof_char_ptr 8

#define NOT_NULL_FLAG  1    /**< Field can't be NULL */
#define BLOB_FLAG      16   /**< Field is a blob */
#define UNSIGNED_FLAG  32   /**< Field is unsigned */
#define ZEROFILL_FLAG  64   /**< Field is zerofill */
#define BINARY_FLAG    128  /**< Field is binary   */
#define ENUM_FLAG      256  /**< field is an enum */
#define TIMESTAMP_FLAG 1024 /**< Field is a timestamp */
#define SET_FLAG       2048 /**< field is a set */

static constexpr uint32_t MY_CS_BINSORT = 1 << 4; // if binary sort order

/* See strings/CHARSET_INFO.txt about information on this structure  */
struct CHARSET_INFO {
    uint number;
    uint primary_number;
    uint binary_number;
    uint state;
    const char *csname;
    const char *m_coll_name;
    const char *tailoring;
    struct Coll_param *coll_param;
};

struct TYPELIB {               /* Different types saved here */
    size_t count{0};           /* How many types */
//    const char *name{nullptr}; /* Name of typelib */
//    const char **type_names{nullptr};   没使用直接注释掉，否则还要考虑 delete 内存
//    unsigned int *type_lengths{nullptr};

};
