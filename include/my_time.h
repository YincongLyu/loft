
#ifndef LOFT_MY_TIME_H
#define LOFT_MY_TIME_H

#include <assert.h>
#include "little_endian.h"
#include <limits>
#include "field_common_properties.h"

constexpr const int TIME_MAX_HOUR = 838;
constexpr const int MINS_PER_HOUR = 60;
constexpr const int64_t SECONDS_IN_24H = 86400LL;
constexpr const int MYTIME_MIN_VALUE = 0;
constexpr const bool HAVE_64_BITS_TIME_T = sizeof(time_t) == sizeof(int64_t);


constexpr const int64_t MYTIME_MAX_VALUE =
    HAVE_64_BITS_TIME_T ? 32536771199
                        : std::numeric_limits<int32_t>::max();

enum enum_mysql_timestamp_type {
  MYSQL_TIMESTAMP_NONE = -2,
  MYSQL_TIMESTAMP_ERROR = -1,

  /// Stores year, month and day components.
  MYSQL_TIMESTAMP_DATE = 0,

  /**
    Stores all date and time components.
    Value is in UTC for `TIMESTAMP` type.
    Value is in local time zone for `DATETIME` type.
  */
  MYSQL_TIMESTAMP_DATETIME = 1,

  /// Stores hour, minute, second and microsecond.
  MYSQL_TIMESTAMP_TIME = 2,

  /**
    A temporary type for `DATETIME` or `TIMESTAMP` types equipped with time
    zone information. After the time zone information is reconciled, the type is
    converted to MYSQL_TIMESTAMP_DATETIME.
  */
  MYSQL_TIMESTAMP_DATETIME_TZ = 3
};


typedef struct MYSQL_TIME {
  unsigned int year, month, day, hour, minute, second;
  unsigned long second_part; /**< microseconds */
  bool neg;
  enum enum_mysql_timestamp_type time_type;
  /// The time zone displacement, specified in seconds.
  int time_zone_displacement;
} MYSQL_TIME;

struct my_timeval {
  int64_t m_tv_sec;
  int64_t m_tv_usec;
};

inline long long int my_packed_time_get_frac_part(long long int i) {
  return (i % (1LL << 24));
}

bool check_datetime_range(const MYSQL_TIME &my_time);

longlong TIME_to_longlong_time_packed(const MYSQL_TIME &my_time);

void my_time_packed_to_binary(longlong nr, uchar *ptr, uint dec);

longlong TIME_to_longlong_datetime_packed(const MYSQL_TIME &my_time);

void my_datetime_packed_to_binary(longlong nr, uchar *ptr, uint dec);

void my_timestamp_to_binary(const my_timeval *tm, uchar *ptr, uint dec);

void str_to_time(const char *str, std::size_t length, MYSQL_TIME *l_time);

void str_to_datetime(const char *const str_arg, std::size_t length, MYSQL_TIME *l_time);

void datetime_to_timeval(const MYSQL_TIME *ltime, my_timeval *tm);

longlong TIME_to_longlong_packed(const MYSQL_TIME &my_time);

#endif