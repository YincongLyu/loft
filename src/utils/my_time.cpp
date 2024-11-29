//
// Created by Takenzz on 2024/11/26.
//

#include "utils/my_time.h"
#include <sys/time.h>
#include <cctype>
#include <time.h>
#include <climits>
#include <cmath>
#include "common/logging.h"

#define TIMEF_OFS 0x800000000000LL
#define TIMEF_INT_OFS 0x800000LL
#define DATETIMEF_INT_OFS 0x8000000000LL
#define EPOCH_YEAR 1970
#define LEAPS_THRU_END_OF(y) ((y) / 4 - (y) / 100 + (y) / 400)
#define isleap(y) (((y) % 4) == 0 && (((y) % 100) != 0 || ((y) % 400) == 0))

constexpr const int SECS_PER_MIN        = 60;
constexpr const int HOURS_PER_DAY       = 24;
constexpr const int DAYS_PER_WEEK       = 7;
constexpr const int DAYS_PER_NYEAR      = 365;
constexpr const int DAYS_PER_LYEAR      = 366;
constexpr const int SECS_PER_HOUR       = (SECS_PER_MIN * MINS_PER_HOUR);
constexpr const int SECS_PER_DAY        = (SECS_PER_HOUR * HOURS_PER_DAY);
constexpr const int MONS_PER_YEAR       = 12;
constexpr const int MAX_TIME_ZONE_HOURS = 14;
#define MAX_DATE_PARTS 8

const ulonglong log_10_int[20] = {1,
    10,
    100,
    1000,
    10000UL,
    100000UL,
    1000000UL,
    10000000UL,
    100000000ULL,
    1000000000ULL,
    10000000000ULL,
    100000000000ULL,
    1000000000000ULL,
    10000000000000ULL,
    100000000000000ULL,
    1000000000000000ULL,
    10000000000000000ULL,
    100000000000000000ULL,
    1000000000000000000ULL,
    10000000000000000000ULL};

static constexpr const char  time_separator    = ':';
static constexpr ulong const days_at_timestart = 719528;

static uint64_t my_time_zone = 0;

static const uint mon_starts[2][MONS_PER_YEAR] = {
    {0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334}, {0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335}};

static longlong my_packed_time_make(longlong i, longlong f)
{
  assert(std::abs(f) <= 0xffffffLL);
  return (static_cast<ulonglong>(i) << 24) + f;
}

inline bool is_time_t_valid_for_timestamp(time_t x)
{
  return (static_cast<int64_t>(x) <= static_cast<int64_t>(MYTIME_MAX_VALUE) && x >= MYTIME_MIN_VALUE);
}

static longlong my_packed_time_get_int_part(longlong i) { return (i >> 24); }

static inline int isspace_char(char ch) { return std::isspace(static_cast<unsigned char>(ch)); }

static inline int isdigit_char(char ch) { return std::isdigit(static_cast<unsigned char>(ch)); }

static inline int ispunct_char(char ch) { return std::ispunct(static_cast<unsigned char>(ch)); }

long calc_daynr(uint year, uint month, uint day)
{
  long delsum;
  int  temp;
  int  y = year; /* may be < 0 temporarily */

  if (y == 0 && month == 0)
    return 0; /* Skip errors */
  /* Cast to int to be able to handle month == 0 */
  delsum = static_cast<long>(365 * y + 31 * (static_cast<int>(month) - 1) + static_cast<int>(day));
  if (month <= 2)
    y--;
  else
    delsum -= static_cast<long>(static_cast<int>(month) * 4 + 23) / 10;
  temp = ((y / 100 + 1) * 3) / 4;
  assert(delsum + static_cast<int>(y) / 4 - temp >= 0);
  return (delsum + static_cast<int>(y) / 4 - temp);
} /* calc_daynr */

int64_t my_system_gmt_sec(const MYSQL_TIME &my_time, int64_t *my_timezone)
{
  uint        loop;
  time_t      tmp   = 0;
  int         shift = 0;
  MYSQL_TIME  tmp_time;
  MYSQL_TIME *t = &tmp_time;
  struct tm  *l_time;
  struct tm   tm_tmp;
  uint64_t    diff, current_timezone;

  tmp_time = my_time;

  if ((t->year == 9999) && (t->month == 1) && (t->day > 4)) {
    t->day -= 2;
    shift = 2;
  }

  int64_t tmp_days    = calc_daynr(static_cast<uint>(t->year), static_cast<uint>(t->month), static_cast<uint>(t->day));
  tmp_days            = tmp_days - static_cast<int64_t>(days_at_timestart);
  int64_t tmp_seconds = tmp_days * SECONDS_IN_24H +
                        (static_cast<int64_t>(t->hour) * 3600 + static_cast<int64_t>(t->minute * 60 + t->second));
  // This will be a narrowing on 32 bit time platforms, but checked range above
  tmp = static_cast<time_t>(tmp_seconds + my_time_zone - 3600);

  current_timezone = my_time_zone;
  localtime_r(&tmp, &tm_tmp);
  l_time = &tm_tmp;
  for (loop = 0;
      loop < 2 && (t->hour != static_cast<uint>(l_time->tm_hour) || t->minute != static_cast<uint>(l_time->tm_min) ||
                      t->second != static_cast<uint>(l_time->tm_sec));
      loop++) { /* One check should be enough ? */
    /* Get difference in days */
    int days = t->day - l_time->tm_mday;
    if (days < -1)
      days = 1; /* Month has wrapped */
    else if (days > 1)
      days = -1;
    diff = (3600L * static_cast<long>(days * 24 + (static_cast<int>(t->hour) - l_time->tm_hour)) +
            static_cast<long>(60 * (static_cast<int>(t->minute) - l_time->tm_min)) +
            static_cast<long>(static_cast<int>(t->second) - l_time->tm_sec));
    current_timezone += diff + 3600; /* Compensate for -3600 above */
    tmp += static_cast<time_t>(diff);
    localtime_r(&tmp, &tm_tmp);
    l_time = &tm_tmp;
  }

  if (loop == 2 && t->hour != static_cast<uint>(l_time->tm_hour)) {
    int days = t->day - l_time->tm_mday;
    if (days < -1)
      days = 1; /* Month has wrapped */
    else if (days > 1)
      days = -1;
    diff = (3600L * static_cast<long>(days * 24 + (static_cast<int>(t->hour) - l_time->tm_hour)) +
            static_cast<long>(60 * (static_cast<int>(t->minute) - l_time->tm_min)) +
            static_cast<long>(static_cast<int>(t->second) - l_time->tm_sec));
    if (diff == 3600)
      tmp += 3600 - t->minute * 60 - t->second; /* Move to next hour */
    else if (diff == -3600)
      tmp -= t->minute * 60 + t->second; /* Move to previous hour */
  }
  *my_timezone = current_timezone;

  /* shift back, if we were dealing with boundary dates */
  tmp += shift * SECONDS_IN_24H;

  if (!is_time_t_valid_for_timestamp(tmp))
    tmp = 0;

  return static_cast<int64_t>(tmp);
}

bool check_datetime_range(const MYSQL_TIME &my_time)
{
  /*
    In case of MYSQL_TIMESTAMP_TIME hour value can be up to TIME_MAX_HOUR.
    In case of MYSQL_TIMESTAMP_DATETIME it cannot be bigger than 23.
  */
  return my_time.year > 9999U || my_time.month > 12U || my_time.day > 31U || my_time.minute > 59U ||
         my_time.second > 59U || my_time.second_part > 999999U ||
         (my_time.hour > (my_time.time_type == MYSQL_TIMESTAMP_TIME ? TIME_MAX_HOUR : 23U));
}

bool time_zone_displacement_to_seconds(const char *str, size_t length, int *result)
{
  if (length < 6)
    return true;

  int sign = str[0] == '+' ? 1 : (str[0] == '-' ? -1 : 0);
  if (sign == 0)
    return true;

  if (!(std::isdigit(str[1]) && std::isdigit(str[2])))
    return true;
  int hours = (str[1] - '0') * 10 + str[2] - '0';

  if (str[3] != ':')
    return true;

  if (!(std::isdigit(str[4]) && std::isdigit(str[5])))
    return true;
  int minutes = (str[4] - '0') * 10 + str[5] - '0';
  if (minutes >= MINS_PER_HOUR)
    return true;
  int seconds = hours * SECS_PER_HOUR + minutes * SECS_PER_MIN;

  if (seconds > MAX_TIME_ZONE_HOURS * SECS_PER_HOUR)
    return true;

  // The SQL standard forbids -00:00.
  if (sign == -1 && hours == 0 && minutes == 0)
    return true;

  for (size_t i = 6; i < length; ++i)
    if (!std::isspace(str[i]))
      return true;

  *result = seconds * sign;
  return false;
}

longlong TIME_to_longlong_time_packed(const MYSQL_TIME &my_time)
{
  /* If month is 0, we mix day with hours: "1 00:10:10" -> "24:00:10" */
  long hms = (((my_time.month ? 0 : my_time.day * 24) + my_time.hour) << 12) | (my_time.minute << 6) | my_time.second;
  longlong tmp = my_packed_time_make(hms, my_time.second_part);
  return my_time.neg ? -tmp : tmp;
}

void my_time_packed_to_binary(longlong nr, uchar *ptr, uint dec)
{
  assert(dec <= DATETIME_MAX_DECIMALS);
  /* Make sure the stored value was previously properly rounded or truncated */
  assert((my_packed_time_get_frac_part(nr) % static_cast<int>(log_10_int[DATETIME_MAX_DECIMALS - dec])) == 0);

  switch (dec) {
    case 0:
    default: mi_int3store(ptr, TIMEF_INT_OFS + my_packed_time_get_int_part(nr)); break;

    case 1:
    case 2:
      mi_int3store(ptr, TIMEF_INT_OFS + my_packed_time_get_int_part(nr));
      ptr[3] = static_cast<unsigned char>(static_cast<char>(my_packed_time_get_frac_part(nr) / 10000));
      break;

    case 4:
    case 3:
      mi_int3store(ptr, TIMEF_INT_OFS + my_packed_time_get_int_part(nr));
      mi_int2store(ptr + 3, my_packed_time_get_frac_part(nr) / 100);
      break;

    case 5:
    case 6: mi_int6store(ptr, nr + TIMEF_OFS); break;
  }
}

longlong TIME_to_longlong_datetime_packed(const MYSQL_TIME &my_time)
{
  longlong ymd = ((my_time.year * 13 + my_time.month) << 5) | my_time.day;
  longlong hms = (my_time.hour << 12) | (my_time.minute << 6) | my_time.second;
  longlong tmp = my_packed_time_make(((ymd << 17) | hms), my_time.second_part);
  assert(!check_datetime_range(my_time)); /* Make sure no overflow */
  return my_time.neg ? -tmp : tmp;
}

void my_datetime_packed_to_binary(longlong nr, uchar *ptr, uint dec)
{
  assert(dec <= DATETIME_MAX_DECIMALS);
  /* The value being stored must have been properly rounded or truncated */
  assert((my_packed_time_get_frac_part(nr) % static_cast<int>(log_10_int[DATETIME_MAX_DECIMALS - dec])) == 0);

  mi_int5store(ptr, my_packed_time_get_int_part(nr) + DATETIMEF_INT_OFS);
  switch (dec) {
    case 0:
    default: break;
    case 1:
    case 2: ptr[5] = static_cast<unsigned char>(static_cast<char>(my_packed_time_get_frac_part(nr) / 10000)); break;
    case 3:
    case 4: mi_int2store(ptr + 5, my_packed_time_get_frac_part(nr) / 100); break;
    case 5:
    case 6: mi_int3store(ptr + 5, my_packed_time_get_frac_part(nr));
  }
}

void my_timestamp_to_binary(const my_timeval *tm, uchar *ptr, uint dec)
{
  assert(dec <= DATETIME_MAX_DECIMALS);
  /* Stored value must have been previously properly rounded or truncated */
  assert((tm->m_tv_usec % static_cast<int>(log_10_int[DATETIME_MAX_DECIMALS - dec])) == 0);
  mi_int4store(ptr, tm->m_tv_sec);
  switch (dec) {
    case 0:
    default: break;
    case 1:
    case 2: ptr[4] = static_cast<unsigned char>(static_cast<char>(tm->m_tv_usec / 10000)); break;
    case 3:
    case 4:
      mi_int2store(ptr + 4, tm->m_tv_usec / 100);
      break;
      /* Impossible second precision. Fall through */
    case 5:
    case 6: mi_int3store(ptr + 4, tm->m_tv_usec);
  }
}

/*
   [-] DAYS [H]H:MM:SS, [H]H:MM:SS, [H]H:MM, [H]HMMSS,[M]MSS or [S]S
*/
void str_to_time(const char *str, std::size_t length, MYSQL_TIME *l_time)
{
  ulong       date[5];
  ulonglong   value;
  uint        state;
  bool        seen_colon = false;
  const char *end        = str + length;
  const char *end_of_days;
  bool        found_days;
  bool        found_hours;
  const char *start;
  const char *str_arg = str;

  l_time->time_type = MYSQL_TIMESTAMP_NONE;
  l_time->neg       = false;

  for (; str != end && isspace_char(*str); str++) {
    length--;
  }

  if (str != end && *str == '-') {
    l_time->neg = true;
    str++;
    length--;
  }

  if (str == end)
    return;
  start = str;

  for (value = 0; str != end && isdigit_char(*str); str++)
    value = value * 10L + static_cast<long>(*str - '0');
  if (value > UINT_MAX)
    return;
  end_of_days = str;

  int spaces = 0;
  for (; str != end && isspace_char(str[0]); str++)
    spaces++;

  state      = 0;
  found_days = found_hours = false;
  if (static_cast<uint>(end - str) > 1 && str != end_of_days && isdigit_char(*str)) {
    date[0]    = static_cast<ulong>(value);
    state      = 1;
    found_days = true;
  } else if ((end - str) > 1 && *str == time_separator && isdigit_char(str[1])) {
    date[0]     = 0;
    date[1]     = static_cast<ulong>(value);
    state       = 2;
    found_hours = true;
    str++; /* skip ':' */
    seen_colon = true;
  } else {
    /* String given as one number; assume HHMMSS format */
    date[0] = 0;
    date[1] = static_cast<ulong>(value / 10000);
    date[2] = static_cast<ulong>(value / 100 % 100);
    date[3] = static_cast<ulong>(value % 100);
    state   = 4;
    goto fractional;
  }

  for (;;) {
    for (value = 0; str != end && isdigit_char(*str); str++)
      value = value * 10L + static_cast<long>(*str - '0');
    date[state++] = value;
    if (state == 4 || (end - str) < 2 || *str != time_separator || !isdigit_char(str[1]))
      break;
    str++;
    seen_colon = true;
  }

  if (state != 4) {
    memset((date + state), 0, sizeof(long) * (4 - state));
  }

fractional:
  if ((end - str) >= 2 && *str == '.' && isdigit_char(str[1])) {
    int field_length = 5;
    str++;
    value = static_cast<uint>(static_cast<uchar>(*str - '0'));
    while (++str != end && isdigit_char(*str)) {
      if (field_length-- > 0)
        value = value * 10 + static_cast<uint>(static_cast<uchar>(*str - '0'));
    }
    if (field_length >= 0) {
      if (field_length > 0)
        value *= static_cast<long>(log_10_int[field_length]);
    } else {
      for (; str != end && isdigit_char(*str); str++) {}
      date[4] = static_cast<ulong>(value);
    }
  } else if ((end - str) == 1 && *str == '.') {
    str++;
    date[4] = 0;
  } else
    date[4] = 0;

  l_time->year        = 0;
  l_time->month       = 0;
  l_time->day         = 0;
  l_time->hour        = date[1] + date[0] * 24;
  l_time->minute      = date[2];
  l_time->second      = date[3];
  l_time->second_part = date[4];

  l_time->time_type              = MYSQL_TIMESTAMP_TIME;
  l_time->time_zone_displacement = 0;
  return;
}

/*
      YYMMDD, YYYYMMDD, YYMMDDHHMMSS, YYYYMMDDHHMMSS
      YY-MM-DD, YYYY-MM-DD, YY-MM-DD HH.MM.SS
      YYYYMMDDTHHMMSS
*/
void str_to_datetime(const char *const str_arg, std::size_t length, MYSQL_TIME *l_time)
{
  uint        field_length = 0;
  uint        year_length  = 0;
  uint        digits;
  uint        number_of_fields;
  uint        date[MAX_DATE_PARTS];
  uint        date_len[MAX_DATE_PARTS];
  uint        start_loop;
  ulong       not_zero_date;
  bool        is_internal_format = false;
  const char *pos;
  const char *last_field_pos     = nullptr;
  const char *end                = str_arg + length;
  bool        found_delimiter    = false;
  bool        found_space        = false;
  bool        found_displacement = false;
  uint        frac_pos;
  uint        frac_len;
  int         displacement = 0;
  const char *str          = str_arg;

  for (; str != end && isspace_char(*str); str++)
    ;  // 跳过空格

  if (str == end || !isdigit_char(*str))
    return;

  is_internal_format = false;  // internal format表示只有数字没有分隔符

  for (pos = str; pos != end && (isdigit_char(*pos) || *pos == 'T'); pos++)
    ;

  digits      = static_cast<uint>(pos - str);  // 第一个part的数字有多少位
  start_loop  = 0;                             /* Start of scan loop */
  date_len[0] = 0;                             /* Length of year field */

  if (pos == end || *pos == '.') {
    /* Found date in internal format (only numbers like YYYYMMDD) */
    year_length        = (digits == 4 || digits == 8 || digits >= 14) ? 4 : 2;
    field_length       = year_length;
    is_internal_format = true;
  } else {
    field_length = 4;
  }

  not_zero_date = 0;
  uint i;
  /*
  一个循环代表一个part
  */
  for (i = start_loop; i < MAX_DATE_PARTS - 1 && str != end && isdigit_char(*str); i++) {
    const char *start            = str;
    ulong       tmp_value        = static_cast<uchar>(*str++ - '0');
    bool        scan_until_delim = !is_internal_format && (i != 6);

    while (str != end && isdigit_char(str[0]) && (scan_until_delim || --field_length)) {
      tmp_value = tmp_value * 10 + static_cast<ulong>(static_cast<uchar>(*str - '0'));
      str++;
    }
    date_len[i] = static_cast<uint>(str - start);
    date[i]     = tmp_value;
    not_zero_date |= tmp_value;

    field_length = 2;  // 年份之后每个field的长度都为2
    if ((last_field_pos = str) == end) {
      i++;
      break;
    }
    if (i == 2 && *str == 'T') {
      str++;
      continue;
    }
    if (i == 5) {
      if (*str == '.') {
        str++;
        last_field_pos = str;
        field_length   = 6; /* 6 digits */
      } else if (isdigit_char(str[0])) {
        i++;
        break;
      } else if (str[0] == '+' || str[0] == '-') {
        if (!time_zone_displacement_to_seconds(str, end - str, &displacement)) {
          found_displacement = true;
          str += end - str;
          last_field_pos = str;
        } else {
          l_time->time_type = MYSQL_TIMESTAMP_NONE;
          return;
        }
      }
      continue;
    }
    if (i == 6 && (str[0] == '+' || str[0] == '-')) {
      if (!time_zone_displacement_to_seconds(str, end - str, &displacement)) {
        found_displacement = true;
        str += end - str;
        last_field_pos = str;
      } else {
        return;
      }
    }

    bool one_delim_seen = false;
    while (str != end && (ispunct_char(*str) || isspace_char(*str))) {
      if (isspace_char(*str)) {
        found_space = true;
      }
      str++;
      one_delim_seen  = true;
      found_delimiter = true;
    }
    if (i == 6) {
      i++;
    }
    last_field_pos = str;
  }

  str              = last_field_pos;
  number_of_fields = i;

  while (i < MAX_DATE_PARTS) {
    date_len[i] = 0;
    date[i++]   = 0;
  }

  if (!is_internal_format) {
    year_length = date_len[0];

    l_time->year                   = date[static_cast<uint>(0)];
    l_time->month                  = date[static_cast<uint>(1)];
    l_time->day                    = date[static_cast<uint>(2)];
    l_time->hour                   = date[static_cast<uint>(3)];
    l_time->minute                 = date[static_cast<uint>(4)];
    l_time->second                 = date[static_cast<uint>(5)];
    l_time->time_zone_displacement = displacement;

    frac_pos = static_cast<uint>(6);
    frac_len = date_len[frac_pos];
    if (frac_len < 6)
      date[frac_pos] *= static_cast<uint>(log_10_int[DATETIME_MAX_DECIMALS - frac_len]);
    l_time->second_part = date[frac_pos];
  } else {
    l_time->year   = date[0];
    l_time->month  = date[1];
    l_time->day    = date[2];
    l_time->hour   = date[3];
    l_time->minute = date[4];
    l_time->second = date[5];
    if (date_len[6] < 6)
      date[6] *= static_cast<uint>(log_10_int[DATETIME_MAX_DECIMALS - date_len[6]]);
    l_time->second_part            = date[6];
    l_time->time_zone_displacement = displacement;
  }
  l_time->neg = false;

  if (year_length == 2 && not_zero_date)
    l_time->year += (l_time->year < 70 ? 2000 : 1900);

  l_time->time_type =
      (number_of_fields <= 3 ? MYSQL_TIMESTAMP_DATE
                             : (found_displacement ? MYSQL_TIMESTAMP_DATETIME_TZ : MYSQL_TIMESTAMP_DATETIME));

  if (str != end && (str[0] == '+' || str[0] == '-')) {
    l_time->time_type              = MYSQL_TIMESTAMP_DATETIME_TZ;
    l_time->time_zone_displacement = displacement;
    return;
  }
  return;
}

void datetime_to_timeval(const MYSQL_TIME *ltime, my_timeval *tm)
{
  int64_t not_used;
  tm->m_tv_sec  = my_system_gmt_sec(*ltime, &not_used);
  tm->m_tv_usec = ltime->second_part;
}

longlong TIME_to_longlong_packed(const MYSQL_TIME &my_time)
{
  switch (my_time.time_type) {
    case MYSQL_TIMESTAMP_DATETIME_TZ: assert(false);  // Should not be this type at this point.
    case MYSQL_TIMESTAMP_DATETIME: return TIME_to_longlong_datetime_packed(my_time);
    case MYSQL_TIMESTAMP_TIME: return TIME_to_longlong_time_packed(my_time);
    case MYSQL_TIMESTAMP_DATE: LOG_ERROR("DATE type not support");
    case MYSQL_TIMESTAMP_NONE:
    case MYSQL_TIMESTAMP_ERROR: return 0;
  }
  assert(false);
  return 0;
}