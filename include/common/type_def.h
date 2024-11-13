//
// Created by Coonger on 2024/11/13.
//
#pragma once

#include <cstdint>

using uint8 = __uint8_t;
using uint16 = __uint16_t;

using int32 = signed int;
using uint32 = __uint32_t;
typedef unsigned int uint;

using int64 = signed long int;
using uint64 = __uint64_t;
typedef unsigned long ulong;

using my_off_t = unsigned long long;
using uchar = unsigned char;

// used for decimal
using dec1 = int32_t;
using udec1 = uint32_t;

struct MYSQL_LEX_CSTRING {
  const char *str;
  std::size_t length;
};
typedef struct MYSQL_LEX_CSTRING LEX_CSTRING;