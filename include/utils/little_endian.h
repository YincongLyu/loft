//
// Created by Coonger on 2024/10/18.
//

#pragma once

#include "common/type_def.h"
#include <cstring>  // memcpy

static inline void int3store(uchar *T, uint A)
{
  *(T)     = (uchar)(A);
  *(T + 1) = (uchar)(A >> 8);
  *(T + 2) = (uchar)(A >> 16);
}

static inline void int5store(uchar *T, uint64 A)
{
  *(T)     = (uchar)(A);
  *(T + 1) = (uchar)(A >> 8);
  *(T + 2) = (uchar)(A >> 16);
  *(T + 3) = (uchar)(A >> 24);
  *(T + 4) = (uchar)(A >> 32);
}

static inline void int6store(uchar *T, uint64 A)
{
  *(T)     = (uchar)(A);
  *(T + 1) = (uchar)(A >> 8);
  *(T + 2) = (uchar)(A >> 16);
  *(T + 3) = (uchar)(A >> 24);
  *(T + 4) = (uchar)(A >> 32);
  *(T + 5) = (uchar)(A >> 40);
}

static inline void int2store(uchar *T, uint16 A) { memcpy(T, &A, sizeof(A)); }

static inline void int4store(uchar *T, uint32 A) { memcpy(T, &A, sizeof(A)); }

static inline void int7store(uchar *T, uint64 A) { memcpy(T, &A, 7); }

static inline void int8store(uchar *T, uint64 A) { memcpy(T, &A, sizeof(A)); }

static uchar *net_store_length(uchar *packet, uint64 length)
{
  if (length < (uint64)251LL) {
    *packet = (uchar)length;
    return packet + 1;
  }
  /* 251 is reserved for NULL */
  if (length < (uint64)65536LL) {
    *packet++ = 252;
    int2store(packet, (uint)length);
    return packet + 2;
  }
  if (length < (uint64)16777216LL) {
    *packet++ = 253;
    int3store(packet, (ulong)length);
    return packet + 3;
  }
  *packet++ = 254;
  int8store(packet, length);
  return packet + 8;
}

// used in write_event.cpp
static void set_N_bit(uchar &f, int N) { f |= (1 << (N - 1)); }

static void clear_N_bit(uchar &f, int N) { f &= ~(1 << (N - 1)); }

// used in decimal.cpp
#define mi_int1store(T, A) *((uchar *)(T)) = (uchar)(A)

#define mi_int2store(T, A)                      \
  {                                             \
    uint def_temp     = (uint)(A);              \
    ((uchar *)(T))[1] = (uchar)(def_temp);      \
    ((uchar *)(T))[0] = (uchar)(def_temp >> 8); \
  }
#define mi_int3store(T, A)                       \
  { /*lint -save -e734 */                        \
    ulong def_temp    = (ulong)(A);              \
    ((uchar *)(T))[2] = (uchar)(def_temp);       \
    ((uchar *)(T))[1] = (uchar)(def_temp >> 8);  \
    ((uchar *)(T))[0] = (uchar)(def_temp >> 16); \
                              /*lint -restore */}
#define mi_int4store(T, A)                       \
  {                                              \
    ulong def_temp    = (ulong)(A);              \
    ((uchar *)(T))[3] = (uchar)(def_temp);       \
    ((uchar *)(T))[2] = (uchar)(def_temp >> 8);  \
    ((uchar *)(T))[1] = (uchar)(def_temp >> 16); \
    ((uchar *)(T))[0] = (uchar)(def_temp >> 24); \
  }
#define mi_int5store(T, A)                                       \
  {                                                              \
    ulong def_temp = (ulong)(A), def_temp2 = (ulong)((A) >> 32); \
    ((uchar *)(T))[4] = (uchar)(def_temp);                       \
    ((uchar *)(T))[3] = (uchar)(def_temp >> 8);                  \
    ((uchar *)(T))[2] = (uchar)(def_temp >> 16);                 \
    ((uchar *)(T))[1] = (uchar)(def_temp >> 24);                 \
    ((uchar *)(T))[0] = (uchar)(def_temp2);                      \
  }
#define mi_int6store(T, A)                                       \
  {                                                              \
    ulong def_temp = (ulong)(A), def_temp2 = (ulong)((A) >> 32); \
    ((uchar *)(T))[5] = (uchar)(def_temp);                       \
    ((uchar *)(T))[4] = (uchar)(def_temp >> 8);                  \
    ((uchar *)(T))[3] = (uchar)(def_temp >> 16);                 \
    ((uchar *)(T))[2] = (uchar)(def_temp >> 24);                 \
    ((uchar *)(T))[1] = (uchar)(def_temp2);                      \
    ((uchar *)(T))[0] = (uchar)(def_temp2 >> 8);                 \
  }
#define mi_int7store(T, A)                                       \
  {                                                              \
    ulong def_temp = (ulong)(A), def_temp2 = (ulong)((A) >> 32); \
    ((uchar *)(T))[6] = (uchar)(def_temp);                       \
    ((uchar *)(T))[5] = (uchar)(def_temp >> 8);                  \
    ((uchar *)(T))[4] = (uchar)(def_temp >> 16);                 \
    ((uchar *)(T))[3] = (uchar)(def_temp >> 24);                 \
    ((uchar *)(T))[2] = (uchar)(def_temp2);                      \
    ((uchar *)(T))[1] = (uchar)(def_temp2 >> 8);                 \
    ((uchar *)(T))[0] = (uchar)(def_temp2 >> 16);                \
  }
#define mi_int8store(T, A)                                        \
  {                                                               \
    ulong def_temp3 = (ulong)(A), def_temp4 = (ulong)((A) >> 32); \
    mi_int4store((uchar *)(T) + 0, def_temp4);                    \
    mi_int4store((uchar *)(T) + 4, def_temp3);                    \
  }
