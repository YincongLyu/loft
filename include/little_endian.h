//
// Created by Coonger on 2024/10/18.
//

#ifndef LOFT_LITTLE_ENDIAN_H
#define LOFT_LITTLE_ENDIAN_H
#include "constants.h"
#include <type_traits>
using namespace loft;

typedef unsigned char uchar;
typedef unsigned long long int ulonglong;
typedef unsigned int uint;
typedef int8_t int8;
typedef uint8_t uint8;
typedef int16_t int16;
typedef uint16_t uint16;
typedef int32_t int32;
typedef uint32_t uint32;
typedef int64_t int64;
typedef uint64_t uint64;
typedef intptr_t intptr;
typedef long long int longlong;
typedef unsigned long int ulong;
typedef uint32 my_bitmap_map;

#define EXTRA_ROW_INFO_LEN_OFFSET 0
#define EXTRA_ROW_INFO_FORMAT_OFFSET 1
#define EXTRA_ROW_INFO_HEADER_LENGTH 2
#define EXTRA_ROW_INFO_MAX_PAYLOAD (255 - EXTRA_ROW_INFO_HEADER_LENGTH)
#define ROWS_MAPID_OFFSET 0
#define ROWS_FLAGS_OFFSET 6
#define ROWS_VHLEN_OFFSET 8
#define EXTRA_ROW_INFO_TYPECODE_LENGTH 1
#define EXTRA_ROW_PART_INFO_VALUE_LENGTH 2


using dec1 = int32;
#define DIG_PER_DEC1 9
#define E_DEC_OK 0
#define E_DEC_TRUNCATED 1
#define E_DEC_OVERFLOW 2
#define E_DEC_DIV_ZERO 4
#define E_DEC_BAD_NUM 8
#define E_DEC_OOM 16
#define ROUND_UP(X) (((X) + DIG_PER_DEC1 - 1) / DIG_PER_DEC1)
using udec1 = std::make_unsigned<dec1>::type;


static inline void int3store(uchar *T, uint A) {
    *(T) = (uchar)(A);
    *(T + 1) = (uchar)(A >> 8);
    *(T + 2) = (uchar)(A >> 16);
}

static inline void int5store(uchar *T, uint64_t A) {
    *(T) = (uchar)(A);
    *(T + 1) = (uchar)(A >> 8);
    *(T + 2) = (uchar)(A >> 16);
    *(T + 3) = (uchar)(A >> 24);
    *(T + 4) = (uchar)(A >> 32);
}

static inline void int6store(uchar *T, uint64_t A) {
    *(T) = (uchar)(A);
    *(T + 1) = (uchar)(A >> 8);



    *(T + 2) = (uchar)(A >> 16);
    *(T + 3) = (uchar)(A >> 24);
    *(T + 4) = (uchar)(A >> 32);
    *(T + 5) = (uchar)(A >> 40);
}

static inline void int2store(uchar *T, uint16_t A) {
    memcpy(T, &A, sizeof(A));
}

static inline void int4store(uchar *T, uint32_t A) {
    memcpy(T, &A, sizeof(A));
}

static inline void int7store(uchar *T, uint64_t A) {
    memcpy(T, &A, 7);
}

static inline void int8store(uchar *T, uint64_t A) {
    memcpy(T, &A, sizeof(A));
}

static uchar *net_store_length(uchar *packet, uint64_t length) {
    if (length < (uint64_t)251LL) {
        *packet = (uchar)length;
        return packet + 1;
    }
    /* 251 is reserved for NULL */
    if (length < (uint64_t)65536LL) {
        *packet++ = 252;
        int2store(packet, (uint)length);
        return packet + 2;
    }
    if (length < (uint64_t)16777216LL) {
        *packet++ = 253;
        int3store(packet, (ulong)length);
        return packet + 3;
    }
    *packet++ = 254;
    int8store(packet, length);
    return packet + 8;
}

static void clear_N_bit(uchar &f, int N) {
        f &= ~(1 << (N - 1));
}

static void set_N_bit(uchar &f, int N) {
        f |= (1 << (N - 1));
}


#define mi_int1store(T, A) *((uchar *)(T)) = (uchar)(A)

#define mi_int2store(T, A)                      \
  {                                             \
    uint def_temp = (uint)(A);                  \
    ((uchar *)(T))[1] = (uchar)(def_temp);      \
    ((uchar *)(T))[0] = (uchar)(def_temp >> 8); \
  }
#define mi_int3store(T, A)                       \
  { /*lint -save -e734 */                        \
    ulong def_temp = (ulong)(A);                 \
    ((uchar *)(T))[2] = (uchar)(def_temp);       \
    ((uchar *)(T))[1] = (uchar)(def_temp >> 8);  \
    ((uchar *)(T))[0] = (uchar)(def_temp >> 16); \
                              /*lint -restore */}
#define mi_int4store(T, A)                       \
  {                                              \
    ulong def_temp = (ulong)(A);                 \
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





#endif // LOFT_LITTLE_ENDIAN_H
