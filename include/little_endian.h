//
// Created by Coonger on 2024/10/18.
//

#ifndef LOFT_LITTLE_ENDIAN_H
#define LOFT_LITTLE_ENDIAN_H
#include "constants.h"
using namespace loft;

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

#endif // LOFT_LITTLE_ENDIAN_H
