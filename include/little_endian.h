//
// Created by Coonger on 2024/10/18.
//

#ifndef LOFT_LITTLE_ENDIAN_H
#define LOFT_LITTLE_ENDIAN_H
#include "constants.h"
using namespace loft;

static inline void int2store(uchar *T, uint16_t A) { memcpy(T, &A, sizeof(A)); }

static inline void int4store(uchar *T, uint32_t A) { memcpy(T, &A, sizeof(A)); }

static inline void int7store(uchar *T, uint64_t A) { memcpy(T, &A, 7); }

static inline void int8store(uchar *T, uint64_t A) {
    memcpy(T, &A, sizeof(A));
}

#endif // LOFT_LITTLE_ENDIAN_H
