#ifndef LOFT_DEMICAL_H
#define LOFT_DEMICAL_H

#include "little_endian.h"
#include <limits>
#include <type_traits>

#define DECIMAL_MAX_FIELD_SIZE 65


static const int dig2bytes[DIG_PER_DEC1 + 1] = {0, 1, 1, 2, 2, 3, 3, 4, 4, 4};
static const dec1 powers10[DIG_PER_DEC1 + 1] = { 1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000};

struct decimal_t {
  int intg, frac, len;
  bool sign;
  int32 *buf;
};

template <typename T, int MinDigits, int MaxDigits, typename = void>
struct DigitCounter {
  constexpr int operator()(T x) const {
    constexpr int mid = (MinDigits + MaxDigits) / 2;
    constexpr T pivot = pow10(mid);
    if (x < pivot)
      return DigitCounter<T, MinDigits, mid>()(x);
    else
      return DigitCounter<T, mid + 1, MaxDigits>()(x);
  }

 private:
  static constexpr T pow10(int n) {
    T x = 1;
    for (int i = 0; i < n; ++i) x *= 10;
    return x;
  }
};

template <typename T, int MinDigits, int MaxDigits>
struct DigitCounter<T, MinDigits, MaxDigits,
                    typename std::enable_if<MinDigits == MaxDigits>::type> {
  constexpr int operator()(T) const { return MinDigits; }
};


template <typename T>
constexpr int count_digits(T x) {
  return DigitCounter<T, 1, std::numeric_limits<T>::digits10 + 1>()(x);
}

static inline dec1 mod_by_pow10(dec1 x, int p) {
  // See div_by_pow10 for rationale.
  switch (p) {
    case 1:
      return static_cast<uint32_t>(x) % 10;
    case 2:
      return static_cast<uint32_t>(x) % 100;
    case 3:
      return static_cast<uint32_t>(x) % 1000;
    case 4:
      return static_cast<uint32_t>(x) % 10000;
    case 5:
      return static_cast<uint32_t>(x) % 100000;
    case 6:
      return static_cast<uint32_t>(x) % 1000000;
    case 7:
      return static_cast<uint32_t>(x) % 10000000;
    case 8:
      return static_cast<uint32_t>(x) % 100000000;
    default:
      return x % powers10[p];
  }
}

static inline dec1 div_by_pow10(dec1 x, int p) {
  switch (p) {
    case 0:
      return static_cast<uint32_t>(x) / 1;
    case 1:
      return static_cast<uint32_t>(x) / 10;
    case 2:
      return static_cast<uint32_t>(x) / 100;
    case 3:
      return static_cast<uint32_t>(x) / 1000;
    case 4:
      return static_cast<uint32_t>(x) / 10000;
    case 5:
      return static_cast<uint32_t>(x) / 100000;
    case 6:
      return static_cast<uint32_t>(x) / 1000000;
    case 7:
      return static_cast<uint32_t>(x) / 10000000;
    case 8:
      return static_cast<uint32_t>(x) / 100000000;
    default:
      return x / powers10[p];
  }
}


static inline dec1 *remove_leading_zeroes(const decimal_t *from,
                                          int *intg_result) {
  // Round up intg so that we don't need special handling of the first word.
  int intg = ROUND_UP(from->intg) * DIG_PER_DEC1;

  // Remove all the leading words that contain only zeros.
  dec1 *buf0 = from->buf;
  while (intg > 0 && *buf0 == 0) {
    ++buf0;
    intg -= DIG_PER_DEC1;
  }

  // Now remove all the leading zeros in the first non-zero word, if there is a
  // non-zero word.
  if (intg > 0) {
    const int digits = count_digits<udec1>(*buf0);
    intg -= DIG_PER_DEC1 - digits;
  }

  *intg_result = intg;
  return buf0;
}

int decimal2bin(const decimal_t *from, uchar *to, int precision, int frac) ;


inline static int decimal_bin_size_inline(int precision, int scale) {
  int intg = precision - scale, intg0 = intg / DIG_PER_DEC1,
      frac0 = scale / DIG_PER_DEC1, intg0x = intg - intg0 * DIG_PER_DEC1,
      frac0x = scale - frac0 * DIG_PER_DEC1;

  assert(scale >= 0 && precision > 0 && scale <= precision);
  assert(intg0x >= 0);
  assert(intg0x <= DIG_PER_DEC1);
  assert(frac0x >= 0);
  assert(frac0x <= DIG_PER_DEC1);
  return intg0 * sizeof(dec1) + dig2bytes[intg0x] + frac0 * sizeof(dec1) +
         dig2bytes[frac0x];
}

int decimal_bin_size(int precision, int scale);

#endif

