
#include "utils/decimal.h"

int decimal2bin(const decimal_t *from, uchar *to, int precision, int frac) {
    dec1 mask = from->sign ? -1 : 0, *buf1 = from->buf, *stop1;
    int error = 0, intg = precision - frac, isize1, intg1, intg1x, from_intg,
        intg0 = intg / DIG_PER_DEC1, frac0 = frac / DIG_PER_DEC1,
        intg0x = intg - intg0 * DIG_PER_DEC1,
        frac0x = frac - frac0 * DIG_PER_DEC1, frac1 = from->frac / DIG_PER_DEC1,
        frac1x = from->frac - frac1 * DIG_PER_DEC1,
        isize0 = intg0 * sizeof(dec1) + dig2bytes[intg0x],
        fsize0 = frac0 * sizeof(dec1) + dig2bytes[frac0x],
        fsize1 = frac1 * sizeof(dec1) + dig2bytes[frac1x];
    const int orig_isize0 = isize0;
    const int orig_fsize0 = fsize0;
    uchar *orig_to = to;

    buf1 = remove_leading_zeroes(from, &from_intg);

    if (from_intg + fsize1 == 0) {
        mask = 0; /* just in case */
        intg = 1;
        buf1 = &mask;
    }

    intg1 = from_intg / DIG_PER_DEC1;
    intg1x = from_intg - intg1 * DIG_PER_DEC1;
    isize1 = intg1 * sizeof(dec1) + dig2bytes[intg1x];

    if (intg < from_intg) {
        buf1 += intg1 - intg0 + (intg1x > 0) - (intg0x > 0);
        intg1 = intg0;
        intg1x = intg0x;
        error = E_DEC_OVERFLOW;
    } else if (isize0 > isize1) {
        while (isize0-- > isize1) {
            *to++ = (char)mask;
        }
    }
    if (fsize0 < fsize1) {
        frac1 = frac0;
        frac1x = frac0x;
        error = E_DEC_TRUNCATED;
    } else if (fsize0 > fsize1 && frac1x) {
        if (frac0 == frac1) {
            frac1x = frac0x;
            fsize0 = fsize1;
        } else {
            frac1++;
            frac1x = 0;
        }
    }

    /* intg1x part */
    if (intg1x) {
        int i = dig2bytes[intg1x];
        dec1 x = mod_by_pow10(*buf1++, intg1x) ^ mask;
        switch (i) {
            case 1:
                mi_int1store(to, x);
                break;
            case 2:
                mi_int2store(to, x);
                break;
            case 3:
                mi_int3store(to, x);
                break;
            case 4:
                mi_int4store(to, x);
                break;
            default:
                break;
        }
        to += i;
    }

    /* intg1+frac1 part */
    for (stop1 = buf1 + intg1 + frac1; buf1 < stop1; to += sizeof(dec1)) {
        dec1 x = *buf1++ ^ mask;
        mi_int4store(to, x);
    }

    /* frac1x part */
    if (frac1x) {
        dec1 x;
        int i = dig2bytes[frac1x],
            lim = (frac1 < frac0 ? DIG_PER_DEC1 : frac0x);
        while (frac1x < lim && dig2bytes[frac1x] == i) {
            frac1x++;
        }
        x = div_by_pow10(*buf1, DIG_PER_DEC1 - frac1x) ^ mask;
        switch (i) {
            case 1:
                mi_int1store(to, x);
                break;
            case 2:
                mi_int2store(to, x);
                break;
            case 3:
                mi_int3store(to, x);
                break;
            case 4:
                mi_int4store(to, x);
                break;
            default:
                break;
        }
        to += i;
    }
    if (fsize0 > fsize1) {
        uchar *to_end = orig_to + orig_fsize0 + orig_isize0;

        while (fsize0-- > fsize1 && to < to_end) {
            *to++ = (uchar)mask;
        }
    }
    orig_to[0] ^= 0x80;

    /* Check that we have written the whole decimal and nothing more */
    return error;
}
