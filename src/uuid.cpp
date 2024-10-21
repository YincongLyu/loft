//
// Created by Coonger on 2024/10/19.
// copy from: mysql libbinlogevents/src/uuid.cpp

#include "uuid.h"

/*
const size_t Uuid::TEXT_LENGTH;
const size_t Uuid::BYTE_LENGTH;
const size_t Uuid::BIT_LENGTH;
*/
namespace binary_log {

const int Uuid::bytes_per_section[NUMBER_OF_SECTIONS] = {4, 2, 2, 2, 6};
const int Uuid::hex_to_byte[] = {
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 0,  1,  2,  3,  4,  5,  6,  7,  8,
    9,  -1, -1, -1, -1, -1, -1, -1, 10, 11, 12, 13, 14, 15, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, 10, 11, 12, 13, 14, 15, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1,
};

int Uuid::parse(const char *string, size_t len) {
    return parse(string, len, bytes);
}

int Uuid::parse(
    const char *in_string, size_t len, const unsigned char *out_str
) {
    const unsigned char **p_out_str = out_str ? &out_str : nullptr;

    switch (len) {
        // UUID without dashes. ex 12345678123456781234567812345678
        case TEXT_LENGTH - 4:
            if (read_section((TEXT_LENGTH - 4) / 2, &in_string, p_out_str)) {
                return 1;
            }
            break;
        // UUID with braces ex {12345678-1234-5678-1234-567812345678}
        case TEXT_LENGTH + 2:
            if (*in_string != '{' || in_string[TEXT_LENGTH + 1] != '}') {
                return 1;
            }
            in_string++;
            [[fallthrough]];
        // standard UUID ex 12345678-1234-5678-1234-567812345678
        case TEXT_LENGTH:
            for (int i = 0; i < NUMBER_OF_SECTIONS - 1; i++) {
                if (read_section(bytes_per_section[i], &in_string, p_out_str)) {
                    return 1;
                }
                if (*in_string == '-') {
                    in_string++;
                } else {
                    return 1;
                }
            }
            if (read_section(
                    bytes_per_section[NUMBER_OF_SECTIONS - 1], &in_string,
                    p_out_str
                )) {
                return 1;
            }
            break;
        default:
            return 1;
    }
    return 0;
}

bool Uuid::read_section(
    int section_len,
    const char **section_str,
    const unsigned char **out_binary_str
) {
    const unsigned char **section_string =
        reinterpret_cast<const unsigned char **>(section_str);
    for (int j = 0; j < section_len; j++) {
        int hi = hex_to_byte[**section_string];
        if (hi == -1) {
            return true;
        }
        (*section_string)++;
        int lo = hex_to_byte[**section_string];
        if (lo == -1) {
            return true;
        }
        (*section_string)++;
        if (out_binary_str) {
            unsigned char *u = const_cast<unsigned char *>(*out_binary_str);
            *u = ((hi << 4) + lo);
            (*out_binary_str)++;
        }
    }
    return false;
}

bool Uuid::is_valid(const char *s, size_t len) {
    return parse(s, len, nullptr) == 0;
}

size_t Uuid::to_string(const unsigned char *bytes_arg, char *buf) {
    static const char byte_to_hex[] = "0123456789abcdef";
    const unsigned char *u = bytes_arg;
    for (int i = 0; i < NUMBER_OF_SECTIONS; i++) {
        if (i > 0) {
            *buf = '-';
            buf++;
        }
        for (int j = 0; j < bytes_per_section[i]; j++) {
            int byte = *u;
            *buf = byte_to_hex[byte >> 4];
            buf++;
            *buf = byte_to_hex[byte & 0xf];
            buf++;
            u++;
        }
    }
    *buf = '\0';
    return TEXT_LENGTH;
}

size_t Uuid::to_string(char *buf) const {
    return to_string(bytes, buf);
}

} // namespace binary_log