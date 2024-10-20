//
// Created by Coonger on 2024/10/19.
//

#ifndef LOFT_UUID_H
#define LOFT_UUID_H

#include <stddef.h>
#include <stdio.h>
#include <string.h>

#include <string>
#include <utility>

#include "template_utils.h"

/**
   标识：在 server 上发起的 txn 编号， 是一个 hash 值
   used in Sid_map::Node, member name is rpl_sid

   只有一个成员
   unsigned char bytes[BYTE_LENGTH];

   有 3 种表示形式：
   XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX or
   XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX or
   {XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX}
*/
namespace binary_log {

struct Uuid {
    // uuid 字节长度
    static const size_t BYTE_LENGTH = 16;
    /** The data for this Uuid. */
    unsigned char bytes[BYTE_LENGTH];

    /// Set to all zeros.
    void clear() { memset(bytes, 0, BYTE_LENGTH); }

    /// Copies the given 16-byte data to this UUID.
    void copy_from(const unsigned char *data) {
        memcpy(bytes, data, BYTE_LENGTH);
    }

    /// Copies the given UUID object to this UUID.
    void copy_from(const Uuid &data) {
        copy_from(pointer_cast<const unsigned char *>(data.bytes));
    }

    /// Copies the given UUID object to this UUID.
    void copy_to(unsigned char *data) const {
        memcpy(data, bytes, BYTE_LENGTH);
    }

    /// Returns true if this UUID is equal the given UUID.
    bool equals(const Uuid &other) const {
        return memcmp(bytes, other.bytes, BYTE_LENGTH) == 0;
    }

    /// uuid 文本长度
    static const size_t TEXT_LENGTH = 36;
    /// uuid 比特长度
    static const size_t BIT_LENGTH = 128;
    // uuid 段数
    static const int NUMBER_OF_SECTIONS = 5;
    // uuid 每段的字节数
    static const int bytes_per_section[NUMBER_OF_SECTIONS];
    static const int hex_to_byte[256];
    /**
        给定的字符是否是有效的 uuid 文本，调用 parse()
    */
    static bool is_valid(const char *string, size_t len);

    /**
        将给定的字符串解析为 uuid 并存储为 UUID 对象
    */
    int parse(const char *string, size_t len);

    /**
      给定的字符串解析并存储为二进制 UUID 字符串，调用 read_section
    */
    static int parse(
        const char *in_string,
        size_t len,
        const unsigned char *out_binary_string
    );
    /**
      解析 uuid 字符串中的一个 section

    */
    static bool read_section(
        int section_len,
        const char **section_str,
        const unsigned char **out_binary_str
    );

    size_t to_string(char *buf) const;
    static size_t to_string(const unsigned char *bytes_arg, char *buf);

    std::string to_string() const {
        char buf[TEXT_LENGTH + 1];
        to_string(buf);
        return buf;
    }

    void print() const {
        char buf[TEXT_LENGTH + 1];
        to_string(buf);
        printf("%s\n", buf);
    }
};

struct Hash_Uuid {
    size_t operator()(const Uuid &uuid) const {
        return std::hash<std::string>()(std::string(
            pointer_cast<const char *>(uuid.bytes), Uuid::BYTE_LENGTH
        ));
    }
};

inline bool operator==(const Uuid &a, const Uuid &b) {
    return a.equals(b);
}

} // namespace binary_log

#endif // LOFT_UUID_H
