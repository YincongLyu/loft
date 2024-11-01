//
// Created by Coonger on 2024/10/28.
//

#ifndef LOFT_MYSQL_FIELDS_H
#define LOFT_MYSQL_FIELDS_H

#include <cassert>
#include <climits>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <sys/types.h>

#include <algorithm>
#include <optional>

//#include "../common/macros.h"
#include "constants.h"
#include "field_common_properties.h"
#include "field_types.h" // enum_field_types

namespace mysql {

class Field {
  private:
    /// 访问 ptr 先判断 m_null_ptr 的值
    // 因为没有 实际的 record，所以不用地址，只用一个 flag 就行，主要利用
    // field_is_nullable()
    //    unsigned char *m_null_ptr;

  public:
    const char *field_name;

    bool m_null = false;
    unsigned char null_bit; // Bit used to test null bit

    // Max width for a VARCHAR column, in number of bytes
    static constexpr size_t MAX_VARCHAR_WIDTH{65535};

    // Maximum sizes of the four BLOB types, in number of bytes
    static constexpr size_t MAX_TINY_BLOB_WIDTH{255};
    static constexpr size_t MAX_SHORT_BLOB_WIDTH{65535};
    static constexpr size_t MAX_MEDIUM_BLOB_WIDTH{16777215};
    static constexpr size_t MAX_LONG_BLOB_WIDTH{4294967295};

    // Length of field. Never write to this member directly; instead, use
    // set_field_length().
    uint32_t field_length;

  private:
    uint32_t flags{0};
    uint16_t m_field_index; // field number in fields array

  public:
    bool is_flag_set(unsigned flag) const { return flags & flag; }

    void set_flag(unsigned flag) { flags |= flag; }

    void clear_flag(unsigned flag) { flags &= ~flag; }

    // Avoid using this function as it makes it harder to change the internal
    // representation.
    uint32_t all_flags() const { return flags; }

  public:
//    DISALLOW_COPY(Field);
    Field(
        uint32_t length_arg,
        bool is_nullable_arg,
        unsigned char null_bit_arg,
        const char *field_name_arg
    );

    virtual ~Field() = default;

    /*
        在内存上，这个 field 在 table row 里所占用的字节数
    */
    virtual uint32_t pack_length() const { return (uint32_t)field_length; }

    /*
      在磁盘上，这个 field 在 table row 里所占用的字节数
      eg：压缩，存储引擎不同
    */
    virtual uint32_t pack_length_in_rec() const { return pack_length(); }

    virtual uint pack_length_from_metadata(uint field_metadata) const {
        return field_metadata;
    }

    virtual uint row_pack_length() const { return 0; }

    int save_field_metadata(unsigned char *first_byte) {
        return do_save_field_metadata(first_byte);
    }

    /*
      data_length() return the "real size" of the data in memory.
      Useful only for variable length datatypes where it's overloaded.
      By default assume the length is constant.
    */
    virtual uint32_t
    data_length(ptrdiff_t row_offset [[maybe_unused]] = 0) const {
        return pack_length();
    }

    virtual enum_field_types type() const = 0;

    virtual enum_field_types real_type() const { return type(); }

    virtual enum_field_types binlog_type() const { return type(); }

    bool is_nullable() const { return m_null; }

    virtual bool is_unsigned() const { return false; }

    virtual uint32_t decimals() const { return 0; }

    /// @return true if this field is NULL-able, false otherwise.

    /**
      Sets field index.

      @param[in]  field_index  Field index.
    */
    virtual void set_field_index(uint16_t field_index) {
        m_field_index = field_index;
    }

    /**
      Returns field index.

      @returns Field index.
    */
    uint16_t field_index() const { return m_field_index; }

  private:
    virtual int do_save_field_metadata(unsigned char *metadata_ptr
                                       [[maybe_unused]]) const {
        return 0;
    }
};

/******************************************************************************
                     integer type
******************************************************************************/

class Field_num : public Field {
  private:
    /**
      - true  - unsigned
      - false - signed
    */
    const bool unsigned_flag;

  public:
    const uint8_t dec;

    Field_num(
        uint32_t len_arg,
        bool is_nullable_arg,
        unsigned char null_bit_arg,
        const char *field_name_arg,
        uint8_t dec_arg,
        bool unsigned_arg
    );

    bool is_unsigned() const final { return unsigned_flag; }

    uint decimals() const final { return (uint)dec; }

    uint row_pack_length() const final { return pack_length(); }

    uint32_t pack_length_from_metadata(uint) const override {
        return pack_length();
    }
};

/* New decimal/numeric field which use fixed point arithmetic */
class Field_new_decimal : public Field_num {
  private:
    bool m_keep_precision{false};
    int do_save_field_metadata(unsigned char *first_byte) const final;

  public:
    /* The maximum number of decimal digits can be stored */
    uint precision;
    uint bin_size;
    // 构造函数
    Field_new_decimal(
        uint32_t len_arg,
        bool is_nullable_arg,
        unsigned char null_bit_arg,
        const char *field_name_arg,
        uint8_t dec_arg,
        bool unsigned_arg
    );

    // 获取类型
    enum_field_types type() const final { return MYSQL_TYPE_NEWDECIMAL; }

    uint32_t pack_length() const final { return (uint32_t)bin_size; }
};

class Field_tiny : public Field_num {
  public:
    Field_tiny(
        uint32_t len_arg,
        bool is_nullable_arg,
        unsigned char null_bit_arg,
        const char *field_name_arg,
        bool unsigned_arg
    )
        : Field_num(
              len_arg,
              is_nullable_arg,
              null_bit_arg,
              field_name_arg,
              0,
              unsigned_arg
          ) {}

    enum_field_types type() const override { return MYSQL_TYPE_TINY; }

    uint32_t pack_length() const final { return 1; }
};

class Field_short final : public Field_num {
  public:
    Field_short(
        uint32_t len_arg,
        bool is_nullable_arg,
        unsigned char null_bit_arg,
        const char *field_name_arg,
        bool unsigned_arg
    )
        : Field_num(
              len_arg,
              is_nullable_arg,
              null_bit_arg,
              field_name_arg,
              0,
              unsigned_arg
          ) {}

    enum_field_types type() const final { return MYSQL_TYPE_SHORT; }

    uint32_t pack_length() const final { return 2; }
};

class Field_medium final : public Field_num {
  public:
    Field_medium(
        uint32_t len_arg,
        bool is_nullable_arg,
        unsigned char null_bit_arg,
        const char *field_name_arg,
        bool unsigned_arg
    )
        : Field_num(
              len_arg,
              is_nullable_arg,
              null_bit_arg,
              field_name_arg,
              0,
              unsigned_arg
          ) {}

    enum_field_types type() const final { return MYSQL_TYPE_INT24; }

    uint32_t pack_length() const final { return 3; }
};

class Field_long : public Field_num {
  public:
    static const int PACK_LENGTH = 4;

    Field_long(
        uint32_t len_arg,
        bool is_nullable_arg,
        unsigned char null_bit_arg,
        const char *field_name_arg,
        bool unsigned_arg
    )
        : Field_num(
              len_arg,
              is_nullable_arg,
              null_bit_arg,
              field_name_arg,
              0,
              unsigned_arg
          ) {}

    enum_field_types type() const final { return MYSQL_TYPE_LONG; }

    uint32_t pack_length() const final { return PACK_LENGTH; }
};

class Field_longlong : public Field_num {
  public:
    static const int PACK_LENGTH = 8;

    Field_longlong(
        uint32_t len_arg,
        bool is_nullable_arg,
        unsigned char null_bit_arg,
        const char *field_name_arg,
        bool unsigned_arg
    )
        : Field_num(
              len_arg,
              is_nullable_arg,
              null_bit_arg,
              field_name_arg,
              0,
              unsigned_arg
          ) {}

    enum_field_types type() const final { return MYSQL_TYPE_LONGLONG; }

    uint32_t pack_length() const final { return PACK_LENGTH; }
};

/******************************************************************************
                     float/double/decimal type
******************************************************************************/

/* base class for float and double and decimal (old one) */
class Field_real : public Field_num {
  public:
    bool not_fixed; // 固定精度

    Field_real(
        uint32_t len_arg,
        bool is_nullable_arg,
        unsigned char null_bit_arg,
        const char *field_name_arg,
        uint8_t dec_arg,
        bool unsigned_arg
    )
        : Field_num(
              len_arg,
              is_nullable_arg,
              null_bit_arg,
              field_name_arg,
              dec_arg,
              unsigned_arg
          )
        , not_fixed(dec_arg >= DECIMAL_NOT_SPECIFIED) {}
};

class Field_decimal final : public Field_real {
  public:
    Field_decimal(
        uint32_t len_arg,
        bool is_nullable_arg,
        unsigned char null_bit_arg,
        const char *field_name_arg,
        uint8_t dec_arg,
        bool unsigned_arg
    )
        : Field_real(
              len_arg,
              is_nullable_arg,
              null_bit_arg,
              field_name_arg,
              dec_arg,
              unsigned_arg
          ) {}

    enum_field_types type() const final { return MYSQL_TYPE_DECIMAL; }
};

class Field_float final : public Field_real {
  private:
    int do_save_field_metadata(unsigned char *first_byte) const final;

  public:
    Field_float(
        uint32_t len_arg,
        bool is_nullable_arg,
        unsigned char null_bit_arg,
        const char *field_name_arg,
        uint8_t dec_arg,
        bool unsigned_arg
    )
        : Field_real(
              len_arg,
              is_nullable_arg,
              null_bit_arg,
              field_name_arg,
              dec_arg,
              unsigned_arg
          ) {}

    enum_field_types type() const final { return MYSQL_TYPE_FLOAT; }

    uint32_t pack_length() const final { return sizeof(float); }
};

class Field_double final : public Field_real {
  private:
    int do_save_field_metadata(unsigned char *first_byte) const final;

  public: // 不考虑精度
    Field_double(
        uint32_t len_arg,
        bool is_nullable_arg,
        unsigned char null_bit_arg,
        const char *field_name_arg,
        uint8_t dec_arg,
        bool unsigned_arg
    )
        : Field_real(
              len_arg,
              is_nullable_arg,
              null_bit_arg,
              field_name_arg,
              dec_arg,
              unsigned_arg
          ) {}

    enum_field_types type() const final { return MYSQL_TYPE_DOUBLE; }

    uint32_t pack_length() const final { return sizeof(double); }
};

/******************************************************************************
                     temporal type
******************************************************************************/

class Field_temporal : public Field {
  protected:
    uint8_t dec; // Number of fractional digits

    /**
    Adjust number of decimal digits from DECIMAL_NOT_SPECIFIED to
    DATETIME_MAX_DECIMALS
  */
    static uint8_t normalize_dec(uint8_t dec_arg) {
        return dec_arg == DECIMAL_NOT_SPECIFIED ? DATETIME_MAX_DECIMALS
                                                : dec_arg;
    }

  public:
    Field_temporal(
        bool is_nullable_arg,
        unsigned char null_bit_arg,
        const char *field_name_arg,
        uint32_t len_arg,
        uint8_t dec_arg
    )
        : Field(
              len_arg
                  + ((normalize_dec(dec_arg)) ? normalize_dec(dec_arg) + 1 : 0),
              is_nullable_arg,
              null_bit_arg,
              field_name_arg
          ) {
        set_flag(BINARY_FLAG);
        dec = normalize_dec(dec_arg);
    }
};

class Field_temporal_with_date : public Field_temporal {
  public:
    Field_temporal_with_date(
        bool is_nullable_arg,
        unsigned char null_bit_arg,
        const char *field_name_arg,
        uint8_t int_length_arg,
        uint8_t dec_arg
    )
        : Field_temporal(
              is_nullable_arg,
              null_bit_arg,
              field_name_arg,
              int_length_arg,
              dec_arg
          ) {}
};

class Field_temporal_with_date_and_time : public Field_temporal_with_date {
  private:
    int do_save_field_metadata(unsigned char *metadata_ptr) const override {
        if (decimals()) {
            *metadata_ptr = decimals();
            return 1;
        }
        return 0;
    }

  public:
    Field_temporal_with_date_and_time(
        bool is_nullable_arg,
        unsigned char null_bit_arg,
        const char *field_name_arg,
        uint8_t dec_arg
    )
        : Field_temporal_with_date(
              is_nullable_arg,
              null_bit_arg,
              field_name_arg,
              MAX_DATETIME_WIDTH,
              dec_arg
          ) {}
};

class Field_timestamp : public Field_temporal_with_date_and_time {
  public:
    static const int PACK_LENGTH = 4;

    Field_timestamp(
        uint32_t len_arg,
        bool is_nullable_arg,
        unsigned char null_bit_arg,
        const char *field_name_arg
    );

    enum_field_types type() const final { return MYSQL_TYPE_TIMESTAMP; }

    uint32_t pack_length() const final { return PACK_LENGTH; }
};

class Field_year final : public Field_tiny {
  public:
    Field_year(
        bool is_nullable_arg, unsigned char null_bit_arg, const char *field_name_arg
    )
        : Field_tiny(4, is_nullable_arg, null_bit_arg, field_name_arg, true) {}

    enum_field_types type() const final { return MYSQL_TYPE_YEAR; }
};

class Field_datetime : public Field_temporal_with_date_and_time {
  public:
    static const int PACK_LENGTH = 8;

    Field_datetime(
        bool is_nullable_arg,
        unsigned char null_bit_arg,
        const char *field_name_arg
    )
        : Field_temporal_with_date_and_time(
              is_nullable_arg, null_bit_arg, field_name_arg, 0
          ) {}

    Field_datetime(const char *field_name_arg)
        : Field_temporal_with_date_and_time(false, 0, field_name_arg, 0) {}

    enum_field_types type() const final { return MYSQL_TYPE_DATETIME; }

    uint32_t pack_length() const final { return PACK_LENGTH; }
};

class Field_time_common : public Field_temporal {
  public:
    Field_time_common(
        bool is_nullable_arg,
        unsigned char null_bit_arg,
        const char *field_name_arg,
        uint8_t dec_arg
    )
        : Field_temporal(
              is_nullable_arg,
              null_bit_arg,
              field_name_arg,
              MAX_TIME_WIDTH,
              dec_arg
          ) {}
};

class Field_time final : public Field_time_common {
  public:
    Field_time(
        bool is_nullable_arg,
        unsigned char null_bit_arg,
        const char *field_name_arg
    )
        : Field_time_common(is_nullable_arg, null_bit_arg, field_name_arg, 0) {}

    enum_field_types type() const final { return MYSQL_TYPE_TIME; }

    uint32_t pack_length() const final { return 3; }
};

/******************************************************************************
                     string type
******************************************************************************/

class Field_str : public Field {
  public:
    Field_str(
        uint32_t len_arg,
        bool is_nullable_arg,
        unsigned char null_bit_arg,
        const char *field_name_arg
    );

    uint32_t decimals() const override { return DECIMAL_NOT_SPECIFIED; }

    // An always-updated cache of the result of char_length(), because
    // dividing by charset()->mbmaxlen can be surprisingly costly compared
    // to the rest of e.g. make_sort_key().
    //    uint32_t char_length_cache;
};

class Field_longstr : public Field_str {
  public:
    Field_longstr(
        uint32_t len_arg,
        bool is_nullable_arg,
        unsigned char null_bit_arg,
        const char *field_name_arg
    )
        : Field_str(len_arg, is_nullable_arg, null_bit_arg, field_name_arg) {}
};

// char
class Field_string : public Field_longstr {
  private:
    int do_save_field_metadata(unsigned char *first_byte) const final;

  public:
    Field_string(
        uint32_t len_arg,
        bool is_nullable_arg,
        unsigned char null_bit_arg,
        const char *field_name_arg
    )
        : Field_longstr(
              len_arg, is_nullable_arg, null_bit_arg, field_name_arg
          ) {}


    enum_field_types type() const final { return MYSQL_TYPE_STRING; }
    enum_field_types real_type() const final { return MYSQL_TYPE_STRING; }

    uint row_pack_length() const final { return field_length; }

    uint pack_length_from_metadata(uint field_metadata) const final {
        if (field_metadata == 0) {
            return row_pack_length();
        }
        return (((field_metadata >> 4) & 0x300) ^ 0x300)
               + (field_metadata & 0x00ff);
    }
};

// varchar
class Field_varstring : public Field_longstr {
  private:
    /* Store number of bytes used to store length (1 or 2) */
    uint32_t length_bytes;

    int do_save_field_metadata(unsigned char *first_byte) const final;

  public:
    Field_varstring(
        uint32_t len_arg,
        uint length_bytes_arg,
        bool is_nullable_arg,
        unsigned char null_bit_arg,
        const char *field_name_arg
    );

    enum_field_types type() const final { return MYSQL_TYPE_VARCHAR; }
    enum_field_types real_type() const final { return MYSQL_TYPE_VARCHAR; }

    uint32_t pack_length() const final {
        return (uint32_t)field_length + length_bytes;
    }

    uint row_pack_length() const final { return field_length; }
};

class Field_blob : public Field_longstr {
  private:
    int do_save_field_metadata(unsigned char *first_byte) const override;

  protected:
    /**
      The number of bytes used to represent the length of the blob.
    */
    uint packlength;

  public:
    Field_blob(
        uint32_t len_arg,
        bool is_nullable_arg,
        unsigned char null_bit_arg,
        const char *field_name_arg,
        bool set_packlength
    )
        : Field_longstr(len_arg, is_nullable_arg, null_bit_arg, field_name_arg)
        , packlength(4) {
        set_flag(BLOB_FLAG);
        if (set_packlength) {
            packlength = len_arg <= 255        ? 1
                         : len_arg <= 65535    ? 2
                         : len_arg <= 16777215 ? 3
                                               : 4;
        }
    }

    enum_field_types type() const override { return MYSQL_TYPE_BLOB; }

    uint32_t pack_length() const final {
        return (uint32_t)(packlength + portable_sizeof_char_ptr);
    }

    /**
    Return the packed length without the pointer size added.

    This is used to determine the size of the actual data in the row
            buffer.

        @returns The length of the raw data itself without the pointer.
            */
    uint32_t pack_length_no_ptr() const { return (uint32_t)(packlength); }

    uint row_pack_length() const final { return pack_length_no_ptr(); }
};

class Field_json : public Field_blob {
  public:
    Field_json(
        uint32_t len_arg,
        bool is_nullable_arg,
        uint null_bit_arg,
        const char *field_name_arg,
        uint blob_pack_length
    )
        : Field_blob(
              len_arg,
              is_nullable_arg,
              null_bit_arg,
              field_name_arg,
              blob_pack_length
          ) {}

    enum_field_types type() const override { return MYSQL_TYPE_JSON; }

    // 无 pack_length
};

class Field_enum : public Field_str {
  private:
    int do_save_field_metadata(unsigned char *first_byte) const final;

  protected:
    uint packlength;

  public:
    TYPELIB *typelib;

    Field_enum(
        uint32_t len_arg,
        bool is_nullable_arg,
        unsigned char null_bit_arg,
        const char *field_name_arg,
        uint packlength_arg,
        TYPELIB *typelib_arg
    )
        : Field_str(len_arg, is_nullable_arg, null_bit_arg, field_name_arg)
        , packlength(packlength_arg) {
        set_flag(ENUM_FLAG);
    }

    enum_field_types type() const final { return MYSQL_TYPE_STRING; }

    uint32_t pack_length() const final { return (uint32_t)packlength; }

    enum_field_types real_type() const override { return MYSQL_TYPE_ENUM; }

    uint pack_length_from_metadata(uint field_metadata) const final {
        return (field_metadata & 0x00ff);
    }
};

class Field_set final : public Field_enum {
  public:
    Field_set(
        uint32_t len_arg,
        bool is_nullable_arg,
        unsigned char null_bit_arg,
        const char *field_name_arg,
        uint32_t packlength_arg,
        TYPELIB *typelib_arg
    )
        : Field_enum(
              len_arg,
              is_nullable_arg,
              null_bit_arg,
              field_name_arg,
              packlength_arg,
              typelib_arg
          )
        , empty_set_string("", 0) {
        clear_flag(ENUM_FLAG);
        set_flag(SET_FLAG);
    }

    enum_field_types real_type() const final { return MYSQL_TYPE_SET; }

  private:
    const loft::MYSQL_LEX_CSTRING empty_set_string;
};

class Field_bit : public Field {
  private:
    int do_save_field_metadata(unsigned char *first_byte) const final;

  public:
    //    unsigned char *bit_ptr; // position in record where 'uneven' bits
    //    store
    unsigned char bit_ofs; // offset to 'uneven' high bits
    uint bit_len;          // number of 'uneven' high bits
    uint bytes_in_rec;
    Field_bit(
        uint32_t len_arg,
        bool is_nullable_arg,
        unsigned char null_bit_arg,
        unsigned char bit_ofs_arg,
        const char *field_name_arg
    );

    enum_field_types type() const final { return MYSQL_TYPE_BIT; }

    uint32_t pack_length() const final {
        return (uint32_t)(field_length + 7) / 8;
    }
};

class Field_null final : public Field_str {
  public:
    Field_null(uint32_t len_arg, const char *field_name_arg)
        // (dummy_null_buffer & 32) is true, so is_null() always returns true.
        : Field_str(len_arg, true, 32, field_name_arg) {}

    enum_field_types type() const final { return MYSQL_TYPE_NULL; }

    uint32_t pack_length() const final { return 0; }
};

/// 构建 Field 的元数据的 除了 charset 的 5 个字段
Field *make_field(
    const char *field_name,
    size_t field_length,
    bool is_unsigned,
    bool is_nullable,
    size_t null_bit,
    enum_field_types field_type,
    TYPELIB *interval,
    uint decimals
);

enum_field_types get_blob_type_from_length(size_t length);
size_t calc_pack_length(enum_field_types type, size_t length);

unsigned int my_time_binary_length(unsigned int dec);
unsigned int my_datetime_binary_length(unsigned int dec);
unsigned int my_timestamp_binary_length(unsigned int dec);

inline uint get_enum_pack_length(int elements) {
    return elements < 256 ? 1 : 2;
}

inline uint get_set_pack_length(int elements) {
    uint len = (elements + 7) / 8;
    return len > 4 ? 8 : len;
}

inline bool is_temporal_real_type(enum_field_types type) {
    switch (type) {
        case MYSQL_TYPE_TIME2:
        case MYSQL_TYPE_TIMESTAMP2:
        case MYSQL_TYPE_DATETIME2:
            return true;
        default:
            return is_temporal_type(type);
    }
}

}
#endif // LOFT_MYSQL_FIELDS_H
