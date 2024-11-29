//
// Created by Coonger on 2024/10/28.
//
#include "sql/mysql_fields.h"
#include "utils/little_endian.h"
#include <memory>

#include "common/logging.h"

namespace mysql {

// 压根就没走这个函数？直接用 length 存了，反而觉得 05 08 和 05 09 是对的
inline uint my_decimal_length_to_precision(uint length, uint scale, bool unsigned_flag)
{
  /* Precision can't be negative thus ignore unsigned_flag when length is 0.
   */
  assert(length || !scale);
  uint retval = (uint)(length - (scale > 0 ? 1 : 0) - (unsigned_flag || !length ? 0 : 1));
  return retval;
}

/// This is used as a table name when the table structure is not set up
Field::Field(uint32 length_arg, bool is_nullable_arg, unsigned char null_bit_arg, const char *field_name_arg)
    : field_name(field_name_arg), m_null(is_nullable_arg), field_length(length_arg)
{
  if (!is_nullable()) {
    set_flag(NOT_NULL_FLAG);
  }
  //    m_field_index = 0;
}

/**
  Numeric fields base class constructor.
*/
Field_num::Field_num(uint32_t len_arg, bool is_nullable_arg, unsigned char null_bit_arg, const char *field_name_arg,
    uint8_t dec_arg, bool unsigned_arg)
    : Field(len_arg, is_nullable_arg, null_bit_arg, field_name_arg), unsigned_flag(unsigned_arg), dec(dec_arg)
{
  if (unsigned_flag) {
    set_flag(UNSIGNED_FLAG);
  }
}

/******************************************************************************
                     Field_new_decimal
******************************************************************************/
Field_new_decimal::Field_new_decimal(uint32_t len_arg, bool is_nullable_arg, unsigned char null_bit_arg,
    const char *name, uint8_t dec_arg, bool unsigned_arg)
    : Field_num(len_arg, is_nullable_arg, null_bit_arg, name, dec_arg, unsigned_arg)
{
  precision = std::min(len_arg,
      //        my_decimal_length_to_precision(len_arg, dec_arg,
      //        unsigned_arg),
      uint(DECIMAL_MAX_PRECISION));
  //    assert((precision <= DECIMAL_MAX_PRECISION) && (dec <=
  //    DECIMAL_MAX_SCALE)); bin_size = my_decimal_get_binary_size(precision,
  //    dec);
}

// 精度存在第一个 byte 中，小数位存在 第二个 byte 中
int Field_new_decimal::do_save_field_metadata(unsigned char *metadata_ptr) const
{
  *metadata_ptr       = precision;
  *(metadata_ptr + 1) = decimals();
  return 2;
}

/******************************************************************************
                     Field_float
******************************************************************************/

int Field_float::do_save_field_metadata(unsigned char *metadata_ptr) const
{
  *metadata_ptr = pack_length();
  return 1;
}

/******************************************************************************
                     Field_double
******************************************************************************/

int Field_double::do_save_field_metadata(unsigned char *metadata_ptr) const
{
  *metadata_ptr = pack_length();
  return 1;
}

/******************************************************************************
                     Field_string
******************************************************************************/
Field_str::Field_str(uint32_t len_arg, bool is_nullable_arg, unsigned char null_bit_arg, const char *field_name_arg)
    : Field(len_arg, is_nullable_arg, null_bit_arg, field_name_arg)
{
  // 默认的是 MY_CS_PRIMARY
  //    set_flag(BINARY_FLAG);
}

int Field_string::do_save_field_metadata(unsigned char *metadata_ptr) const
{
  assert(field_length < 1024);
  assert((real_type() & 0xF0) == 0xF0);
  LOG_INFO("field_length: %u, real_type: %u", field_length, real_type());
  *metadata_ptr       = (real_type() ^ ((field_length & 0x300) >> 4));  // fe
  *(metadata_ptr + 1) = (field_length * 4) & 0xFF;                      // 20 主动乘以 4
  return 2;
}

/******************************************************************************
                     Field_varstring
******************************************************************************/
Field_varstring::Field_varstring(
    uint32_t len_arg, uint length_bytes_arg, bool is_nullable_arg, uchar null_bit_arg, const char *field_name_arg)
    : Field_longstr(len_arg, is_nullable_arg, null_bit_arg, field_name_arg), length_bytes(len_arg < 256 ? 1 : 2)
{
  // Table_SHARE 是用来统计 表中的字段信息
}

int Field_varstring::do_save_field_metadata(unsigned char *metadata_ptr) const
{
  assert(field_length <= 65535);
  // FIXME 源码里强制转换成了 char*
  int2store(metadata_ptr, field_length);
  return 2;
}

/******************************************************************************
                     Field_blob
******************************************************************************/

int Field_blob::do_save_field_metadata(unsigned char *metadata_ptr) const
{
  *metadata_ptr = pack_length_no_ptr();
  LOG_INFO("metadata: %u (pack_length_no_ptr)", *metadata_ptr);
  return 1;
}

int Field_json::do_save_field_metadata(unsigned char *metadata_ptr) const
{
  *metadata_ptr = 4;
  return 1;
}

/******************************************************************************
                     Field_enum
******************************************************************************/

int Field_enum::do_save_field_metadata(unsigned char *metadata_ptr) const
{
  *metadata_ptr       = real_type();
  *(metadata_ptr + 1) = pack_length();
  return 2;
}

/******************************************************************************
                     Field_bit
******************************************************************************/

Field_bit::Field_bit(uint32_t len_arg, bool is_nullable_arg, unsigned char null_bit_arg, unsigned char bit_ofs_arg,
    const char *field_name_arg)
    : Field(len_arg, is_nullable_arg, null_bit_arg, field_name_arg),
      bit_ofs(bit_ofs_arg),
      bit_len(len_arg & 7),
      bytes_in_rec(len_arg / 8)
{
  LOG_INFO(
        "len_arg: %u, bit_len: "
        "%u, bytes_in_rec: %u",
        len_arg, bit_len, bytes_in_rec
    );

  set_flag(UNSIGNED_FLAG);

  if (!m_null) {
    null_bit = bit_ofs_arg;
  }
}

int Field_bit::do_save_field_metadata(unsigned char *metadata_ptr) const
{
  LOG_INFO("bit_len: %d, bytes_in_rec: %d", bit_len, bytes_in_rec);
  /*
    Since this class and Field_bit_as_char have different ideas of
    what should be stored here, we compute the values of the metadata
    explicitly using the field_length.
   */
  metadata_ptr[0] = field_length % 8;
  metadata_ptr[1] = field_length / 8;
  return 2;
}

/******************************************************************************
                     Field_temporal
******************************************************************************/

Field_timestamp::Field_timestamp(
    uint32_t len_arg, bool is_nullable_arg, unsigned char null_bit_arg, const char *field_name_arg)
    : Field_temporal_with_date_and_time(is_nullable_arg, null_bit_arg, field_name_arg, 0)
{
  set_flag(TIMESTAMP_FLAG);
  set_flag(UNSIGNED_FLAG);
}

auto make_field(const char *field_name, size_t field_length, bool is_unsigned, bool is_nullable,
    size_t           null_bit, /* 怎么考虑初始化？*/
    enum_field_types field_type, int interval_count, uint decimals) -> FieldRef
{
  //    uchar *bit_ptr = nullptr;
  uchar bit_offset = 0;

  if (field_type == MYSQL_TYPE_BIT) {
    //        bit_ptr = null_pos;
    bit_offset = null_bit;
    if (is_nullable)  // if null field
    {
      //            bit_ptr += (null_bit == 7); // shift bit_ptr and
      //            bit_offset
      bit_offset = (bit_offset + 1) & 7;
    }
  }

  if (!is_nullable) {
    //        null_pos = nullptr; 不用使用 地址，所以只用传入的 is_nullable

    null_bit = 0;
  } else {
    null_bit = ((uchar)1) << null_bit;
  }

  //    if (is_temporal_real_type(field_type)) {
  //        field_charset = &my_charset_numeric; skip
  //    }

  LOG_INFO(
        "field_type: %d, field_length: %zu, "
        "interval: %p",
        field_type, field_length, interval
    );

  /*
    FRMs from 3.23/4.0 can have strings with field_type == MYSQL_TYPE_DECIMAL.
    We should not be getting them after upgrade to new data-dictionary.
  */

  switch (field_type) {
    case MYSQL_TYPE_VAR_STRING:
    case MYSQL_TYPE_STRING: return std::make_shared<Field_string>(field_length, is_nullable, null_bit, field_name);
    case MYSQL_TYPE_VARCHAR:
      return std::make_shared<Field_varstring>(
          field_length, HA_VARCHAR_PACKLENGTH(field_length), is_nullable, null_bit, field_name);
    case MYSQL_TYPE_BLOB:
    case MYSQL_TYPE_MEDIUM_BLOB:
    case MYSQL_TYPE_TINY_BLOB:
    case MYSQL_TYPE_LONG_BLOB: {
      /*
        Field_blob constructor expects number of bytes used to represent
        length of the blob as parameter and not the real field
        pack_length.
      */
      uint pack_length = calc_pack_length(field_type, field_length) - portable_sizeof_char_ptr;
      // field_length 对于 text 和 blob 来说，传进来的都是0，那么
      // pack_lenggh
      switch (pack_length) {
        case 1: field_length = 255; break;
        case 2: field_length = 65535; break;
        case 3: field_length = 16777215; break;
        case 4: field_length = 4294967295; break;
      }
      return std::make_shared<Field_blob>(field_length, is_nullable, null_bit, field_name, true);
    }
    case MYSQL_TYPE_JSON: {
      uint pack_length = calc_pack_length(field_type, field_length) - portable_sizeof_char_ptr;

      return std::make_shared<Field_json>(field_length, is_nullable, null_bit, field_name, pack_length);
    }
    case MYSQL_TYPE_ENUM:
      assert(interval_count != 0);
      return std::make_shared<Field_enum>(
          field_length, is_nullable, null_bit, field_name, get_enum_pack_length(interval_count));
    case MYSQL_TYPE_SET:
      assert(interval_count != 0);
      return std::make_shared<Field_set>(
          field_length, is_nullable, null_bit, field_name, get_set_pack_length(interval_count));
    case MYSQL_TYPE_DECIMAL:  // never
      return std::make_shared<Field_decimal>(field_length, is_nullable, null_bit, field_name, decimals, is_unsigned);
    case MYSQL_TYPE_NEWDECIMAL:
      return std::make_shared<Field_new_decimal>(
          field_length, is_nullable, null_bit, field_name, decimals, is_unsigned);
    case MYSQL_TYPE_FLOAT:
      return std::make_shared<Field_float>(field_length, is_nullable, null_bit, field_name, decimals, is_unsigned);
    case MYSQL_TYPE_DOUBLE:
      return std::make_shared<Field_double>(field_length, is_nullable, null_bit, field_name, decimals, is_unsigned);
    case MYSQL_TYPE_TINY:
      return std::make_shared<Field_tiny>(field_length, is_nullable, null_bit, field_name, is_unsigned);
    case MYSQL_TYPE_SHORT:
      return std::make_shared<Field_short>(field_length, is_nullable, null_bit, field_name, is_unsigned);
    case MYSQL_TYPE_INT24:
      return std::make_shared<Field_medium>(field_length, is_nullable, null_bit, field_name, is_unsigned);
    case MYSQL_TYPE_LONG:
      return std::make_shared<Field_long>(field_length, is_nullable, null_bit, field_name, is_unsigned);
    case MYSQL_TYPE_LONGLONG:
      return std::make_shared<Field_longlong>(field_length, is_nullable, null_bit, field_name, is_unsigned);
    case MYSQL_TYPE_YEAR: return std::make_shared<Field_year>(is_nullable, null_bit, field_name);
    case MYSQL_TYPE_TIMESTAMP:
      return std::make_shared<Field_timestamp>(field_length, is_nullable, null_bit, field_name);
    case MYSQL_TYPE_TIME: return std::make_shared<Field_time>(is_nullable, null_bit, field_name);
    case MYSQL_TYPE_DATETIME: return std::make_shared<Field_datetime>(is_nullable, null_bit, field_name);
    case MYSQL_TYPE_NULL: return std::make_shared<Field_null>(field_length, field_name);
    case MYSQL_TYPE_BIT:
      return std::make_shared<Field_bit>(field_length, is_nullable, null_bit, bit_offset, field_name);
    case MYSQL_TYPE_INVALID:
    case MYSQL_TYPE_BOOL:
    case MYSQL_TYPE_TIMESTAMP2:
    case MYSQL_TYPE_TIME2:
    case MYSQL_TYPE_DATETIME2:
    case MYSQL_TYPE_NEWDATE: LOG_INFO(
                "Field type %d not impl, refer to enum_field_types.h status code",
                field_type
            );
    default: break;
  }
  return nullptr;
}

enum_field_types get_blob_type_from_length(size_t length)
{
  enum_field_types type;
  if (length < 256) {
    type = MYSQL_TYPE_TINY_BLOB;
  } else if (length < 65536) {
    type = MYSQL_TYPE_BLOB;
  } else if (length < 256L * 256L * 256L) {
    type = MYSQL_TYPE_MEDIUM_BLOB;
  } else {
    type = MYSQL_TYPE_LONG_BLOB;
  }
  return type;
}

size_t calc_pack_length(enum_field_types type, size_t length)
{
  switch (type) {
    case MYSQL_TYPE_VAR_STRING:
    case MYSQL_TYPE_STRING:
    case MYSQL_TYPE_DECIMAL: return (length);
    case MYSQL_TYPE_VARCHAR: return (length + (length < 256 ? 1 : 2));
    case MYSQL_TYPE_BOOL:
    case MYSQL_TYPE_YEAR:
    case MYSQL_TYPE_TINY: return 1;
    case MYSQL_TYPE_SHORT: return 2;
    case MYSQL_TYPE_INT24:
    case MYSQL_TYPE_NEWDATE: return 3;
    case MYSQL_TYPE_TIME: return 3;
    case MYSQL_TYPE_TIME2: return length > MAX_TIME_WIDTH ? my_time_binary_length(length - MAX_TIME_WIDTH - 1) : 3;
    case MYSQL_TYPE_TIMESTAMP: return 4;
    case MYSQL_TYPE_TIMESTAMP2:
      return length > MAX_DATETIME_WIDTH ? my_timestamp_binary_length(length - MAX_DATETIME_WIDTH - 1) : 4;
    case MYSQL_TYPE_DATE:
    case MYSQL_TYPE_LONG: return 4;
    case MYSQL_TYPE_FLOAT: return sizeof(float);
    case MYSQL_TYPE_DOUBLE: return sizeof(double);
    case MYSQL_TYPE_DATETIME: return 8;
    case MYSQL_TYPE_DATETIME2:
      return length > MAX_DATETIME_WIDTH ? my_datetime_binary_length(length - MAX_DATETIME_WIDTH - 1) : 5;
    case MYSQL_TYPE_LONGLONG: return 8; /* Don't crash if no longlong */
    case MYSQL_TYPE_NULL: return 0;
    case MYSQL_TYPE_TINY_BLOB: return 1 + portable_sizeof_char_ptr;
    case MYSQL_TYPE_BLOB: return 2 + portable_sizeof_char_ptr;
    case MYSQL_TYPE_MEDIUM_BLOB: return 3 + portable_sizeof_char_ptr;
    case MYSQL_TYPE_LONG_BLOB: return 4 + portable_sizeof_char_ptr;
    case MYSQL_TYPE_GEOMETRY: return 4 + portable_sizeof_char_ptr;
    case MYSQL_TYPE_JSON: return 4 + portable_sizeof_char_ptr;
    case MYSQL_TYPE_SET:
    case MYSQL_TYPE_ENUM:
    case MYSQL_TYPE_NEWDECIMAL: assert(false); return 0;  // This shouldn't happen
    case MYSQL_TYPE_BIT: return length / 8;
    case MYSQL_TYPE_INVALID:
    case MYSQL_TYPE_TYPED_ARRAY: break;
  }
  assert(false);
  return 0;
}

unsigned int my_time_binary_length(unsigned int dec)
{
  LOFT_ASSERT(dec <= DATETIME_MAX_DECIMALS, "time dec is too large");
  return 3 + (dec + 1) / 2;
}

unsigned int my_datetime_binary_length(unsigned int dec)
{
  LOFT_ASSERT(dec <= DATETIME_MAX_DECIMALS, "datetime dec is toolarge");
  return 5 + (dec + 1) / 2;
}

unsigned int my_timestamp_binary_length(unsigned int dec)
{
  LOFT_ASSERT(dec <= DATETIME_MAX_DECIMALS, "timestamp dec is toolarge");
  return 4 + (dec + 1) / 2;
}

}  // namespace mysql
