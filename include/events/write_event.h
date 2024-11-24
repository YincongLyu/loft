#pragma once

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <string>
#include <unordered_map>
#include <vector>
#include <chrono>

#include "events/abstract_event.h"
#include "utils/decimal.h"
#include "utils/table_id.h"
#include "utils/little_endian.h"
#include "common/logging.h"

class Rows_event : public AbstractEvent
{
public:
  Rows_event(
      const Table_id &tid, unsigned long wid, uint16 flag, Log_event_type type, uint64 immediate_commit_timestamp_arg);
  ~Rows_event() override;

  DISALLOW_COPY(Rows_event);

  void Set_flags(uint16_t flags) { m_flags = flags; }

  void Set_width(unsigned long width) { m_width = width; }

  //    void buf_resize(uint8_t* &buf, size_t size1, size_t size2);
  void buf_resize(std::unique_ptr<uchar[]>& buf, size_t& capacity, size_t current_size, size_t needed_size);

  void double2demi(double num, decimal_t &t, int precision, int frac);

  int Get_N()
  {
    int N = (m_width + 7) / 8;
    return N;
  }

  void cols_init();

  /*
    delete,update
  */
  void set_null_before(std::vector<uint8> &&t)
  {
    assert(t.size() == rows_before.size());
    null_before = std::move(t);
  }

  /*
    insert,update
  */
  void set_null_after(std::vector<uint8> &&t)
  {
    assert(t.size() == rows_after.size());
    null_after = std::move(t);
  }

  /*
    insert,update
  */
  void set_rows_after(std::vector<int> &&t)
  {
    assert(t.size() <= m_width);
    this->rows_after = std::move(t);
  }

  /*
    delete,update
  */
  void set_rows_before(std::vector<int> &&t)
  {
    assert(t.size() <= m_width);
    this->rows_before = std::move(t);
  }

  size_t get_data_size() override { return calculate_event_size(); }

  /**
   * @brief 每个 row value 连续追加写到 buf 中
   * @param buf
   * @param data 实际值
   * @param capacity 目前 buf 已分配的容量
   * @param data_size 使用的大小
   * @param type Field type
   * @param length Field 占的 byte 数
   * @param str_length 字符串长度
   * @param precision 精度
   * @param frac 小数点后的位数
   */
  template <typename T>
  void data_to_binary(std::unique_ptr<uchar[]>& buf, T* data, size_t& capacity, size_t& data_size,
      enum_field_types type, size_t length, size_t str_length, int precision, int frac) {

    switch (type) {
      // Fixed-length numeric types
      case enum_field_types::MYSQL_TYPE_TINY:
        handle_fixed_length(buf, static_cast<void *>(data), capacity, data_size, 1);
        break;
      case enum_field_types::MYSQL_TYPE_SHORT:
        handle_fixed_length(buf, data, capacity, data_size, 2);
        break;
      case enum_field_types::MYSQL_TYPE_LONG:
        handle_fixed_length(buf, data, capacity, data_size, 4);
        break;
      case enum_field_types::MYSQL_TYPE_LONGLONG:
        handle_fixed_length(buf, data, capacity, data_size, 8);
        break;
      case enum_field_types::MYSQL_TYPE_INT24:
        handle_fixed_length(buf, data, capacity, data_size, 3);
        break;
      case enum_field_types::MYSQL_TYPE_YEAR:
        handle_fixed_length(buf, data, capacity, data_size, 1);
        break;
      case enum_field_types::MYSQL_TYPE_DATE:
        handle_fixed_length(buf, data, capacity, data_size, 1);
        break;

      // Types that use the length parameter
      case enum_field_types::MYSQL_TYPE_FLOAT:
      case enum_field_types::MYSQL_TYPE_DOUBLE:
      case enum_field_types::MYSQL_TYPE_ENUM:
      case enum_field_types::MYSQL_TYPE_SET:
      case enum_field_types::MYSQL_TYPE_BIT:
        handle_fixed_length(buf, data, capacity, data_size, length);
        break;

      // BLOB type
      case enum_field_types::MYSQL_TYPE_BLOB: {
        size_t text_byte = str_length + length;
        buf_resize(buf, capacity, data_size, data_size + text_byte);
        memcpy(buf.get() + data_size, &str_length, length);
        data_size += length;
        memcpy(buf.get() + data_size, data, str_length);
        data_size += str_length;
        break;
      }

      // DECIMAL type
      case enum_field_types::MYSQL_TYPE_NEWDECIMAL: {
        decimal_t t;
        double2demi(*data, t, precision, frac);
        size_t demi_size = dig2bytes[t.intg % 9] + (t.intg / 9) * 4 +
                           dig2bytes[t.frac % 9] + (t.frac / 9) * 4;
        buf_resize(buf, capacity, data_size, data_size + demi_size);
        decimal2bin(&t, buf.get() + data_size, precision, frac);
        data_size += demi_size;

        delete[] t.buf;
        break;
      }

      // String types
      case enum_field_types::MYSQL_TYPE_VARCHAR:
      case enum_field_types::MYSQL_TYPE_STRING:
        handle_string_type(buf, data, capacity, data_size, length, str_length);
        break;
      // Unimplemented types
      case enum_field_types::MYSQL_TYPE_JSON:
      case enum_field_types::MYSQL_TYPE_TIMESTAMP:
      case enum_field_types::MYSQL_TYPE_TIME:
      case enum_field_types::MYSQL_TYPE_DATETIME:
        break;

      default:
        break;
    }
  }

  template <typename T>
  void write_data_before(
      T *data, enum_field_types type, size_t length = 0, size_t str_length = 0, int precision = 0, int frac = 0)
  {
    data_to_binary(m_rows_before_buf, data, m_before_capacity, before_data_size_used, type, length, str_length, precision, frac);
  }

  template <typename T>
  void write_data_after(
      T *data, enum_field_types type, size_t length = 0, size_t str_length = 0, int precision = 0, int frac = 0)
  {
    data_to_binary(m_rows_after_buf, data, m_after_capacity, after_data_size_used, type, length, str_length, precision, frac);
  }
  // 统一Rows event 的数据写入接口，被不同类型的 handler 调用
  template <typename T>
  void writeData(
      T *data, enum_field_types type, size_t length = 0, size_t str_length = 0, int precision = 0, int frac = 0)
  {
    if (m_is_before) {
      write_data_before(data, type, length, str_length, precision, frac);
    } else {
      write_data_after(data, type, length, str_length, precision, frac);
    }
  }
  void setBefore(bool is_before) { m_is_before = is_before; }

  bool write(Basic_ostream *ostream) override;
  bool write_data_header(Basic_ostream *) override;
  bool write_data_body(Basic_ostream *) override;

  size_t write_data_header_to_buffer(uchar* buffer) override;
  size_t write_data_body_to_buffer(uchar* buffer) override;

private:
  size_t calculate_event_size();

  inline void handle_fixed_length(std::unique_ptr<uchar[]>& buf, void* data,
      size_t& capacity, size_t& data_size,
      size_t bytes) {
    buf_resize(buf, capacity, data_size, data_size + bytes);
    memcpy(buf.get() + data_size, data, bytes);
    data_size += bytes;
  }

  inline void handle_string_type(std::unique_ptr<uchar[]>& buf, void* data,
      size_t& capacity, size_t& data_size,
      size_t length, size_t str_length) {
    size_t len_bytes = (length > 255) ? 2 : 1;
    buf_resize(buf, capacity, data_size, data_size + len_bytes + str_length);
    memcpy(buf.get() + data_size, &str_length, len_bytes);
    data_size += len_bytes;
    memcpy(buf.get() + data_size, data, str_length);
    data_size += str_length;
  }

private:
  Table_id       m_table_id;
  uint16_t       m_flags; /** Flags for row-level events */
  Log_event_type m_type;
  unsigned long  m_width;

  std::unique_ptr<uchar[]> columns_before_image;
  std::unique_ptr<uchar[]> columns_after_image;
  std::unique_ptr<uchar[]> row_bitmap_before;
  std::unique_ptr<uchar[]> row_bitmap_after;

  std::unique_ptr<uchar[]> m_rows_before_buf;
  std::unique_ptr<uchar[]> m_rows_after_buf;
  size_t m_before_capacity;  // 当前已分配的容量
  size_t m_after_capacity;
  size_t before_data_size_used;  // 实际使用的大小
  size_t after_data_size_used;

  std::vector<int>  rows_before;
  std::vector<int>  rows_after;
  std::vector<uint8> null_after;
  std::vector<uint8> null_before;

  bool              m_is_before;
};
