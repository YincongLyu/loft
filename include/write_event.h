#include "decimal.h"
#include "field_types.h"
#include "init_setting.h"
#include "table_id.h"
#include <abstract_event.h>
#include <algorithm>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <little_endian.h>
#include <string>
#include <unordered_map>
#include <vector>

class Rows_event : public AbstractEvent {
  public:
    Rows_event(
        const Table_id &tid,
        unsigned long wid,
        uint16_t flag,
        Log_event_type type
    );
    ~Rows_event() override;

    DISALLOW_COPY(Rows_event);

    void Set_flags(uint16_t flags) { m_flags = flags; }

    void Set_width(unsigned long width) { m_width = width; }

    //    void buf_resize(uint8_t* &buf, size_t size1, size_t size2);
    void buf_resize(std::unique_ptr<uchar[]> &buf, size_t size1, size_t size2);

    void double2demi(double num, decimal_t &t, int precision, int frac);

    int Get_N() {
        int N = (m_width + 7) / 8;
        return N;
    }

    void cols_init();

    /*
      delete,update
    */
    void set_null_before(std::vector<bool> t) {
        assert(t.size() == rows_before.size());
        null_before = t;
    }

    /*
      insert,update
    */
    void set_null_after(std::vector<bool> t) {
        assert(t.size() == rows_after.size());
        null_after = t;
    }

    /*
      insert,update
    */
    void set_rows_after(std::vector<int> rows) {
        assert(rows.size() <= m_width);
        this->rows_after = rows;
    }

    /*
      delete,update
    */
    void set_rows_before(std::vector<int> rows) {
        assert(rows.size() <= m_width);
        this->rows_before = rows;
    }

    size_t get_data_size() override { return calculate_event_size(); }

    // 模板函数定义需要与使用它的代码在同一编译单元中
    template<typename T>
    void data_to_binary(
        std::unique_ptr<uchar[]> &buf,
        T *data,
        size_t &data_size,
        enum_field_types type,
        size_t length,
        size_t str_length,
        int precision,
        int frac
    ) {
        switch (type) {
            case enum_field_types::MYSQL_TYPE_TINY: {
                size_t tiny_byte = 1;
                buf_resize(buf, data_size, data_size + tiny_byte);
                memcpy(buf.get() + data_size, data, tiny_byte);
                data_size += tiny_byte;
                break;
            }
            case enum_field_types::MYSQL_TYPE_SHORT: {
                size_t small_byte = 2;
                buf_resize(buf, data_size, data_size + small_byte);
                memcpy(buf.get() + data_size, data, small_byte);
                data_size += small_byte;
                break;
            }
            case enum_field_types::MYSQL_TYPE_LONG: {
                size_t int_byte = 4;
                buf_resize(buf, data_size, data_size + int_byte);
                memcpy(buf.get() + data_size, data, int_byte);
                data_size += int_byte;
                break;
            }
            case enum_field_types::MYSQL_TYPE_LONGLONG: {
                size_t bigint_byte = 8;
                buf_resize(buf, data_size, data_size + bigint_byte);
                memcpy(buf.get() + data_size, data, bigint_byte);
                data_size += bigint_byte;
                break;
            }
            case enum_field_types::MYSQL_TYPE_INT24: {
                size_t mediumint_byte = 3;
                buf_resize(buf, data_size, data_size + mediumint_byte);
                memcpy(buf.get() + data_size, data, mediumint_byte);
                data_size += mediumint_byte;
                break;
            }
            case enum_field_types::MYSQL_TYPE_FLOAT: {
                //                size_t  float_byte = 4;
                buf_resize(buf, data_size, data_size + length);
                memcpy(buf.get() + data_size, data, length);
                data_size += length;
                break;
            }
            case enum_field_types::MYSQL_TYPE_DOUBLE: {
                buf_resize(buf, data_size, data_size + length);
                memcpy(buf.get() + data_size, data, length);
                data_size += length;
                break;
            }
            case enum_field_types::MYSQL_TYPE_YEAR: {
                size_t year_byte = 1;
                buf_resize(buf, data_size, data_size + year_byte);
                memcpy(buf.get() + data_size, data, year_byte);
                data_size += year_byte;
                break;
            }
            case enum_field_types::MYSQL_TYPE_DATE: {
                size_t date_byte = 1;
                buf_resize(buf, data_size, data_size + date_byte);
                memcpy(buf.get() + data_size, data, date_byte);
                data_size += date_byte;
                break;
            }
            case enum_field_types::MYSQL_TYPE_BLOB: {
                size_t text_byte = str_length + length;
                buf_resize(buf, data_size, data_size + text_byte);
                memcpy(buf.get() + data_size, &str_length, length);
                data_size += length;
                memcpy(buf.get() + data_size, data, str_length);
                data_size += str_length;
                break;
            }
            case enum_field_types::MYSQL_TYPE_NEWDECIMAL: {
                decimal_t t;
                double2demi(*data, t, precision, frac);
                size_t demi_size = dig2bytes[t.intg % 9] + (t.intg / 9) * 4
                                   + dig2bytes[t.frac % 9] + (t.frac / 9) * 4;
                buf_resize(buf, data_size, data_size + demi_size);
                decimal2bin(&t, buf.get() + data_size, precision, frac);
                data_size += demi_size;
                break;
            }
            case enum_field_types::MYSQL_TYPE_ENUM: {
                buf_resize(buf, data_size, data_size + length);
                memcpy(buf.get() + data_size, data, length);
                data_size += length;
                break;
            }
            case enum_field_types::MYSQL_TYPE_SET: {
                buf_resize(buf, data_size, data_size + length);
                memcpy(buf.get() + data_size, data, length);
                data_size += length;
                break;
            }
            case enum_field_types::MYSQL_TYPE_BIT: {
                buf_resize(buf, data_size, data_size + length);
                memcpy(buf.get() + data_size, data, length);
                data_size += length;
                break;
            }
            case enum_field_types::MYSQL_TYPE_STRING: {
                size_t char_size = 0;
                if (length > 255) {
                    char_size = 2;
                } else {
                    char_size = 1;
                }
                buf_resize(buf, data_size, data_size + char_size + str_length);
                memcpy(buf.get() + data_size, &str_length, char_size);
                data_size += char_size;
                memcpy(buf.get() + data_size, data, str_length);
                data_size += str_length;
                break;
            }
            case enum_field_types::MYSQL_TYPE_VARCHAR: {
                size_t vchar_size = 0;
                if (length > 255) {
                    vchar_size = 2;
                } else {
                    vchar_size = 1;
                }
                buf_resize(buf, data_size, data_size + vchar_size + str_length);
                memcpy(buf.get() + data_size, &str_length, vchar_size);
                data_size += vchar_size;
                memcpy(buf.get() + data_size, data, str_length);
                data_size += str_length;
                break;
            }
            case enum_field_types::MYSQL_TYPE_JSON:
                break;
            case enum_field_types::MYSQL_TYPE_TIMESTAMP:
                break;
            case enum_field_types::MYSQL_TYPE_TIME:
                break;
            case enum_field_types::MYSQL_TYPE_DATETIME:
                break;
            default:
                break;
        }
        return;
    }

    template<typename T>
    void write_data_before(
        T *data,
        enum_field_types type,
        size_t length = 0,
        size_t str_length = 0,
        int precision = 0,
        int frac = 0
    ) {
        data_to_binary(
            m_rows_before_buf, data, data_size1, type, length, str_length,
            precision, frac
        );
    }

    template<typename T>
    void write_data_after(
        T *data,
        enum_field_types type,
        size_t length = 0,
        size_t str_length = 0,
        int precision = 0,
        int frac = 0
    ) {
        data_to_binary(
            m_rows_after_buf, data, data_size2, type, length, str_length,
            precision, frac
        );
    }

    bool write(Basic_ostream *ostream) override;
    bool write_data_header(Basic_ostream *) override;
    bool write_data_body(Basic_ostream *) override;

  private:
    size_t calculate_event_size();

  private:
    Table_id m_table_id;
    uint16_t m_flags; /** Flags for row-level events */
    Log_event_type m_type;
    unsigned long m_width;

    std::unique_ptr<uchar[]> columns_before_image;
    std::unique_ptr<uchar[]> columns_after_image;
    std::unique_ptr<uchar[]> row_bitmap_before;
    std::unique_ptr<uchar[]> row_bitmap_after;
    std::unique_ptr<uchar[]> m_rows_before_buf;
    std::unique_ptr<uchar[]> m_rows_after_buf;

    std::vector<int> rows_before;
    std::vector<int> rows_after;
    std::vector<bool> null_after;
    std::vector<bool> null_before;
    size_t data_size1 = 0;
    size_t data_size2 = 0;
};
