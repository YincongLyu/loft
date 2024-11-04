#include <stdint.h>
#include <vector>
#include <fstream>
#include <string.h>
#include <unordered_map>
#include <algorithm>
#include <little_endian.h>
#include <abstract_event.h>
#include "./binlogevents/table_id.h"
#include <string>
#include "demical.h"

enum enum_field_types {
    MYSQL_TYPE_DECIMAL,
    MYSQL_TYPE_TINY,
    MYSQL_TYPE_SMALLINT,
    MYSQL_TYPE_SHORT,
    MYSQL_TYPE_LONG,
    MYSQL_TYPE_FLOAT,
    MYSQL_TYPE_DOUBLE,
    MYSQL_TYPE_NULL,
    MYSQL_TYPE_TIMESTAMP,
    MYSQL_TYPE_LONGLONG,
    MYSQL_TYPE_INT24,
    MYSQL_TYPE_DATE,
    MYSQL_TYPE_TIME,
    MYSQL_TYPE_DATETIME,
    MYSQL_TYPE_YEAR,
    MYSQL_TYPE_NEWDATE, /**< Internal to MySQL. Not used in protocol */
    MYSQL_TYPE_VARCHAR,
    MYSQL_TYPE_BIT,
    MYSQL_TYPE_TIMESTAMP2,
    MYSQL_TYPE_DATETIME2,   /**< Internal to MySQL. Not used in protocol */
    MYSQL_TYPE_TIME2,       /**< Internal to MySQL. Not used in protocol */
    MYSQL_TYPE_TYPED_ARRAY, /**< Used for replication only */
    MYSQL_TYPE_INVALID = 243,
    MYSQL_TYPE_BOOL = 244, /**< Currently just a placeholder */
    MYSQL_TYPE_JSON = 245,
    MYSQL_TYPE_NEWDECIMAL = 246,
    MYSQL_TYPE_ENUM = 247,
    MYSQL_TYPE_SET = 248,
    MYSQL_TYPE_TINY_BLOB = 249,
    MYSQL_TYPE_MEDIUM_BLOB = 250,
    MYSQL_TYPE_LONG_BLOB = 251,
    MYSQL_TYPE_BLOB = 252,
    MYSQL_TYPE_VAR_STRING = 253,
    MYSQL_TYPE_STRING = 254,
    MYSQL_TYPE_GEOMETRY = 255
};

class Rows_event : public AbstractEvent{
public :

  Rows_event(uint64_t id, unsigned long wid,uint16_t flag, Log_event_type type);

  ~Rows_event();

 void Set_flags(uint16_t flags) { m_flags = flags; }
 void Set_width(unsigned long width) { m_width = width; }

  template<typename T>
  void set_row_after_buf(T &row, size_t str_length) {

    data_to_binary(m_rows_after_buf,row, data_size, str_length);

  }

  template<typename T>
  void set_row_before_buf(T &row) {
    data_to_binary(m_rows_before_buf,row, data_size);
  }

  template<typename T>
  void data_to_binary(uint8_t* &buf, T *data, std::string &type_string, size_t length, size_t str_length,int precision, int frac) {
    enum_field_types type = f_types[type_string];
    switch (type)
    {
    case enum_field_types::MYSQL_TYPE_TINY: {
      size_t tiny_byte = 1;
      buf_resize(buf, data_size, data_size + tiny_byte);
      memcpy(buf + data_size, data, tiny_byte);
      data_size += tiny_byte;
      break;
    }
    case enum_field_types::MYSQL_TYPE_SMALLINT :{      
      size_t  small_byte = 2;
      buf_resize(buf, data_size, data_size + small_byte);
      memcpy(buf + data_size, data, small_byte);
      data_size += small_byte;
      break;
    }
    case enum_field_types::MYSQL_TYPE_FLOAT :{
      size_t  float_byte = 4;
      buf_resize(buf, data_size, data_size + float_byte);
      memcpy(buf + data_size, data, float_byte);
      data_size += float_byte;
      break;
    }
    case enum_field_types::MYSQL_TYPE_DOUBLE :{
      size_t  double_byte = 4;
      buf_resize(buf, data_size, data_size + double_byte);
      memcpy(buf + data_size, data, double_byte);
      data_size += double_byte;
      break;   
    }
    case enum_field_types::MYSQL_TYPE_YEAR :{
      size_t  year_byte = 1;
      buf_resize(buf, data_size, data_size + year_byte);
      memcpy(buf + data_size, data, year_byte);
      data_size += year_byte;
      break;
    }
    case enum_field_types::MYSQL_TYPE_DATE :{
      size_t  date_byte = 1;
      buf_resize(buf, data_size, data_size + date_byte);
      memcpy(buf + data_size, data, date_byte);
      data_size += date_byte;
      break;
    }
    case enum_field_types::MYSQL_TYPE_TINY_BLOB :{
      size_t  tinytext_byte = str_length + 1;
      buf_resize(buf, data_size, data_size + tinytext_byte);
      memcpy(buf + data_size, &str_length, 1);
      data_size += 1;
      memcpy(buf + data_size, data, str_length);
      data_size += str_length;
      break;
    }
    case enum_field_types::MYSQL_TYPE_BLOB :{
      size_t  text_byte = str_length + 2;
      buf_resize(buf, data_size, data_size + text_byte);
      memcpy(buf + data_size, &str_length, 2);
      data_size += 2;
      memcpy(buf + data_size, data, str_length);
      data_size += str_length;
      break;
    }
    case enum_field_types::MYSQL_TYPE_MEDIUM_BLOB :{
      size_t  mediumtext_byte = str_length + 3;
      buf_resize(buf, data_size, data_size + mediumtext_byte);
      memcpy(buf + data_size, &str_length, 3);
      data_size += 3;
      memcpy(buf + data_size, data, str_length);
      data_size += str_length;
      break;
    }
    case enum_field_types::MYSQL_TYPE_LONG_BLOB :{
      size_t  longtext_byte = str_length + 4;
      buf_resize(buf, data_size, data_size + longtext_byte);
      memcpy(buf + data_size, &str_length, 4);
      data_size += 4;
      memcpy(buf + data_size, data, str_length);
      data_size += str_length;
      break;
    }
    case enum_field_types::MYSQL_TYPE_DECIMAL :{
      decimal_t t;
      double2demi(*data, t, precision, frac);
      size_t demi_size = dig2bytes[t.intg % 9] + (t.intg / 9) * 4 + dig2bytes[t.frac % 9] + (t.frac / 9) * 4;
      buf_resize(buf, data_size, data_size + demi_size);
      decimal2bin(&t, buf + data_size, precision, frac);
      data_size += demi_size;
      break;
    }
    case enum_field_types::MYSQL_TYPE_ENUM :{
      size_t enum_size;
      if(length >= 256) enum_size = 2;
      else enum_size = 1;
      buf_resize(buf, data_size, data_size + enum_size);
      memcpy(buf + data_size, data, enum_size);
      data_size += enum_size;
      break;
    }
    case enum_field_types::MYSQL_TYPE_SET :{
      size_t set_size;
      if(length >= 1 && length <= 8) {
        set_size = 1;
      } else if(length >= 9 && length <= 16) {
        set_size = 2;
      } else if(length >= 17 && length <= 24) {
        set_size = 3;
      } else if(length >= 25 && length <= 32) {
        set_size = 4;
      } else if(length >= 33 && length <= 64) {
        set_size = 8;
      }
      buf_resize(buf, data_size, data_size + set_size);
      memcpy(buf + data_size, data, set_size);
      data_size += set_size;
      break;
    }
    case enum_field_types::MYSQL_TYPE_BIT :{
    size_t bit_size;
    bit_size = (length + 7) / 9;
    buf_resize(buf, data_size, data_size + bit_size);
    memcpy(buf + data_size, data, bit_size);
    data_size += bit_size;
      break;
    case enum_field_types::MYSQL_TYPE_STRING :
      size_t char_size = 0;
      if(length > 255) {
        char_size = 2;
      }else {
        char_size = 1;
      }
      buf_resize(buf, data_size, data_size + char_size + str_length);
      memcpy(buf + data_size, &str_length, char_size);
      data_size += char_size;
      memcpy(buf + data_size, data, str_length);
      data_size += str_length;
      break;
    }
    case enum_field_types::MYSQL_TYPE_VARCHAR :{
      size_t vchar_size = 0;
      if(length > 255) {
        vchar_size = 2;
      }else {
        vchar_size = 1;
      }
      buf_resize(buf, data_size, data_size + vchar_size + str_length);
      memcpy(buf + data_size, &str_length, vchar_size);
      data_size += vchar_size;
      memcpy(buf + data_size, data, str_length);
      data_size += str_length;
      break;
    }
    case enum_field_types::MYSQL_TYPE_JSON :
      break;
    default: 
      break;
    }
    return ;
}

  void buf_resize(uint8_t* &buf, size_t size1, size_t size2);

  void double2demi(double num, decimal_t &t, int precision, int frac);

  int Get_N() {
    int N = (m_width + 7)/8;
    return N;
  }

  void cols_init() ;

  void bits_init();

  void set_rows_after_exist(std::vector<int> &rows) {
    this->rows_after_exist = rows;
  }

  void set_rows_before_exist(std::vector<int> &rows) {
    this->rows_before_exist = rows;
  }

  void set_rows_after(std::vector<int> &rows) {
    this->rows_after = rows;
  }

  void set_rows_before(std::vector<int> &rows) {
    this->rows_before= rows;
  }
  
  void set_data_size1() {
    data_size1 = data_size;
  }

  void set_dat_size2() {
    data_size2 = data_size - data_size1;
  }

  template<typename T>
  void write_data_before(T *data, std::string &type, size_t length, size_t str_length,int precision, int frac){
    data_to_binary(m_rows_before_buf, data, type, length, str_length, precision, frac);
  }

  template<typename T>
  void write_data_after(T *data, std::string &type, size_t length, size_t str_length,int precision, int frac) {
    data_to_binary(m_rows_after_buf, data, type, length, str_length, precision, frac);
}
  bool write(Basic_ostream *ostream) override;
  bool write_data_header(Basic_ostream *) override;
  bool write_data_body(Basic_ostream *) override;

private :
  Table_id m_table_id;
  uint16_t m_flags; /** Flags for row-level events */
  Log_event_type m_type; 
  unsigned long m_width;
  unsigned char* columns_before_image;
  unsigned char* columns_after_image ;
  uchar *row_bitmap_after ;
  uchar *row_bitmap_before ;
  unsigned char* m_rows_before_buf;
  unsigned char* m_rows_after_buf;
  std::vector<int> rows_before;
  std::vector<int> rows_after;
  std::vector<int> rows_after_exist;
  std::vector<int> rows_before_exist;  
  size_t data_size;
  size_t data_size1;
  size_t data_size2;


  std::unordered_map<std::string, enum_field_types>  f_types = {
    {"DECIMAL", enum_field_types::MYSQL_TYPE_DECIMAL},
    {"TINYINT", enum_field_types::MYSQL_TYPE_TINY},
    {"SMALLINT", enum_field_types::MYSQL_TYPE_SMALLINT},
    {"FLOAT", enum_field_types::MYSQL_TYPE_FLOAT},
    {"DOUBLE", enum_field_types::MYSQL_TYPE_DOUBLE},
    {"REAL", enum_field_types::MYSQL_TYPE_DOUBLE},
    {"YEAR", enum_field_types::MYSQL_TYPE_YEAR},
    {"DATE", enum_field_types::MYSQL_TYPE_DATE},
    {"TINYTEXT", enum_field_types::MYSQL_TYPE_TINY_BLOB},
    {"TINYBLOB", enum_field_types::MYSQL_TYPE_TINY_BLOB},
    {"TEXT", enum_field_types::MYSQL_TYPE_BLOB},
    {"BLOB", enum_field_types::MYSQL_TYPE_BLOB},
    {"MEDIUMTEXT", enum_field_types::MYSQL_TYPE_MEDIUM_BLOB},
    {"MEDIUMBLOB", enum_field_types::MYSQL_TYPE_MEDIUM_BLOB},
    {"LONGTEXT", enum_field_types::MYSQL_TYPE_LONG_BLOB},
    {"LONGBLOB", enum_field_types::MYSQL_TYPE_LONG_BLOB},
    {"ENUM", enum_field_types::MYSQL_TYPE_ENUM},
    {"SET", enum_field_types::MYSQL_TYPE_SET},
    {"BIT", enum_field_types::MYSQL_TYPE_BIT},
    {"CHAR", enum_field_types::MYSQL_TYPE_STRING},
    {"VARCHAR", enum_field_types::MYSQL_TYPE_VARCHAR},
    {"JSON", enum_field_types::MYSQL_TYPE_JSON},
  };
};
