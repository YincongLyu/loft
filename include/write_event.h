#include <stdint.h>
#include <vector>
#include <fstream>
#include <string.h>
#include <unordered_map>
#include <algorithm>
#include <little_endian.h>
#include <abstract_event.h>
#include "table_id.h"
#include <string>
#include "demical.h"
#include "field_types.h"
#include "my_time.h"
#include "my_json.h"

class Rows_event : public AbstractEvent{
public :

  Rows_event(uint64_t id, unsigned long wid,uint16_t flag, Log_event_type type);

  ~Rows_event();

 void Set_flags(uint16_t flags) { m_flags = flags; }
 void Set_width(unsigned long width) { m_width = width; }

  template<typename T>
  void data_to_binary(uint8_t* &buf, T *data, size_t &data_size, std::string &type_string, size_t length, size_t str_length,int precision, int frac) {
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
    case enum_field_types::MYSQL_TYPE_INT :{   
      size_t  int_byte = 4;
      buf_resize(buf, data_size, data_size + int_byte);
      memcpy(buf + data_size, data, int_byte);
      data_size += int_byte;
      break;
    }
    case enum_field_types::MYSQL_TYPE_BIGINT :{      
      size_t  bigint_byte = 8;
      buf_resize(buf, data_size, data_size + bigint_byte);
      memcpy(buf + data_size, data, bigint_byte);
      data_size += bigint_byte;
      break;
    }
    case enum_field_types::MYSQL_TYPE_MEDIUMINT :{      
      size_t  mediumint_byte = 3;
      buf_resize(buf, data_size, data_size + mediumint_byte);
      memcpy(buf + data_size, data, mediumint_byte);
      data_size += mediumint_byte;
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
    case enum_field_types::MYSQL_TYPE_SET : {
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
    case enum_field_types::MYSQL_TYPE_BIT : {
    size_t bit_size;
    bit_size = (length + 7) / 9;
    buf_resize(buf, data_size, data_size + bit_size);
    memcpy(buf + data_size, data, bit_size);
    data_size += bit_size;
      break;
    }
    case enum_field_types::MYSQL_TYPE_STRING : {
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
    case enum_field_types::MYSQL_TYPE_JSON : {
      Json_dom_ptr json_ptr = Json_dom::parse(data,str_length);
      std::string dest;
      json2bin(json_ptr.get(),&dest);
      buf_resize(buf, data_size, data_size + dest.size() + 4);
      store_json_binary(dest.c_str(), dest.size(), buf);
      data_size += dest.size() + 4;
      break;
    }
    case enum_field_types::MYSQL_TYPE_TIME : {
      size_t time_size = 3;
      MYSQL_TIME ltime;
      if(precision == 1 || precision == 2) {
        time_size += 1;
      }else if(precision == 3 || precision == 4) {
        time_size += 2;
      }else if(precision == 5 || precision == 6) {
        time_size += 3;
      }
      buf_resize(buf, data_size, data_size + time_size);
      str_to_time(data, str_length, &ltime);
      longlong nr;
      nr = TIME_to_longlong_time_packed(ltime);
      my_time_packed_to_binary(nr, buf + data_size, precision);
      data_size += time_size;
      break;
    }
    case enum_field_types::MYSQL_TYPE_DATETIME : {
      size_t datetime_size = 5;
      MYSQL_TIME ltime;
      if(precision == 1 || precision == 2) {
        datetime_size += 1;
      }else if(precision == 3 || precision == 4) {
        datetime_size += 2;
      }else if(precision == 5 || precision == 6) {
        datetime_size += 3;
      }
      buf_resize(buf, data_size, data_size + datetime_size);
      str_to_datetime(data, str_length, &ltime);
      longlong nr;
      nr = TIME_to_longlong_datetime_packed(ltime);
      my_datetime_packed_to_binary(nr, buf + data_size, precision);
      data_size += datetime_size;
      break;
    }
    case enum_field_types::MYSQL_TYPE_TIMESTAMP : {
      size_t timestamp_size = 4;
      MYSQL_TIME ltime;
      my_timeval val;
      if(precision == 1 || precision == 2) {
        timestamp_size += 1;
      }else if(precision == 3 || precision == 4) {
        timestamp_size += 2;
      }else if(precision == 5 || precision == 6) {
        timestamp_size += 3;
      }
      buf_resize(buf, data_size, data_size + timestamp_size);
      str_to_datetime(data, str_length, &ltime);
      datetime_to_timeval(&ltime, &val);
      my_timestamp_to_binary(&val, buf, precision);
      data_size += timestamp_size;        
      break;
    }
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
    this->rows_before= rows;
  }
  

  void calculate_event_size() {
    size_t n = Get_N();
    uchar sbuf[sizeof(m_width) + 1];
    uchar *const sbuf_end = net_store_length(sbuf, (size_t)m_width);
    event_size += ROWS_HEADER_LEN_V2 ;
    event_size += data_size1;
    event_size += data_size2;
    event_size += (sbuf_end - sbuf);
     if(m_type == Log_event_type::WRITE_ROWS_EVENT) {
        event_size += n;
        event_size += (rows_after.size() + 7) / 8;
     }else if( m_type == Log_event_type::DELETE_ROWS_EVENT) {
        event_size += n;
        event_size += (rows_before.size() + 7) / 8;
     }else if(m_type == Log_event_type::UPDATE_ROWS_EVENT) {
        event_size += n;
        event_size += (rows_before.size() + 7) / 8;
        event_size += n;
        event_size += (rows_after.size() + 7) / 8;
     }
     return ;
  }

  size_t get_event_size() {
      return event_size;
  }

  template<typename T>
  void write_data_before(T *data, std::string &type, size_t length = 0, size_t str_length = 0,int precision = 0, int frac = 0){
    data_to_binary(m_rows_before_buf, data, data_size1, type, length, str_length, precision, frac);
  }

  template<typename T>
  void write_data_after(T *data, std::string &type, size_t length  = 0, size_t str_length = 0,int precision = 0, int frac = 0) {
    data_to_binary(m_rows_after_buf, data, data_size2, type, length, str_length, precision, frac);
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
  std::vector<bool> null_after;
  std::vector<bool> null_before;
  size_t data_size1 = 0;
  size_t data_size2 = 0;
  size_t  event_size = 0;


std::unordered_map<std::string, enum_field_types>  f_types = {
    {"INT", enum_field_types::MYSQL_TYPE_INT},
    {"MEDIUMINT", enum_field_types::MYSQL_TYPE_MEDIUMINT},
    {"BIGINT", enum_field_types::MYSQL_TYPE_BIGINT},
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
    {"TIMESTAMP", enum_field_types::MYSQL_TYPE_TIMESTAMP},
    {"TIME", enum_field_types::MYSQL_TYPE_TIME},
    {"DATETIME", enum_field_types::MYSQL_TYPE_DATETIME}
  };
};
