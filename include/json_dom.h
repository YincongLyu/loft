#ifndef LOFT_JSON_DOM_H
#define LOFT_JSON_DOM_H


#include "little_endian.h"
#include "rapidjson/memorystream.h"
#include "rapidjson/reader.h"
#include <memory>
#include <string>
#include <map>
#include <vector>
#include "demical.h"
#include "my_time.h"

class Json_dom;
class Json_container;

using Json_dom_ptr = std::unique_ptr<Json_dom>;

enum class enum_json_type {
  J_NULL,
  J_DECIMAL,
  J_INT,
  J_UINT,
  J_DOUBLE,
  J_STRING,
  J_OBJECT,
  J_ARRAY,
  J_BOOLEAN,
  J_DATE,
  J_TIME,
  J_DATETIME,
  J_TIMESTAMP,
  J_OPAQUE,
  J_ERROR
};

template <typename T, typename... Args>
inline std::unique_ptr<T> create_dom_ptr(Args &&... args) {
  return std::unique_ptr<T>(new (std::nothrow) T(std::forward<Args>(args)...));
}

/*
    Json_dom
      Json_container
        Json_obejct
        Json_array
      Json_scalar
        Json_string
        Json_number
          Json_decimal
          Json_double
          Json_int
          Json_uint
        Json_null
        Json_datetime
        Json_boolean
*/
class Json_dom {
  friend class Json_object;
  friend class Json_array;
public:
  virtual ~Json_dom() = default;

  Json_container *parent() const { return m_parent; }
  virtual enum_json_type json_type() const = 0;
  virtual bool is_scalar() const { return false; }
  virtual bool is_number() const { return false; }
  virtual uint32 depth() const = 0;
static Json_dom_ptr parse(const char *text, size_t length);

private:
void set_parent(Json_container *parent) { m_parent = parent; }

Json_container *m_parent{nullptr};
};

class Json_container : public Json_dom
{
};

struct Json_string_compare {
    bool operator()(const std::string& lhs, const std::string& rhs) const {
        // 如果长度相同，则按照字典顺序排序
        if (lhs.length() == rhs.length()) {
            return lhs < rhs;
        }
        // 否则按照长度排序 
        return lhs.length() < rhs.length();
    }
};

class Json_object final : public Json_container {
private :
  std::map<std::string, Json_dom_ptr,Json_string_compare> m_map;

public :
  Json_object() {  }
  enum_json_type json_type() const override { return enum_json_type::J_OBJECT; }
  
  /**
    add value to the object 
    @param[in] key    the Json key to be added
    @param[in] ptr    the Json value to be added 
    @retval           false on success
    @retval           true on false
  */
  bool add_value(const std::string &key, Json_dom *value) {
    return add_value(key,Json_dom_ptr(value));
  }
  
  /**
    add value to the object 
    @param[in] key    the Json key to be added
    @param[in] ptr    the Json value to be added 
    @retval           false on success
    @retval           true on false
  */
  bool add_value(const std::string &key, Json_dom_ptr value);
  
  /**
    get Json value 
    @param[in] key    the Json key
    @retval           the Json value
  */
  Json_dom *get(const std::string &key) const;

  /**
   * @retval  The number of the elements in this Json object
   */
  size_t size() const;

  uint32 depth() const override;

  typedef std::map<std::string, Json_dom_ptr>::const_iterator  const_iterator;

  //return const_iterator of the first element 
  const_iterator begin() const { return m_map.begin(); }

  //return the const_iterator  past the last element.
  const_iterator end() const { return m_map.end(); }
};

class Json_array final : public Json_container {
private :
  std::vector<Json_dom_ptr> m_v;

public :
  Json_array() { }
  enum_json_type json_type() const override {  return enum_json_type::J_ARRAY;  }
 
/**
 * Insert the value at position index of the array.
 * @param[in] index the index of array insert to
 * @param[in] value the value of array insert to
 * @retval  false on success
 * @retval true on failure
 */
  bool insert_value(size_t index, Json_dom_ptr value);

/**
 * append the value to the array
 * @retval  false on success
 * @retval true on failure
 */
  bool append_value(Json_dom *value) {
    return append_value(Json_dom_ptr(value));
  }

/**
 * append the value to the array
 * @retval  false on success
 * @retval true on failure
 */
  bool append_value(Json_dom_ptr value) {
    return insert_value(size(), std::move(value));
  }


/**
 * get the size of the Obejct_array
 */
  size_t size() const { return m_v.size(); }

  uint32 depth() const override;

  Json_dom *operator[](size_t index) const {
    assert(m_v[index]->parent() == this);
    return m_v[index].get();
  }

  using const_iterator = decltype(m_v)::const_iterator;

  /// Returns a const_iterator that refers to the first element.
  const_iterator begin() const { return m_v.begin(); }

  /// Returns a const_iterator that refers past the last element.
  const_iterator end() const { return m_v.end(); }

};

class Json_scalar : public Json_dom {
 public:
  uint32 depth() const final { return 1; }

  bool is_scalar() const final { return true; }
};

class Json_opaque final : public Json_scalar {
 private:
  enum_field_types m_mytype;
  std::string m_val;

 public:
  /**
    An opaque MySQL value.

    @param[in] mytype  the MySQL type of the value
    @param[in] args    arguments to construct the binary value to be stored
                       in the DOM (anything accepted by the std::string
                       constructors)
    @see #enum_field_types
    @see Class documentation
  */
  template <typename... Args>
  explicit Json_opaque(enum_field_types mytype, Args &&... args)
      : Json_scalar(), m_mytype(mytype), m_val(std::forward<Args>(args)...) {}

  enum_json_type json_type() const override { return enum_json_type::J_OPAQUE; }

  /**
    @return a pointer to the opaque value. Use #size() to get its size.
  */
  const char *value() const { return m_val.data(); }

  /**
    @return the MySQL type of the value
  */
  enum_field_types type() const { return m_mytype; }
  /**
    @return the size in bytes of the value
  */
  size_t size() const { return m_val.size(); }

};


class Json_string final : public Json_scalar {
 private:
  std::string m_str;  //!< holds the string
 public:
  /** 
    Construct a Json_string object.
    @param args any arguments accepted by std::string's constructors
  */
  template <typename... Args>
  explicit Json_string(Args &&... args)
      : Json_scalar(), m_str(std::forward<Args>(args)...) {}

  enum_json_type json_type() const override { return enum_json_type::J_STRING; }

  /**
    Get the reference to the value of the JSON string.
    @return the string reference
  */

  const std::string &value() const { return m_str; }
  /**
    Get the number of characters in the string.
    @return the number of characters
  */
  size_t size() const { return m_str.size(); }
};

class Json_number : public Json_scalar {
 public:
  bool is_number() const final { return true; }
};

class Json_decimal final : public Json_number {
 private:
  decimal_t m_dec;  //!< holds the decimal number

 public:
  static const int MAX_BINARY_SIZE = DECIMAL_MAX_FIELD_SIZE + 2;

  explicit Json_decimal(const decimal_t &value)
        : Json_number(), m_dec(value) {}

  /**
    Get the number of bytes needed to store this decimal in a Json_opaque.
    @return the number of bytes.
  */
  int binary_size() const;

  /**
    Get the binary representation of the wrapped my_decimal, so that this
    value can be stored inside of a Json_opaque.

    @param dest the destination buffer to which the binary representation
                is written
    @return false on success, true on error
  */
  bool get_binary(char *dest) const;

  enum_json_type json_type() const override {
    return enum_json_type::J_DECIMAL;
  }

  /**
    Get a pointer to the MySQL decimal held by this object. Ownership
    is _not_ transferred.
    @return the decimal
  */
  const decimal_t *value() const { return &m_dec; }

  /**
    Returns stored DECIMAL binary

    @param  bin   serialized Json_decimal object

    @returns
      pointer to the binary decimal value

    @see #convert_from_binary
  */
  static const char *get_encoded_binary(const char *bin) {
    // Skip stored precision and scale
    return bin + 2;
  }

  /**
    Returns length of stored DECIMAL binary

    @param  length  length of serialized Json_decimal object

    @returns
      length of the binary decimal value

    @see #convert_from_binary
  */
  static size_t get_encoded_binary_len(size_t length) {
    // Skip stored precision and scale
    return length - 2;
  }
};

class Json_double final : public Json_number {
 private:
  double m_f;  //!< holds the double value
 public:
  explicit Json_double(double value) : Json_number(), m_f(value) {}

  enum_json_type json_type() const override { return enum_json_type::J_DOUBLE; }

  /**
    Return the double value held by this object.
    @return the value
  */
  double value() const { return m_f; }
};

class Json_int final : public Json_number {
 private:
  longlong m_i;  //!< holds the value
 public:
  explicit Json_int(longlong value) : Json_number(), m_i(value) {}

  enum_json_type json_type() const override { return enum_json_type::J_INT; }

  /**
    Return the signed int held by this object.
    @return the value
  */
  longlong value() const { return m_i; }

  /**
    @return true if the number can be held by a 16 bit signed integer
  */
  bool is_16bit() const { return INT_MIN16 <= m_i && m_i <= INT_MAX16; }

  /**
    @return true if the number can be held by a 32 bit signed integer
  */
  bool is_32bit() const { return INT_MIN32 <= m_i && m_i <= INT_MAX32; }

};

class Json_uint final : public Json_number {
 private:
  ulonglong m_i;  //!< holds the value
 public:
  explicit Json_uint(ulonglong value) : Json_number(), m_i(value) {}

  enum_json_type json_type() const override { return enum_json_type::J_UINT; }

  /**
    Return the unsigned int held by this object.
    @return the value
  */
  ulonglong value() const { return m_i; }

  /**
    @return true if the number can be held by a 16 bit unsigned
    integer.
  */
  bool is_16bit() const { return m_i <= UINT_MAX16; }

  /**
    @return true if the number can be held by a 32 bit unsigned
    integer.
  */
  bool is_32bit() const { return m_i <= UINT_MAX32; }

};

class Json_null final : public Json_scalar {
 public:
  enum_json_type json_type() const override { return enum_json_type::J_NULL; }
};

class Json_datetime final : public Json_scalar {
 private:
  MYSQL_TIME m_t;                 //!< holds the date/time value
  enum_field_types m_field_type;  //!< identifies which type of date/time

 public:
  /**
    Constructs a object to hold a MySQL date/time value.

    @param[in] t   the time/value
    @param[in] ft  the field type: must be one of MYSQL_TYPE_TIME,
                   MYSQL_TYPE_DATE, MYSQL_TYPE_DATETIME or
                   MYSQL_TYPE_TIMESTAMP.
  */
  Json_datetime(const MYSQL_TIME &t, enum_field_types ft)
      : Json_scalar(), m_t(t), m_field_type(ft) {}

  enum_json_type json_type() const override ;

  /**
    Return a pointer the date/time value. Ownership is _not_ transferred.
    To identify which time time the value represents, use @c field_type.
    @return the pointer
  */
  const MYSQL_TIME *value() const { return &m_t; }

  /**
    Return what kind of date/time value this object holds.
    @return One of MYSQL_TYPE_TIME, MYSQL_TYPE_DATE, MYSQL_TYPE_DATETIME
            or MYSQL_TYPE_TIMESTAMP.
  */
  enum_field_types field_type() const { return m_field_type; }

  /**
    Convert the datetime to the packed format used when storing
    datetime values.
    @param dest the destination buffer to write the packed datetime to
    (must at least have size PACKED_SIZE)
  */
  void to_packed(char *dest) const;

  /** Datetimes are packed in eight bytes. */
  static const size_t PACKED_SIZE = 8;
};

class Json_boolean final : public Json_scalar {
 private:
  bool m_v;  //!< false or true: represents the eponymous JSON literal
 public:
  explicit Json_boolean(bool value) : Json_scalar(), m_v(value) {}

  enum_json_type json_type() const override {
    return enum_json_type::J_BOOLEAN;
  }

  /**
    @return false for JSON false, true for JSON true
  */
  bool value() const { return m_v; }
 
};

#endif