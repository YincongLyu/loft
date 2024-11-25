#include "json_dom.h"
#include <cmath>

class Rapid_json_handler {
 private:
// std::cerr << "callback " << name << ':' << state << '\n'; std::cerr.flush()
#define DUMP_CALLBACK(name, state)

  enum enum_state {
    expect_anything,
    expect_array_value,
    expect_object_key,
    expect_object_value,
    expect_eof
  };

  enum_state m_state;           ///< Tells what kind of value to expect next.
  Json_dom_ptr m_dom_as_built;  ///< Root of the DOM being built.
  Json_dom *m_current_element;  ///< The current object/array being parsed.
  size_t m_depth;     ///< The depth at which parsing currently happens.
  std::string m_key;  ///< The name of the current member of an object.
 public:
  explicit Rapid_json_handler()
      : m_state(expect_anything),
        m_dom_as_built(nullptr),
        m_current_element(nullptr),
        m_depth(0),
        m_key() {}

  /**
    @returns The built JSON DOM object.
    Deallocation of the returned value is the responsibility of the caller.
  */
  Json_dom_ptr get_built_doc() { return std::move(m_dom_as_built); }

 private:
  /**
    Function which is called on each value found in the JSON
    document being parsed.

    @param[in] value the value that was seen
    @return true if parsing should continue, false if an error was
            found and parsing should stop
  */
  bool seeing_value(Json_dom_ptr value) {
    if (value == nullptr) return false; /* purecov: inspected */
    switch (m_state) {
      case expect_anything:
        m_dom_as_built = std::move(value);
        m_state = expect_eof;
        return true;
      case expect_array_value: {
        auto array = static_cast<Json_array *>(m_current_element);
        if (array->append_value(std::move(value)))
          return false; /* purecov: inspected */
        return true;
      }
      case expect_object_value: {
        m_state = expect_object_key;
        auto object = static_cast<Json_object *>(m_current_element);
        return !object->add_value(m_key, std::move(value));
      }
      default:
        /* purecov: begin inspected */
        assert(false);
        return false;
        /* purecov: end */
    }
  }

 public:
  bool Null() {
    DUMP_CALLBACK("null", state);
    return seeing_value(create_dom_ptr<Json_null>());
  }

  bool Bool(bool b) {
    DUMP_CALLBACK("bool", state);
    return seeing_value(create_dom_ptr<Json_boolean>(b));
  }

  bool Int(int i) {
    DUMP_CALLBACK("int", state);
    return seeing_value(create_dom_ptr<Json_int>(i));
  }

  bool Uint(unsigned u) {
    DUMP_CALLBACK("uint", state);
    return seeing_value(create_dom_ptr<Json_int>(static_cast<longlong>(u)));
  }

  bool Int64(int64_t i) {
    DUMP_CALLBACK("int64", state);
    return seeing_value(create_dom_ptr<Json_int>(i));
  }

  bool Uint64(uint64_t ui64) {
    DUMP_CALLBACK("uint64", state);
    return seeing_value(create_dom_ptr<Json_uint>(ui64));
  }

  bool Double(double d) {
    DUMP_CALLBACK("double", state);
    /*
      We only accept finite values. RapidJSON normally stops non-finite values
      from getting here, but sometimes +/-inf values could end up here anyway.
    */
    if (!std::isfinite(d)) return false;
    return seeing_value(create_dom_ptr<Json_double>(d));
  }

  /* purecov: begin deadcode */
  bool RawNumber(const char *, rapidjson::SizeType, bool) {
    /*
      Never called, since we don't instantiate the parser with
      kParseNumbersAsStringsFlag.
    */
    assert(false);
    return false;
  }
  /* purecov: end */

  bool String(const char *str, rapidjson::SizeType length, bool) {
    DUMP_CALLBACK("string", state);
    return seeing_value(create_dom_ptr<Json_string>(str, length));
  }

  bool StartObject() {
    DUMP_CALLBACK("start object {", state);
    return start_object_or_array(create_dom_ptr<Json_object>(),
                                 expect_object_key);
  }

  bool EndObject(rapidjson::SizeType) {
    DUMP_CALLBACK("} end object", state);
    assert(m_state == expect_object_key);
    end_object_or_array();
    return true;
  }

  bool StartArray() {
    DUMP_CALLBACK("start array [", state);
    return start_object_or_array(create_dom_ptr<Json_array>(),
                                 expect_array_value);
  }

  bool EndArray(rapidjson::SizeType) {
    DUMP_CALLBACK("] end array", state);
    assert(m_state == expect_array_value);
    end_object_or_array();
    return true;
  }

  bool Key(const char *str, rapidjson::SizeType len, bool) {
    assert(m_state == expect_object_key);
    m_state = expect_object_value;
    m_key.assign(str, len);
    return true;
  }

 private:
  bool start_object_or_array(Json_dom_ptr value, enum_state next_state) {
    Json_dom *dom = value.get();
    bool success = seeing_value(std::move(value));
    ++m_depth;
    m_current_element = dom;
    m_state = next_state;
    return success;
  }

  void end_object_or_array() {
    m_depth--;
    m_current_element = m_current_element->parent();
    if (m_current_element == nullptr) {
      assert(m_depth == 0);
      m_state = expect_eof;
    } else if (m_current_element->json_type() == enum_json_type::J_OBJECT)
      m_state = expect_object_key;
    else {
      assert(m_current_element->json_type() == enum_json_type::J_ARRAY);
      m_state = expect_array_value;
    }
  }
};

Json_dom_ptr Json_dom::parse(const char *text, size_t length) {
  Rapid_json_handler handler;
  rapidjson::Reader reader;
  rapidjson::MemoryStream ss(text, length);
  bool success = reader.Parse<rapidjson::kParseDefaultFlags>(ss, handler);
  if (success) return handler.get_built_doc();
  
  return nullptr;
}

bool Json_object::add_value(const std::string &key, Json_dom_ptr value) {
  if(!value) return true; 
  value->set_parent(this);
  m_map.emplace(key,nullptr).first->second = std::move(value);
  return false;
}

Json_dom* Json_object::get(const std::string &key) const {
  auto it = m_map.find(key);
  if(it == m_map.end()) return nullptr;

    return it->second.get();
}

size_t Json_object::size() const {  return m_map.size();  }

uint32 Json_object::depth() const {
  uint deepest_child = 0;

  for (auto iter = m_map.begin();iter != m_map.end(); ++iter) {
    deepest_child = std::max(deepest_child, iter->second->depth());
  }

  return 1 + deepest_child;
}

bool Json_array::insert_value(size_t index, Json_dom_ptr value) {
  if(!value) return true;
  value->set_parent(this);
  auto pos = m_v.begin() + std::min(m_v.size(), index);
  m_v.emplace(pos, std::move(value));
  return false;
}

uint32 Json_array::depth() const {
  uint deepest_child = 0;

  for (const auto &child : m_v) {
    deepest_child = std::max(deepest_child, child->depth());
  }
  return 1 + deepest_child;
}

int Json_decimal::binary_size()  const {
  return decimal_bin_size(m_dec.frac + m_dec.intg, m_dec.frac);
}

bool Json_decimal::get_binary(char *dest) const {
  dest[0] = static_cast<char>(m_dec.frac + m_dec.intg);
  dest[1] = static_cast<char>(m_dec.frac);
  // Then store the decimal value.
  return decimal2bin(&m_dec, (uchar *)dest + 2, m_dec.frac + m_dec.intg, m_dec.frac);
}

enum_json_type Json_datetime::json_type() const {
  switch (m_field_type) {
    case MYSQL_TYPE_TIME:
      return enum_json_type::J_TIME;
    case MYSQL_TYPE_DATETIME:
      return enum_json_type::J_DATETIME;
    case MYSQL_TYPE_DATE:
      return enum_json_type::J_DATE;
    case MYSQL_TYPE_TIMESTAMP:
      return enum_json_type::J_TIMESTAMP;
    default:;
 }
 return enum_json_type::J_ERROR;
}

void  Json_datetime::to_packed(char *dest) const{
  longlong packed = TIME_to_longlong_packed(m_t);
  int8store(static_cast<uchar *>(static_cast<void *>(dest)), packed);
}


