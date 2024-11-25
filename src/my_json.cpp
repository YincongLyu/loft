#include "my_json.h"
#include <iostream>

constexpr char JSONB_TYPE_SMALL_OBJECT = 0x0;
constexpr char JSONB_TYPE_LARGE_OBJECT = 0x1;
constexpr char JSONB_TYPE_SMALL_ARRAY = 0x2;
constexpr char JSONB_TYPE_LARGE_ARRAY = 0x3;
constexpr char JSONB_TYPE_LITERAL = 0x4;
constexpr char JSONB_TYPE_INT16 = 0x5;
constexpr char JSONB_TYPE_UINT16 = 0x6;
constexpr char JSONB_TYPE_INT32 = 0x7;
constexpr char JSONB_TYPE_UINT32 = 0x8;
constexpr char JSONB_TYPE_INT64 = 0x9;
constexpr char JSONB_TYPE_UINT64 = 0xA;
constexpr char JSONB_TYPE_DOUBLE = 0xB;
constexpr char JSONB_TYPE_STRING = 0xC;
constexpr char JSONB_TYPE_OPAQUE = 0xF;


constexpr char JSONB_NULL_LITERAL = 0x0;
constexpr char JSONB_TRUE_LITERAL = 0x1;
constexpr char JSONB_FALSE_LITERAL = 0x2;

/*
  The size of offset or size fields in the small and the large storage
  format for JSON objects and JSON arrays.
*/
constexpr uint8 SMALL_OFFSET_SIZE = 2;
constexpr uint8 LARGE_OFFSET_SIZE = 4;

/*
  The size of key entries for objects when using the small storage
  format or the large storage format. In the small format it is 4
  bytes (2 bytes for key length and 2 bytes for key offset). In the
  large format it is 6 (2 bytes for length, 4 bytes for offset).
*/
constexpr uint8 KEY_ENTRY_SIZE_SMALL = 2 + SMALL_OFFSET_SIZE;
constexpr uint8 KEY_ENTRY_SIZE_LARGE = 2 + LARGE_OFFSET_SIZE;

/*
  The size of value entries for objects or arrays. When using the
  small storage format, the entry size is 3 (1 byte for type, 2 bytes
  for offset). When using the large storage format, it is 5 (1 byte
  for type, 4 bytes for offset).
*/
constexpr uint8 VALUE_ENTRY_SIZE_SMALL = 1 + SMALL_OFFSET_SIZE;
constexpr uint8 VALUE_ENTRY_SIZE_LARGE = 1 + LARGE_OFFSET_SIZE;

enum enum_serialization_result {
  /**
    Success. The JSON value was successfully serialized.
  */
  OK,
  /**
    The JSON value was too big to be serialized. If this status code
    is returned, and the small storage format is in use, the caller
    should retry the serialization with the large storage format. If
    this status code is returned, and the large format is in use,
    my_error() will already have been called.
  */
  VALUE_TOO_BIG,
  /**
    Some other error occurred. my_error() will have been called with
    more specific information about the failure.
  */
  FAILURE
};

static enum_serialization_result serialize_json_value(
    const Json_dom *dom, size_t type_pos, std::string *dest,
    size_t depth, bool small_parent);

static uint8 key_entry_size(bool large) {
  return large ? KEY_ENTRY_SIZE_LARGE : KEY_ENTRY_SIZE_SMALL;
}

/**
  Get the size of a value entry.
  @param large true if the large storage format is used
  @return the size of a value entry
*/
static uint8 value_entry_size(bool large) {
  return large ? VALUE_ENTRY_SIZE_LARGE : VALUE_ENTRY_SIZE_SMALL;
}

static bool append_offset_or_size(std::string *dest, size_t offset_or_size,
                                  bool large) {
  if (large)
    return append_int32(dest, static_cast<int32>(offset_or_size));
  else
    return append_int16(dest, static_cast<int16>(offset_or_size));
}


static void write_offset_or_size(char *dest, size_t offset_or_size,
                                 bool large) {
  if (large)
    int4store(dest, static_cast<uint32>(offset_or_size));
  else
    int2store(dest, static_cast<uint16>(offset_or_size));
}
 

static bool append_variable_length(std::string *dest, size_t length) {
  do {
    // Filter out the seven least significant bits of length.
    uchar ch = (length & 0x7F);

    /*
      Right-shift length to drop the seven least significant bits. If there
      is more data in length, set the high bit of the byte we're writing
      to the String.
    */
    length >>= 7;
    if (length != 0) ch |= 0x80;

    dest->append(1,ch);
  } while (length != 0);



  // Successfully appended the length.
  return false;
}

static enum_serialization_result append_key_entries(const Json_object *object,
                                                    std::string *dest, size_t offset,
                                                    bool large) {
  const std::string *prev_key = nullptr;

  // Add the key entries.
  for (Json_object::const_iterator it = object->begin(); it != object->end();
       ++it) {
    const std::string *key = &it->first;
    size_t len = key->length();

    // Check that the DOM returns the keys in the correct order.
    if (prev_key) {
      assert(prev_key->length() <= len);
      if (len == prev_key->length())
        assert(memcmp(prev_key->data(), key->data(), len) < 0);
    }
    prev_key = key;

    // We only have two bytes for the key size. Check if the key is too big.
    if (len > UINT_MAX16) {
      return FAILURE;
    }
    if (append_offset_or_size(dest, offset, large) ||
        append_int16(dest, static_cast<int16>(len)))
      return FAILURE; /* purecov: inspected */
    offset += len;
  }

  return OK;
}

static void insert_offset_or_size(std::string *dest, size_t pos,
                                  size_t offset_or_size, bool large) {
  char *ptr = &dest->at(0);
  write_offset_or_size(ptr + pos, offset_or_size, large);
}


static bool attempt_inline_value(const Json_dom *value, std::string *dest,
                                 size_t pos, bool large) {
  int32 inlined_val;
  char inlined_type;
  switch (value->json_type()) {
    case enum_json_type::J_NULL:
      inlined_val = JSONB_NULL_LITERAL;
      inlined_type = JSONB_TYPE_LITERAL;
      break;
    case enum_json_type::J_BOOLEAN:
      inlined_val = static_cast<const Json_boolean *>(value)->value()
                        ? JSONB_TRUE_LITERAL
                        : JSONB_FALSE_LITERAL;
      inlined_type = JSONB_TYPE_LITERAL;
      break;
    case enum_json_type::J_INT: {
      const Json_int *i = static_cast<const Json_int *>(value);
      if (!i->is_16bit() && !(large && i->is_32bit()))
        return false;  // cannot inline this value
      inlined_val = static_cast<int32>(i->value());
      inlined_type = i->is_16bit() ? JSONB_TYPE_INT16 : JSONB_TYPE_INT32;
      break;
    }
    case enum_json_type::J_UINT: {
      const Json_uint *i = static_cast<const Json_uint *>(value);
      if (!i->is_16bit() && !(large && i->is_32bit()))
        return false;  // cannot inline this value
      inlined_val = static_cast<int32>(i->value());
      inlined_type = i->is_16bit() ? JSONB_TYPE_UINT16 : JSONB_TYPE_UINT32;
      break;
    }
    default:
      return false;  // cannot inline value of this type
  }

  (*dest)[pos] = inlined_type;
  insert_offset_or_size(dest, pos + 1, inlined_val, large);
  return true;
}

static enum_serialization_result serialize_opaque(const Json_opaque *opaque,
                                                  size_t type_pos,
                                                  std::string *dest) {
  assert(type_pos < dest->length());
  dest->append(1,static_cast<char>(opaque->type()));
  append_variable_length(dest, opaque->size());
  dest->append(opaque->value(), opaque->size());
    return FAILURE; /* purecov: inspected */
  (*dest)[type_pos] = JSONB_TYPE_OPAQUE;
  return OK;
}

static enum_serialization_result serialize_json_array(const Json_array *array,
                                                      std::string *dest, bool large,
                                                      size_t depth) {

  const size_t start_pos = dest->length();
  const size_t size = array->size();

  if (++depth > 100) {
    return FAILURE;
  }

  // First write the number of elements in the array.
  if (append_offset_or_size(dest, size, large))
    return FAILURE; /* purecov: inspected */

  // Reserve space for the size of the array in bytes. To be filled in later.
  const size_t size_pos = dest->length();
  if (append_offset_or_size(dest, 0, large))
    return FAILURE; /* purecov: inspected */

  size_t entry_pos = dest->length();

  // Reserve space for the value entries at the beginning of the array.
  const auto entry_size = value_entry_size(large);
     
  dest->reserve(dest->size() + size * entry_size);
  dest->resize(dest->size() + size * entry_size);

  for (const auto &child : *array) {
    const Json_dom *elt = child.get();
    if (!attempt_inline_value(elt, dest, entry_pos, large)) {
      size_t offset = dest->length() - start_pos;
      insert_offset_or_size(dest, entry_pos + 1, offset, large);
      auto res = serialize_json_value(elt, entry_pos, dest, depth, !large);
      if (res != OK) return res;
    }
    entry_pos += entry_size;
  }

  // Finally, write the size of the object in bytes.
  size_t bytes = dest->length() - start_pos;
  insert_offset_or_size(dest, size_pos, bytes, large);

  return OK;
}

static enum_serialization_result serialize_decimal(const Json_decimal *jd,
                                                   size_t type_pos,
                                                   std::string *dest) {
  // Store DECIMALs as opaque values.
  const int bin_size = jd->binary_size();
  char buf[Json_decimal::MAX_BINARY_SIZE];
  if (jd->get_binary(buf)) return FAILURE; /* purecov: inspected */
  Json_opaque o(MYSQL_TYPE_NEWDECIMAL, buf, bin_size);
  return serialize_opaque(&o, type_pos, dest);
}

/**
  Serialize a DATETIME value at the end of the destination string.
  @param[in]  jdt       the DATETIME value
  @param[in]  type_pos  where to write the type specifier
  @param[out] dest      the destination string
  @return serialization status
*/
static enum_serialization_result serialize_datetime(const Json_datetime *jdt,
                                                    size_t type_pos,
                                                    std::string *dest) {
  // Store datetime as opaque values.
  char buf[Json_datetime::PACKED_SIZE];
  jdt->to_packed(buf);
  Json_opaque o(jdt->field_type(), buf, sizeof(buf));
  return serialize_opaque(&o, type_pos, dest);
}

static enum_serialization_result serialize_json_object(
    const Json_object *object, std::string *dest, bool large,
    size_t depth) {

  const size_t start_pos = dest->length();
  const size_t size = object->size();

  ++depth;
  // First write the number of members in the object.
  if (append_offset_or_size(dest, size, large))
    return FAILURE; /* purecov: inspected */

  // Reserve space for the size of the object in bytes. To be filled in later.
  const size_t size_pos = dest->length();
  if (append_offset_or_size(dest, 0, large))
    return FAILURE; /* purecov: inspected */

  const auto key_size = key_entry_size(large);
  const auto value_size = value_entry_size(large);

  /*
    Calculate the offset of the first key relative to the start of the
    object. The first key comes right after the value entries.
  */
  const size_t first_key_offset =
      dest->length() + size * (key_size + value_size) - start_pos;

  // Append all the key entries.
  enum_serialization_result res =
      append_key_entries(object, dest, first_key_offset, large);
  if (res != OK) return res;

  const size_t start_of_value_entries = dest->length();

  dest->resize(dest->size() + size * value_size);

  // Add the actual keys.
  for (const auto &member : *object) {       
    dest->append(member.first.c_str(), member.first.length());
  }

  // Add the values, and update the value entries accordingly.
  size_t entry_pos = start_of_value_entries;
  for (const auto &member : *object) {
    const Json_dom *child = member.second.get();
    if (!attempt_inline_value(child, dest, entry_pos, large)) {
      size_t offset = dest->length() - start_pos;
      insert_offset_or_size(dest, entry_pos + 1, offset, large);
      res = serialize_json_value(child, entry_pos, dest, depth, !large);
      if (res != OK) return res;
    }
    entry_pos += value_size;
  }

  // Finally, write the size of the object in bytes.
  size_t bytes = dest->length() - start_pos;

  insert_offset_or_size(dest, size_pos, bytes, large);

  return OK;
}

static enum_serialization_result serialize_json_value(
    const Json_dom *dom, size_t type_pos, std::string *dest,
    size_t depth, bool small_parent) {
  const size_t start_pos = dest->length();
  assert(type_pos < start_pos);

  enum_serialization_result result;
  switch (dom->json_type()) {
    case enum_json_type::J_ARRAY: {
      const Json_array *array = static_cast<const Json_array *>(dom);
      (*dest)[type_pos] = JSONB_TYPE_SMALL_ARRAY;
      result = serialize_json_array(array, dest, false, depth);
      break;
    }
    case enum_json_type::J_OBJECT: {
      const Json_object *object = static_cast<const Json_object *>(dom);
      (*dest)[type_pos] = JSONB_TYPE_SMALL_OBJECT;
      result = serialize_json_object(object, dest, false, depth);
      break;
    }
    case enum_json_type::J_STRING: {
      const Json_string *jstr = static_cast<const Json_string *>(dom);
      size_t size = jstr->size();
      if (append_variable_length(dest, size))
        return FAILURE; /* purecov: inspected */
      int start_pos = dest->size();
      dest->append(jstr->value().c_str(), size);
      // dest->resize(dest->size() + size);
      // char *start = &dest->at(0) + start_pos;
      // memcpy(start,jstr->value().c_str(),size);
      (*dest)[type_pos] = JSONB_TYPE_STRING;
      result = OK;
      break;
    }
    case enum_json_type::J_INT: {
      const Json_int *i = static_cast<const Json_int *>(dom);
      longlong val = i->value();
      if (i->is_16bit()) {
        if (append_int16(dest, static_cast<int16>(val)))
          return FAILURE; /* purecov: inspected */
        (*dest)[type_pos] = JSONB_TYPE_INT16;
      } else if (i->is_32bit()) {
        if (append_int32(dest, static_cast<int32>(val)))
          return FAILURE; /* purecov: inspected */
        (*dest)[type_pos] = JSONB_TYPE_INT32;
      } else {
        if (append_int64(dest, val)) return FAILURE; /* purecov: inspected */
        (*dest)[type_pos] = JSONB_TYPE_INT64;
      }
      result = OK;
      break;
    }
    case enum_json_type::J_UINT: {
      const Json_uint *i = static_cast<const Json_uint *>(dom);
      ulonglong val = i->value();
      if (i->is_16bit()) {
        if (append_int16(dest, static_cast<int16>(val)))
          return FAILURE; /* purecov: inspected */
        (*dest)[type_pos] = JSONB_TYPE_UINT16;
      } else if (i->is_32bit()) {
        if (append_int32(dest, static_cast<int32>(val)))
          return FAILURE; /* purecov: inspected */
        (*dest)[type_pos] = JSONB_TYPE_UINT32;
      } else {
        if (append_int64(dest, val)) return FAILURE; /* purecov: inspected */
        (*dest)[type_pos] = JSONB_TYPE_UINT64;
      }
      result = OK;
      break;
    }
    case enum_json_type::J_DOUBLE: {
      // Store the double in a platform-independent eight-byte format.
      const Json_double *d = static_cast<const Json_double *>(dom);
      int start_pos = dest->length();
      dest->resize(8 + dest->size());
      char* ptr = &dest->at(0);
      float8store(ptr + start_pos, d->value());
      (*dest)[type_pos] = JSONB_TYPE_DOUBLE;
      result = OK;
      break;
    }
    case enum_json_type::J_NULL:
      dest->append(1,JSONB_NULL_LITERAL);
      (*dest)[type_pos] = JSONB_TYPE_LITERAL;
      result = OK;
      break;
    case enum_json_type::J_BOOLEAN: {
      char c = (static_cast<const Json_boolean *>(dom)->value())
                   ? JSONB_TRUE_LITERAL    
                   : JSONB_FALSE_LITERAL;
      dest->append(1 , c);
      (*dest)[type_pos] = JSONB_TYPE_LITERAL;
      result = OK;
      break;
    }
    case enum_json_type::J_DECIMAL:
      result = serialize_decimal(static_cast<const Json_decimal *>(dom), type_pos,
                                 dest);
      break;
    case enum_json_type::J_DATETIME:
    case enum_json_type::J_DATE:
    case enum_json_type::J_TIME:
    case enum_json_type::J_TIMESTAMP:
      result = serialize_datetime(static_cast<const Json_datetime *>(dom),
                                  type_pos, dest);
      break;
    default:
      /* purecov: begin deadcode */
      assert(false);
      return FAILURE;
      /* purecov: end */
  }

  return result;
}

void json2bin(const Json_dom *dom, std::string *dest) {
  dest->append(1 , '\0');
  serialize_json_value(dom,0, dest, 0, false);
}

void store_json_binary(const char *data,size_t length, uchar *dest) { 
    memcpy(dest, &length, 4);
    memcpy(dest + 4, data, length);
}