// COPY from 'mysql/libbinlogevents/include/event_reader.h'

/**
    反序列化 event，读出 buffer 里的字段
*/

#ifndef EVENT_READER_INCLUDED
#define EVENT_READER_INCLUDED

#include <list>
#include <map>
#include <string>
#include <vector>
#include "byteorder.h" // no used!
#include "wrapper_functions.h"
#include <cstring>

namespace binary_log {

// #define PRINT_READER_STATUS(message)               \
//   BAPI_PRINT("debug", (message ": m_buffer= %p, "  \
//                                "m_limit= %llu, "   \
//                                "m_length= %llu, "  \
//                                "position()= %llu", \
//                        m_buffer, m_limit, m_length, Event_reader::position()))


class Event_reader {
 public:

  Event_reader(const char *buffer, unsigned long long length)
      : m_buffer(buffer),
        m_ptr(buffer),
        m_length(length),
        m_limit(length),
        m_error(nullptr) {}

  bool has_error() {
    // BAPI_PRINT("debug", ("m_error= %s", m_error ? m_error : "nullptr"));
    return m_error != nullptr;
  }

  const char *get_error() { return m_error; }

  void set_error(const char *error);

  unsigned long long length() { return (m_length); }

  void set_length(unsigned long long length);

 
  void shrink_limit(unsigned long long bytes);

  const char *buffer() { return m_buffer; }


  const char *ptr() { return m_ptr; }

  const char *ptr(unsigned long long length);

  unsigned long long position() {
    return m_ptr >= m_buffer ? m_ptr - m_buffer : m_limit;
  }
  unsigned long long available_to_read() {
    // BAPI_ASSERT(position() <= m_limit);
    return m_limit - position();
  }

  bool can_read(unsigned long long bytes) {
    return (available_to_read() >= bytes);
  }

  const char *go_to(unsigned long long position);

  const char *forward(unsigned long long bytes) {
    // BAPI_PRINT("debug", ("Event_reader::forward(%llu)", bytes));
    return go_to((m_ptr - m_buffer) + bytes);
  }

  template <class T>
  T memcpy() {
    // PRINT_READER_STATUS("Event_reader::memcpy");
    if (!can_read(sizeof(T))) {
      set_error("Cannot read from out of buffer bounds");
      // BAPI_PRINT("debug", ("Event_reader::memcpy(): "
      //                      "sizeof()= %zu",
      //                      sizeof(T)));
      return 0;
    }
    T value = 0;
    ::memcpy((char *)&value, m_ptr, sizeof(T));
    m_ptr = m_ptr + sizeof(T);
    return value;
  }

  
  template <typename T>
  T read(unsigned char bytes = sizeof(T)) {
    // PRINT_READER_STATUS("Event_reader::read");
    if (!can_read(bytes)) {
      set_error("Cannot read from out of buffer bounds");
      // BAPI_PRINT("debug", ("Event_reader::read(): "
      //                      "sizeof()= %zu, bytes= %u",
      //                      sizeof(T), bytes));
      return 0;
    }
    T value = 0;
    ::memcpy((char *)&value, m_ptr, bytes);
    m_ptr = m_ptr + bytes;
    return (bytes > 1) ? letoh(value) : value;
  }

  template <typename T>
  T strndup(size_t length) {
    // PRINT_READER_STATUS("Event_reader::strndup");
    if (!can_read(length)) {
      // BAPI_PRINT("debug", ("Event_reader::strndup(%zu)", length));
      set_error("Cannot read from out of buffer bounds");
      return nullptr;
    }
    T str;
    // str = reinterpret_cast<T>(bapi_strndup(m_ptr, length));
    m_ptr = m_ptr + length;
    return str;
  }

  template <typename T>
  void memcpy(T destination, size_t length) {
    // PRINT_READER_STATUS("Event_reader::memcpy");
    if (!can_read(length)) {
      // BAPI_PRINT("debug", ("Event_reader::memcpy(%zu)", length));
      set_error("Cannot read from out of buffer bounds");
      return;
    }
    ::memcpy(destination, m_ptr, length);
    m_ptr = m_ptr + length;
  }

  void alloc_and_memcpy(unsigned char **destination, size_t length, int flags);

  void alloc_and_strncpy(char **destination, size_t length, int flags);

  void read_str_at_most_255_bytes(const char **destination, uint8_t *length);

  uint64_t net_field_length_ll();

  
  void read_data_set(uint32_t set_len, std::list<const char *> *set);

  void read_data_map(uint32_t map_len, std::map<std::string, std::string> *map);

  void strncpyz(char *destination, size_t max_length, size_t dest_length);

  void assign(std::vector<uint8_t> *destination, size_t length);

 private:
  /* The buffer with the serialized binary log event */
  const char *m_buffer;
  /* The cursor: a pointer to the current read position in the buffer */
  const char *m_ptr;
  /* The length of the buffer */
  unsigned long long m_length;
  /* The limit the reader shall respect when reading from the buffer */
  unsigned long long m_limit;
  /* The pointer to the current error message, or nullptr */
  const char *m_error;

  uint16_t letoh(uint16_t value) { return le16toh(value); }

  /**
    Wrapper to le32toh to be used by read function.

    @param[in] value the value to be converted.

    @return the converted value.
  */
  int32_t letoh(int32_t value) { return le32toh(value); }

  uint32_t letoh(uint32_t value) { return le32toh(value); }

  int64_t letoh(int64_t value) { return le64toh(value); }

  
  uint64_t letoh(uint64_t value) { return le64toh(value); }
};
}  // end namespace binary_log
/**
  @} (end of group Replication)
*/
#endif /* EVENT_READER_INCLUDED */
