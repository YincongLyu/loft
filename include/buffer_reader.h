//
// Created by Coonger on 2024/11/14.
//

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <type_traits>

class BufferReader {
public:
  BufferReader(const char *buffer, unsigned long long length) noexcept;
  ~BufferReader() = default;

  /**
   * @brief 一次性读取 sizeof(T) 个 char byte，并将指针向前移动，读取已做小端处理
   */
  template <typename T>
  T read(unsigned char bytes = sizeof(T));

  template <typename T>
  void memcpy(T destination, size_t length);

  /**
   * @brief ptr 向前移动 length 个 byte
   * @param length
   */
  void forward(size_t length);

  unsigned long long position() const noexcept;
  bool valid() const noexcept;

private:
  /**
   * @brief 小端解释读出 value
   */
  template <typename T>
  static T letoh(T value);

private:
  const char *buffer_;
  const char *ptr_;
  unsigned long long limit_;


};

template <typename T>
T BufferReader::read(unsigned char bytes) {
  if (ptr_ + bytes > buffer_ + limit_) {
    throw std::out_of_range("Attempt to read beyond buffer limit");
  }
  T value = 0;
  std::memcpy(reinterpret_cast<char *>(&value), ptr_, bytes);
  ptr_ += bytes;
  return (bytes > 1) ? letoh(value) : value;
}

template <typename T>
void BufferReader::memcpy(T destination, size_t length) {
  if (ptr_ + length > buffer_ + limit_) {
    throw std::out_of_range("Attempt to copy beyond buffer limit");
  }
  std::memcpy(destination, ptr_, length);
  ptr_ += length;
}


template <typename T>
T BufferReader::letoh(T value) {
  if constexpr (std::is_same_v<T, uint16_t>) {
    return le16toh(value);
  } else if constexpr (std::is_same_v<T, int32_t> || std::is_same_v<T, uint32_t>) {
    return le32toh(value);
  } else if constexpr (std::is_same_v<T, int64_t> || std::is_same_v<T, uint64_t>) {
    return le64toh(value);
  } else {
    throw std::invalid_argument("Unsupported type for letoh");
  }
}


