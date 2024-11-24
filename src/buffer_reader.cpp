//
// Created by Coonger on 2024/11/14.
//

#include "buffer_reader.h"
#include <stdexcept>

BufferReader::BufferReader(const char *buffer, unsigned long long length) noexcept
    : buffer_(buffer), ptr_(buffer), limit_(length) {}

void BufferReader::forward(size_t length) {
  if (ptr_ + length > buffer_ + limit_) {
    throw std::out_of_range("Attempt to forward beyond buffer limit");
  }
  ptr_ += length;
}

unsigned long long BufferReader::position() const noexcept {
  return ptr_ >= buffer_ ? ptr_ - buffer_ : limit_;
}

bool BufferReader::valid() const noexcept {
  return ptr_ < buffer_ + limit_;
}
