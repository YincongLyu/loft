//
// Created by Coonger on 2024/10/21.
//

#pragma once
#include <cassert>
#include <cstdint>

class Table_id
{
private:
  /* In table map event and rows events, table id is 6 bytes.*/
  static const unsigned long long TABLE_ID_MAX = (~0ULL >> 16);
  uint64_t                        m_id_;

public:
  Table_id() : m_id_(0) {}

  explicit Table_id(unsigned long long id) : m_id_(id) {}

  unsigned long long get_id() const { return m_id_; }

  bool is_valid() const { return m_id_ <= TABLE_ID_MAX; }

  Table_id &operator=(unsigned long long id)
  {
    m_id_ = id;
    return *this;
  }

  bool operator==(const Table_id &tid) const { return m_id_ == tid.m_id_; }

  bool operator!=(const Table_id &tid) const { return m_id_ != tid.m_id_; }

  /* Support implicit type converting from Table_id to unsigned long long */
  operator unsigned long long() const { return m_id_; }

  Table_id operator++(int)
  {
    Table_id id(m_id_);

    /* m_id is reset to 0, when it exceeds the max value. */
    m_id_ = (m_id_ == TABLE_ID_MAX ? 0 : m_id_ + 1);

    assert(m_id_ <= TABLE_ID_MAX);
    return id;
  }
};
