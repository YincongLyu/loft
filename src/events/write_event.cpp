//
// Created by Takenzz on 2024/10/20.
//

#include "events/write_event.h"

Rows_event::Rows_event(
    const Table_id &tid, unsigned long wid, uint16 flag, Log_event_type type, uint64 immediate_commit_timestamp_arg)
    : m_table_id(tid), m_type(type), AbstractEvent(type)
{
  // 构造函数中预分配内存，按照 30 columns 来算 * 8 byte， string类型会经常扩容
  const size_t INITIAL_SIZE = 64;
  m_rows_before_buf         = std::make_unique<uchar[]>(INITIAL_SIZE);
  m_rows_after_buf          = std::make_unique<uchar[]>(INITIAL_SIZE);
  m_before_capacity         = INITIAL_SIZE;
  m_after_capacity          = INITIAL_SIZE;
  before_data_size_used     = 0;
  after_data_size_used      = 0;

  this->Set_width(wid);
  this->Set_flags(flag);
  cols_init();

  time_t i_ts          = static_cast<time_t>(immediate_commit_timestamp_arg / 1000000);
  this->common_header_ = std::make_unique<EventCommonHeader>(i_ts);
  //    this->common_footer_ = new EventCommonFooter(BINLOG_CHECKSUM_ALG_OFF);
}

Rows_event::~Rows_event() = default;

void Rows_event::cols_init()
{
  int N               = Get_N();
  columns_after_image = std::make_unique<uchar[]>(N);
  memset(columns_after_image.get(), 0xff, N * sizeof(uchar));

  columns_before_image = std::make_unique<uchar[]>(N);
  memset(columns_before_image.get(), 0xff, N * sizeof(uchar));
}

bool Rows_event::write_data_header(Basic_ostream *ostream)
{
  uchar buf[ROWS_HEADER_LEN_V2];
  int6store(buf + ROWS_MAPID_OFFSET, m_table_id.get_id());
  int2store(buf + ROWS_FLAGS_OFFSET, m_flags);
  uint extra_row_info_payloadlen = EXTRA_ROW_INFO_HEADER_LENGTH;
  int2store(buf + ROWS_VHLEN_OFFSET, extra_row_info_payloadlen);
  return ostream->write(buf, ROWS_HEADER_LEN_V2);
}

bool Rows_event::write_data_body(Basic_ostream *ostream)
{
  bool         res = true;
  uchar        sbuf[sizeof(m_width) + 1];
  uchar *const sbuf_end = net_store_length(sbuf, (size_t)m_width);
  res &= ostream->write(sbuf, sbuf_end - sbuf);

  if (m_type == Log_event_type::UPDATE_ROWS_EVENT || m_type == Log_event_type::DELETE_ROWS_EVENT) {
    int N = Get_N();
    if (rows_before.size() != 0) {
      memset(columns_before_image.get(), 0, N * sizeof(uchar));  // 使用 get()
    }

    for (int i = 0; i < rows_before.size(); i++) {
      assert(rows_before[i] <= m_width);
      int n = N - ((rows_before[i] - 1) / 8 + 1);
      set_N_bit(*(columns_before_image.get() + n), (rows_before[i] - 1) % 8 + 1);  // 使用 get()
    }

    if (rows_before.size() != 0) {
      size_t row_bitmap_size = (rows_before.size() + 7) / 8;
      row_bitmap_before      = std::make_unique<uchar[]>(row_bitmap_size);
      memset(row_bitmap_before.get(), 0x00, row_bitmap_size * sizeof(uchar));  // 使用 get()
    }

    std::reverse(columns_before_image.get(), columns_before_image.get() + N);  // 使用 get()
    res &= ostream->write(columns_before_image.get(), N);                      // 使用 get()
  }

  if (m_type == Log_event_type::UPDATE_ROWS_EVENT || m_type == Log_event_type::WRITE_ROWS_EVENT) {
    int N = Get_N();
    if (rows_after.size() != 0) {
      memset(columns_after_image.get(), 0, N * sizeof(uchar));  // 使用 get()
    }

    for (int i = 0; i < rows_after.size(); i++) {
      assert(rows_after[i] <= m_width);
      int n = N - ((rows_after[i] - 1) / 8 + 1);
      set_N_bit(*(columns_after_image.get() + n), (rows_after[i] - 1) % 8 + 1);  // 使用 get()
    }

    if (rows_after.size() != 0) {
      size_t row_bitmap_size = (rows_after.size() + 7) / 8;
      row_bitmap_after       = std::make_unique<uchar[]>(row_bitmap_size);
      memset(row_bitmap_after.get(), 0x00, row_bitmap_size * sizeof(uchar));  // 使用 get()
    }

    std::reverse(columns_after_image.get(), columns_after_image.get() + N);  // 使用 get()
    res &= ostream->write(columns_after_image.get(), N);                     // 使用 get()
  }

  if (m_type == Log_event_type::UPDATE_ROWS_EVENT || m_type == Log_event_type::DELETE_ROWS_EVENT) {
    int N = (rows_before.size() + 7) / 8;
    for (int i = 0; i < null_before.size(); i++) {
      if (null_before[i]) {
        int n = N - (i / 8 + 1);
        set_N_bit(*(row_bitmap_before.get() + n), i % 8 + 1);
      }
    }
    std::reverse(row_bitmap_before.get(), row_bitmap_before.get() + N);
    res &= ostream->write(row_bitmap_before.get(), N);
    res &= ostream->write(m_rows_before_buf.get(), before_data_size_used);
  }

  if (m_type == Log_event_type::UPDATE_ROWS_EVENT || m_type == Log_event_type::WRITE_ROWS_EVENT) {
    int N = (rows_after.size() + 7) / 8;
    for (int i = 0; i < null_after.size(); i++) {
      if (null_after[i]) {
        int n = N - (i / 8 + 1);
        set_N_bit(*(row_bitmap_after.get() + n), i % 8 + 1);
      }
    }
    std::reverse(row_bitmap_after.get(), row_bitmap_after.get() + N);
    res &= ostream->write(row_bitmap_after.get(), N);
    res &= ostream->write(m_rows_after_buf.get(), after_data_size_used);
  }
  return res;
}

bool Rows_event::write(Basic_ostream *ostream)
{
  return write_common_header(ostream, get_data_size()) && write_data_header(ostream) && write_data_body(ostream);
}

void Rows_event::buf_resize(std::unique_ptr<uchar[]> &buf, size_t &capacity, size_t current_size, size_t needed_size)
{
  if (needed_size <= capacity) {
    return;  // 如果现有容量足够，直接返回
  }

  // 计算新容量：至少是needed_size，并且是当前容量的2倍
  size_t new_capacity = std::max(needed_size, capacity * 2);
  auto   new_buf      = std::make_unique<uchar[]>(new_capacity);

  // 拷贝现有数据
  if (current_size > 0 && buf) {
    memcpy(new_buf.get(), buf.get(), current_size);
  }

  buf      = std::move(new_buf);
  capacity = new_capacity;
}

void Rows_event::double2demi(double num, decimal_t &t, int precision, int frac)
{
  if (num < 0) {
    num    = -num;
    t.sign = true;
  } else {
    t.sign = false;
  }
  int32_t           *buf   = new int32_t[precision / 9 + precision % 9];
  unsigned long long intg  = num;
  double             frac1 = num - intg;
  unsigned long long fracg;
  int                j = 0;
  for (int i = 0; i < frac; i++) {
    frac1 *= 10;
  }
  fracg = frac1;
  while (fracg <= 99999999 && fracg != 0) {
    fracg *= 10;
  }
  while (intg) {
    buf[j++] = intg % 1000000000;
    intg /= 1000000000;
  }
  while (fracg) {
    buf[j++] = fracg % 1000000000;
    fracg /= 1000000000;
  }
  t.buf  = buf;
  t.len  = 9;
  t.intg = precision - frac;
  t.frac = frac;
}

size_t Rows_event::calculate_event_size()
{
  size_t       event_size = 0;
  size_t       n          = Get_N();
  uchar        sbuf[sizeof(m_width) + 1];
  uchar *const sbuf_end = net_store_length(sbuf, (size_t)m_width);
  event_size += ROWS_HEADER_LEN_V2;
  event_size += before_data_size_used;
  event_size += after_data_size_used;
  event_size += (sbuf_end - sbuf);
  if (m_type == Log_event_type::WRITE_ROWS_EVENT) {
    event_size += n;
    event_size += (rows_after.size() + 7) / 8;
  } else if (m_type == Log_event_type::DELETE_ROWS_EVENT) {
    event_size += n;
    event_size += (rows_before.size() + 7) / 8;
  } else if (m_type == Log_event_type::UPDATE_ROWS_EVENT) {
    event_size += n;
    event_size += (rows_before.size() + 7) / 8;
    event_size += n;
    event_size += (rows_after.size() + 7) / 8;
  }
  return event_size;
}

size_t Rows_event::write_data_header_to_buffer(uchar *buffer)
{
  int6store(buffer + ROWS_MAPID_OFFSET, m_table_id.get_id());
  int2store(buffer + ROWS_FLAGS_OFFSET, m_flags);
  uint extra_row_info_payloadlen = EXTRA_ROW_INFO_HEADER_LENGTH;
  int2store(buffer + ROWS_VHLEN_OFFSET, extra_row_info_payloadlen);

  return ROWS_HEADER_LEN_V2;
}

size_t Rows_event::write_data_body_to_buffer(uchar *buffer)
{
  uchar *current_pos = buffer;

  // 写入width
  uchar        sbuf[sizeof(m_width) + 1];
  uchar *const sbuf_end = net_store_length(sbuf, (size_t)m_width);
  memcpy(current_pos, sbuf, sbuf_end - sbuf);
  current_pos += (sbuf_end - sbuf);

  // 处理DELETE和UPDATE事件的before image
  if (m_type == Log_event_type::UPDATE_ROWS_EVENT || m_type == Log_event_type::DELETE_ROWS_EVENT) {
    int N = Get_N();
    if (rows_before.size() != 0) {
      memset(columns_before_image.get(), 0, N * sizeof(uchar));
    }

    for (int i = 0; i < rows_before.size(); i++) {
      assert(rows_before[i] <= m_width);
      int n = N - ((rows_before[i] - 1) / 8 + 1);
      set_N_bit(*(columns_before_image.get() + n), (rows_before[i] - 1) % 8 + 1);
    }

    if (rows_before.size() != 0) {
      size_t row_bitmap_size = (rows_before.size() + 7) / 8;
      row_bitmap_before      = std::make_unique<uchar[]>(row_bitmap_size);
      memset(row_bitmap_before.get(), 0x00, row_bitmap_size * sizeof(uchar));
    }

    std::reverse(columns_before_image.get(), columns_before_image.get() + N);
    memcpy(current_pos, columns_before_image.get(), N);
    current_pos += N;
  }

  // 处理WRITE和UPDATE事件的after image
  if (m_type == Log_event_type::UPDATE_ROWS_EVENT || m_type == Log_event_type::WRITE_ROWS_EVENT) {
    int N = Get_N();
    if (rows_after.size() != 0) {
      memset(columns_after_image.get(), 0, N * sizeof(uchar));
    }

    for (int i = 0; i < rows_after.size(); i++) {
      assert(rows_after[i] <= m_width);
      int n = N - ((rows_after[i] - 1) / 8 + 1);
      set_N_bit(*(columns_after_image.get() + n), (rows_after[i] - 1) % 8 + 1);
    }

    if (rows_after.size() != 0) {
      size_t row_bitmap_size = (rows_after.size() + 7) / 8;
      row_bitmap_after       = std::make_unique<uchar[]>(row_bitmap_size);
      memset(row_bitmap_after.get(), 0x00, row_bitmap_size * sizeof(uchar));
    }

    std::reverse(columns_after_image.get(), columns_after_image.get() + N);
    memcpy(current_pos, columns_after_image.get(), N);
    current_pos += N;
  }

  // 写入before数据
  if (m_type == Log_event_type::UPDATE_ROWS_EVENT || m_type == Log_event_type::DELETE_ROWS_EVENT) {
    int N = (rows_before.size() + 7) / 8;
    for (int i = 0; i < null_before.size(); i++) {
      if (null_before[i]) {
        int n = N - (i / 8 + 1);
        set_N_bit(*(row_bitmap_before.get() + n), i % 8 + 1);
      }
    }
    std::reverse(row_bitmap_before.get(), row_bitmap_before.get() + N);
    memcpy(current_pos, row_bitmap_before.get(), N);
    current_pos += N;

    memcpy(current_pos, m_rows_before_buf.get(), before_data_size_used);
    current_pos += before_data_size_used;
  }

  // 写入after数据
  if (m_type == Log_event_type::UPDATE_ROWS_EVENT || m_type == Log_event_type::WRITE_ROWS_EVENT) {
    int N = (rows_after.size() + 7) / 8;
    for (int i = 0; i < null_after.size(); i++) {
      if (null_after[i]) {
        int n = N - (i / 8 + 1);
        set_N_bit(*(row_bitmap_after.get() + n), i % 8 + 1);
      }
    }
    std::reverse(row_bitmap_after.get(), row_bitmap_after.get() + N);
    memcpy(current_pos, row_bitmap_after.get(), N);
    current_pos += N;

    memcpy(current_pos, m_rows_after_buf.get(), after_data_size_used);
    current_pos += after_data_size_used;
  }

  return current_pos - buffer;
}
