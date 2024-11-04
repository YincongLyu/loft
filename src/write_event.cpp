#include "write_event.h"

Rows_event::Rows_event(uint64_t id, unsigned long wid,uint16_t flag, Log_event_type type): m_table_id(id), m_type(type), AbstractEvent(type){
    data_size = 0;
    data_size1 = 0;
    data_size2 = 0;
    this->Set_width(wid);
    this->Set_flags(flag);
    cols_init();
    bits_init();

    this->common_header_ = new EventCommonHeader();
   
    this->common_footer_ = new EventCommonFooter(BINLOG_CHECKSUM_ALG_OFF);
}

Rows_event::~Rows_event() {
}

void Rows_event::cols_init() {
    int N = Get_N();
    columns_after_image = (uchar *)malloc(N * sizeof(uchar));
    memset(columns_after_image, 0xff, N * sizeof(uchar));
    columns_before_image = (uchar *)malloc(N * sizeof(uchar));
    memset(columns_before_image, 0xff, N * sizeof(uchar));
}

void Rows_event::bits_init()  {
    int N = Get_N();
    row_bitmap_after = (uchar *)malloc(N * sizeof(uchar));
    memset(row_bitmap_after, 0x00, N * sizeof(uchar));
    for(int i = 0; i < m_width; i++) {
      int n = Get_N() - (i/8 + 1);
      set_N_bit(*(row_bitmap_after+n),  i%8 + 1);
    }

    row_bitmap_before = (uchar *)malloc(N * sizeof(uchar));
    memset(row_bitmap_before, 0x00, N * sizeof(uchar));
    for(int i = 0; i < m_width; i++) {
      int n = Get_N() - (i/8 + 1);
      set_N_bit(*(row_bitmap_before+n),  i%8 + 1);
    }
  }

bool  Rows_event::write_data_header(Basic_ostream *ostream) {
    uchar   buf[10];
    int4store(buf + ROWS_MAPID_OFFSET, m_table_id.id());
    int2store(buf + ROWS_FLAGS_OFFSET, m_flags);
    uint extra_row_info_payloadlen = EXTRA_ROW_INFO_HEADER_LENGTH;
    int2store(buf + ROWS_VHLEN_OFFSET, extra_row_info_payloadlen);
    return ostream->write(buf,10);
}

bool Rows_event::write_data_body(Basic_ostream *ostream) {
    bool res = true;
    uchar sbuf[sizeof(m_width) + 1];
    uchar *const sbuf_end = net_store_length(sbuf, (size_t)m_width);
    res &= ostream->write(sbuf, sbuf_end - sbuf);

    if(m_type == Log_event_type::UPDATE_ROWS_EVENT || m_type == Log_event_type::DELETE_ROWS_EVENT) {
      if(rows_before.size() != 0)      memset(columns_before_image, 0, Get_N() * sizeof(uchar));
      for(int i = 0; i < rows_before.size(); i++) {
        int n = Get_N() - ((rows_before[i]-1)/8 + 1);
        clear_N_bit(*(columns_before_image+n), (rows_before[i]-1)%8 + 1);
      }
      if(rows_before.size() != 0)         memcpy(row_bitmap_before, columns_before_image, Get_N() * sizeof(uchar));
      std::reverse(columns_before_image, columns_before_image + Get_N());
      res &= ostream->write(columns_before_image, Get_N());
    }

    if(m_type == Log_event_type::UPDATE_ROWS_EVENT || m_type == Log_event_type::WRITE_ROWS_EVENT) {
      if(rows_after.size() != 0)      memset(columns_after_image, 0, Get_N() * sizeof(uchar));
      for(int i = 0; i < rows_after.size(); i++) {
        int n = Get_N() - ((rows_after[i] - 1)/8 + 1);
        set_N_bit(*(columns_after_image+n), (rows_after[i] - 1)%8 + 1);
      }
      if(rows_after.size() != 0)         memcpy(row_bitmap_after, columns_after_image, Get_N() * sizeof(uchar));
      std::reverse(columns_after_image, columns_after_image + Get_N());
      res &= ostream->write(columns_after_image, Get_N());
    }

    if(m_type == Log_event_type::UPDATE_ROWS_EVENT || m_type == Log_event_type::DELETE_ROWS_EVENT) {
      for(int i = 0; i < rows_before_exist.size(); i++) {
        int n = Get_N() - ((rows_before_exist[i]-1)/8 + 1);
        clear_N_bit(*(row_bitmap_before+n), (rows_before_exist[i] - 1)%8 + 1);
      }   
      res &= ostream->write(row_bitmap_before, Get_N());
      res &= ostream->write(m_rows_before_buf, data_size1);
    }
    if(m_type == Log_event_type::UPDATE_ROWS_EVENT || m_type == Log_event_type::WRITE_ROWS_EVENT) {
      for(int i = 0; i < rows_after_exist.size(); i++) {
        int n = Get_N() - ((rows_after_exist[i]-1)/8 + 1);
        clear_N_bit(*(row_bitmap_after+n), (rows_after_exist[i] - 1)%8 + 1);
      }   
      res &= ostream->write(row_bitmap_after, Get_N());
      res &= ostream->write(m_rows_after_buf, data_size2);
    }
    return res;
}

bool Rows_event::write(Basic_ostream *ostream) {
    return AbstractEvent::write(ostream);
}

void Rows_event::buf_resize(uint8_t* &buf, size_t size1, size_t size2){
  if(data_size == 0) {
    buf = (uchar *)malloc(size2 * sizeof(uchar));
    return ;
  }
  uchar *m = (uchar *)malloc(size2 * sizeof(uchar));
  memcpy(m, buf,size1);
  delete buf;
  buf = m;
}

void Rows_event::double2demi(double num, decimal_t &t, int precision, int frac) {
  if(num < 0) {
    num = -num;
    t.sign = true;
  } else {
    t.sign = false;
  }
  int32 *buf = new int32[precision/9 + precision % 9];
  unsigned long long intg = num; 
  double frac1 = num - intg;
  unsigned long long fracg;
  int j = 0;
  for(int i = 0; i < frac; i++)  frac1 *= 10;
  fracg = frac1;
  while(fracg <= 99999999) fracg *= 10;
  while(intg) {
    buf[j++] = intg % 1000000000;
    intg /= 1000000000;
  }
  while(fracg) {
    buf[j++] = fracg % 1000000000;
    fracg /= 1000000000;
  }
  t.buf = buf;
  t.len = 9;
  t.intg = precision - frac;
  t.frac = frac;
}
