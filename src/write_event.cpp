#include "write_event.h"

Rows_event::Rows_event(const Table_id &tid, unsigned long wid,uint16_t flag, Log_event_type type):
    m_table_id(tid), m_type(type), AbstractEvent(type) {
    data_size1 = 0;
    data_size2 = 0;
    this->Set_width(wid);
    this->Set_flags(flag);
    cols_init();
    //bits_init();

    this->common_header_ = new EventCommonHeader();
//    this->common_footer_ = new EventCommonFooter(BINLOG_CHECKSUM_ALG_OFF);
}

Rows_event::~Rows_event() = default;

void Rows_event::cols_init() {
    int N = Get_N();
    columns_after_image = (uchar *)malloc(N * sizeof(uchar));
    memset(columns_after_image, 0xff, N * sizeof(uchar));
    columns_before_image = (uchar *)malloc(N * sizeof(uchar));
    memset(columns_before_image, 0xff, N * sizeof(uchar));
}

void Rows_event::bits_init() {
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
    uchar   buf[ROWS_HEADER_LEN_V2];
    int6store(buf + ROWS_MAPID_OFFSET, m_table_id.get_id());
    int2store(buf + ROWS_FLAGS_OFFSET, m_flags);
    uint extra_row_info_payloadlen = EXTRA_ROW_INFO_HEADER_LENGTH;
    int2store(buf + ROWS_VHLEN_OFFSET, extra_row_info_payloadlen);
    return ostream->write(buf,ROWS_HEADER_LEN_V2);
}


bool Rows_event::write_data_body(Basic_ostream *ostream) {
    bool res = true;
    uchar sbuf[sizeof(m_width) + 1];
    uchar *const sbuf_end = net_store_length(sbuf, (size_t)m_width);
    res &= ostream->write(sbuf, sbuf_end - sbuf);

    if(m_type == Log_event_type::UPDATE_ROWS_EVENT || m_type == Log_event_type::DELETE_ROWS_EVENT) {
        int N =  Get_N();
        if(rows_before.size() != 0)      memset(columns_before_image, 0,N * sizeof(uchar));
      for(int i = 0; i < rows_before.size(); i++) {
            assert(rows_before[i] <= m_width);
            int n = N - ((rows_before[i]-1)/8 + 1);
            set_N_bit(*(columns_before_image+n), (rows_before[i]-1)%8 + 1);
        }
        if(rows_before.size() != 0) {
            row_bitmap_before = (uchar *)malloc(((rows_before.size() + 7) /8) * sizeof(uchar));
            memset(row_bitmap_before, 0x00, ((rows_before.size() + 7) /8) * sizeof(uchar));
        }
        std::reverse(columns_before_image, columns_before_image + N);
        res &= ostream->write(columns_before_image, N);
    }

    if(m_type == Log_event_type::UPDATE_ROWS_EVENT || m_type == Log_event_type::WRITE_ROWS_EVENT) {
        int N =  Get_N();
        if(rows_after.size() != 0)      memset(columns_after_image, 0, N * sizeof(uchar));
      for(int i = 0; i < rows_after.size(); i++) {
            assert(rows_after[i] <= m_width);
            int n = N - ((rows_after[i] - 1)/8 + 1);
            set_N_bit(*(columns_after_image+n), (rows_after[i] - 1)%8 + 1);
        }
        if(rows_after.size() != 0)   {
            row_bitmap_after = (uchar *)malloc(((rows_after.size() + 7) /8) * sizeof(uchar));
            memset(row_bitmap_after, 0x00, ((rows_after.size() + 7) /8) * sizeof(uchar));
        }
        std::reverse(columns_after_image, columns_after_image + N);
        res &= ostream->write(columns_after_image, N);
    }

    if(m_type == Log_event_type::UPDATE_ROWS_EVENT || m_type == Log_event_type::DELETE_ROWS_EVENT) {
        int N = (rows_before.size() + 7) /8;
        for(int i = 0; i < null_before.size(); i++) {
            if(null_before[i]) {
                int n = N - (i/8 + 1);
                set_N_bit(*(row_bitmap_before+n), i%8 + 1);
            }
        }
        std::reverse(row_bitmap_before, row_bitmap_before + N);
        res &= ostream->write(row_bitmap_before, N);
        res &= ostream->write(m_rows_before_buf, data_size1);
    }

    if(m_type == Log_event_type::UPDATE_ROWS_EVENT || m_type == Log_event_type::WRITE_ROWS_EVENT) {
        int N = (rows_after.size() + 7) / 8;
        for(int i = 0; i < null_after.size(); i++) {
            if(null_after[i]) {
                int n = N - (i/8 + 1);
                set_N_bit(*(row_bitmap_after+n), i%8 + 1);
            }
        }
        std::reverse(row_bitmap_after, row_bitmap_after + N);
        res &= ostream->write(row_bitmap_after, N);
        res &= ostream->write(m_rows_after_buf, data_size2);
    }
    return res;
}

bool Rows_event::write(Basic_ostream *ostream) {
    return
        write_common_header(ostream, get_data_size())
        && write_data_header(ostream) && write_data_body(ostream);
}

void Rows_event::buf_resize(uint8_t* &buf, size_t size1, size_t size2) {
    uchar *m = new uchar[size2];
    if(size1 > 0) {
        memcpy(m, buf, size1);
        delete[] buf;  // 使用 delete[] 因为是数组
    }
    buf = m;
}

void Rows_event::double2demi(double num, decimal_t &t, int precision, int frac) {
    if(num < 0) {
        num = -num;
        t.sign = true;
    } else {
        t.sign = false;
    }
    int32_t *buf = new int32_t[precision/9 + precision % 9];
    unsigned long long intg = num;
    double frac1 = num - intg;
    unsigned long long fracg;
    int j = 0;
    for(int i = 0; i < frac; i++)  frac1 *= 10;
  fracg = frac1;
    while(fracg <= 99999999 && fracg != 0) fracg *= 10;
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

size_t Rows_event::calculate_event_size() {
    size_t event_size = 0;
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
    return event_size;
}
