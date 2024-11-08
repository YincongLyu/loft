//
// Created by Coonger on 2024/10/21.
//
#include "rows_event.h"
#include "./sql/mysql_fields.h"
#include "abstract_event.h"
#include "little_endian.h"

#include "logging.h"

template<typename T, typename UT>
class Bit_stream_base {
  protected:
    T *m_ptr;
    uint m_current_bit;

  public:
    Bit_stream_base(T *ptr) : m_ptr(ptr), m_current_bit(0) {}

    /**
      Set the buffer pointer.
      @param ptr Pointer where bits will be read or written.
    */
    void set_ptr(T *ptr) { m_ptr = ptr; }

    /**
      Set the buffer pointer, using an unsigned datatype.
      @param ptr Pointer where bits will be read or written.
    */
    void set_ptr(UT *ptr) { m_ptr = (T *)ptr; }

    /// @return the current position.
    uint tell() const { return m_current_bit; }
};

/*
 * bit 写入器
 */
class Bit_writer : public Bit_stream_base<char, uchar> {
  public:
    Bit_writer(char *ptr = nullptr) : Bit_stream_base<char, uchar>(ptr) {}

    Bit_writer(uchar *ptr) : Bit_writer((char *)ptr) {}

    /**
      Write the next bit and move the write position one bit forward.
      @param set_to_on If true, set the bit to 1, otherwise set it to 0.
    */
    void set(bool set_to_on) {
        uint byte = m_current_bit / 8;
        uint bit_within_byte = m_current_bit % 8;
        m_current_bit++;
        if (bit_within_byte == 0) {
            m_ptr[byte] = set_to_on ? 1 : 0;
        } else if (set_to_on) {
            m_ptr[byte] |= 1 << bit_within_byte;
        }
    }
};

Table_map_event::Table_map_event(
    const Table_id &tid,
    unsigned long colcnt,
    const char *dbnam,
    size_t dblen,
    const char *tblnam,
    size_t tbllen,
    std::vector<mysql::Field *> &column_view
)
    : AbstractEvent(TABLE_MAP_EVENT)
    , m_table_id_(tid)
    , m_data_size_(0)
    , m_dbnam_("")
    , m_dblen_(dblen)
    , m_tblnam_("")
    , m_tbllen_(tbllen)
    , m_colcnt_(colcnt)
    , m_column_view_(column_view)
    , /* json fields's size()*/
    m_field_metadata_size_(0)
    , m_field_metadata_(nullptr)
    , m_null_bits_(nullptr) {
    if (dbnam) {
        m_dbnam_ = std::string(dbnam, m_dblen_);
    }
    if (tblnam) {
        m_tblnam_ = std::string(tblnam, m_tbllen_);
    }

    m_data_size_ = TABLE_MAP_HEADER_LEN;

    uchar dbuf[sizeof(m_dblen_) + 1];
    uchar tbuf[sizeof(m_tbllen_) + 1];
    uchar *const dbuf_end = net_store_length(dbuf, (size_t)m_dblen_);
    assert(static_cast<size_t>(dbuf_end - dbuf) <= sizeof(dbuf));
    uchar *const tbuf_end = net_store_length(tbuf, (size_t)m_tbllen_);
    assert(static_cast<size_t>(tbuf_end - tbuf) <= sizeof(tbuf));

    m_data_size_ +=
        m_dblen_ + 1 + (dbuf_end - dbuf); // Include length and terminating \0
    m_data_size_ +=
        m_tbllen_ + 1 + (tbuf_end - tbuf); // Include length and terminating \0

    // =========================m_column_view_ 初始化, 制作 表头==============
    // 1. 根据 tid 找 Table 对象, 实际上不用，因为现在 tid
    // 只是个临时值，没有实际含义，所有的信息都是从外部读取的
    // 但是为了做调度，所以 tid 还是要记下来，不用实时给外面响应

    m_coltype_ = static_cast<unsigned char *>(malloc(colcnt));
    long pos = 0;
    for (auto &field : m_column_view_) {
        m_coltype_[pos++] = field->binlog_type();
        LOG_INFO(
            "init coltype_: field->binlog_type() = %d", field->binlog_type()
        );
    }

    uchar cbuf[sizeof(m_colcnt_) + 1];
    uchar *cbuf_end;
    cbuf_end = net_store_length(cbuf, (size_t)m_colcnt_);
    assert(static_cast<size_t>(cbuf_end - cbuf) <= sizeof(cbuf));
    m_data_size_ += (cbuf_end - cbuf) + m_colcnt_; // COLCNT and column types

    // 3. 得到每个 Field 的元数据
    m_field_metadata_ = static_cast<unsigned char *>(malloc(m_colcnt_ * 4));
    memset(m_field_metadata_, 0, m_colcnt_ * 4);
    m_field_metadata_size_ =
        save_field_metadata(); // 同时也填充了 m_field_metadata_
    if (m_field_metadata_size_ < 251) {
        m_data_size_ += m_field_metadata_size_ + 1;
    } else {
        m_data_size_ += m_field_metadata_size_ + 3;
    }

    /////////////////////////////
    uint num_null_bytes = (m_colcnt_ + 7) / 8;
    m_data_size_ += num_null_bytes;

    m_null_bits_ = static_cast<unsigned char *>(malloc(num_null_bytes));
    memset(m_null_bits_, 0, num_null_bytes);
    Bit_writer bit_writer{this->m_null_bits_};

    for (auto &field : m_column_view_) {
        bit_writer.set(field->is_nullable());
    }

    LOG_INFO("table_map_event data size: %zu", m_data_size_);

    this->common_header_ = new EventCommonHeader();
    //    this->common_footer_ = new EventCommonFooter(BINLOG_CHECKSUM_ALG_OFF);
}

Table_map_event::~Table_map_event() = default;

int Table_map_event::save_field_metadata() {
    int index = 0;
    for (auto &field : m_column_view_) {
        index += field->save_field_metadata(&m_field_metadata_[index]);
    }

    return index;
}

bool Table_map_event::write_data_header(Basic_ostream *ostream) {
    assert(m_table_id_.is_valid());
    uchar buf[AbstractEvent::TABLE_MAP_HEADER_LEN];
    int6store(buf + TM_MAPID_OFFSET, m_table_id_.get_id());
    int2store(buf + TM_FLAGS_OFFSET, m_flags);
    return ostream->write(buf, AbstractEvent::TABLE_MAP_HEADER_LEN);
}

bool Table_map_event::write_data_body(Basic_ostream *ostream) {
    assert(!m_dbnam_.empty());
    assert(!m_tblnam_.empty());

    uchar dbuf[sizeof(m_dblen_) + 1];
    uchar *const dbuf_end = net_store_length(dbuf, (size_t)m_dblen_);
    assert(static_cast<size_t>(dbuf_end - dbuf) <= sizeof(dbuf));

    uchar tbuf[sizeof(m_tbllen_) + 1];
    uchar *const tbuf_end = net_store_length(tbuf, (size_t)m_tbllen_);
    assert(static_cast<size_t>(tbuf_end - tbuf) <= sizeof(tbuf));

    uchar cbuf[sizeof(m_colcnt_) + 1];
    uchar *const cbuf_end = net_store_length(cbuf, (size_t)m_colcnt_);
    assert(static_cast<size_t>(cbuf_end - cbuf) <= sizeof(cbuf));

    /*
      Store the size of the field metadata.
    */
    uchar mbuf[2 * sizeof(m_field_metadata_size_)];
    uchar *const mbuf_end = net_store_length(mbuf, m_field_metadata_size_);

    //    uchar *m_optional_metadata_ = new uchar[17];
    //    // 初始化数据
    //    uchar data[17] = {0x01, 0x02, 0x2D, 0x56, 0x02, 0x0B, 0xFC, 0xFF,
    //    0x00,
    //                      0x05, 0x3F, 0x06, 0x3F, 0x07, 0x3F, 0x08, 0x3F};
    //    // 将数据复制到 m_optional_metadata_ 指向的内存中
    //    std::memcpy(m_optional_metadata_, data, 17);
    //    size_t m_optional_metadata_len = 17;

    return ostream->write(dbuf, (size_t)(dbuf_end - dbuf))
           && ostream->write((const uchar *)m_dbnam_.c_str(), m_dblen_ + 1)
           && ostream->write(tbuf, (size_t)(tbuf_end - tbuf))
           && ostream->write((const uchar *)m_tblnam_.c_str(), m_tbllen_ + 1)
           && ostream->write(cbuf, (size_t)(cbuf_end - cbuf))
           && ostream->write(m_coltype_, m_colcnt_)
           && ostream->write(mbuf, (size_t)(mbuf_end - mbuf))
           && ostream->write(m_field_metadata_, m_field_metadata_size_)
           && ostream->write(m_null_bits_, (m_colcnt_ + 7) / 8);
    // 最后一个 m_optional_metadata_len, m_optional_metadata 可以暂时不管
}
