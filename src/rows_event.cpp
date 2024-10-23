//
// Created by Coonger on 2024/10/21.
//
#include "rows_event.h"
#include "abstract_event.h"
#include "little_endian.h"

Table_map_event::Table_map_event(const Table_id &tid, unsigned long colcnt, const char *dbnam,
                                 size_t dblen, const char *tblnam, size_t tbllen): AbstractEvent(TABLE_MAP_EVENT),
      m_table_id_(tid),
      m_data_size_(0),
      m_dbnam_(""),
      m_dblen_(dblen),
      m_tblnam_(""),
      m_tbllen_(tbllen),
      m_colcnt_(colcnt), /* json fields's size()*/
      m_field_metadata_size_(0),
      m_field_metadata_(nullptr),
      m_null_bits_(nullptr),
      m_optional_metadata_len(0),
      m_optional_metadata(nullptr) {

    if (dbnam) m_dbnam_ = std::string(dbnam, m_dblen_);
  if (tblnam) m_tblnam_ = std::string(tblnam, m_tbllen_);

  // TODO m_colcnt_, m_coltype_, m_field_metadata_size_, field_metadata_, m_null_bits, m_optional_metadata_len, m_optional_metadata 初始化
    // m_column_view_ 初始化
  // 1. 根据 tid 找 Table 对象

  // 2. 处理每个 column 字段
  // 2.1 遍历 fields 里面的每个 Field 包装成一个 Field 对象， 放入 m_column_view_
  // 2.2.1 同时 计算得到 m_coltype
  // 2.2.2 计算 m_null_bits_

  // 3. 得到每个 Field 的元数据

  m_metadata_buf_.Reserve(1024);
  m_field_metadata_size_ = save_field_metadata();

  // 4. 可以暂时不管 m_optinal_metadata_len, m_optional_metadata
}

int Table_map_event::save_field_metadata() {

    int index = 0;
    for (auto& field_obj : m_column_view_) {
        Field *field = field_obj.get();
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

    return  ostream->write(dbuf, (size_t)(dbuf_end - dbuf)) &&
            ostream->write((const uchar *)m_dbnam_.c_str(), m_dblen_ + 1) &&
            ostream->write(tbuf, (size_t)(tbuf_end - tbuf)) &&
            ostream->write((const uchar *)m_tblnam_.c_str(), m_tbllen_ + 1) &&
            ostream->write(cbuf, (size_t)(cbuf_end - cbuf)) &&
            ostream->write(m_coltype_, m_colcnt_) &&
            ostream->write(mbuf, (size_t)(mbuf_end - mbuf)) &&
            ostream->write(m_field_metadata_, m_field_metadata_size_) &&
            ostream->write(m_null_bits_, (m_colcnt_ + 7) / 8) &&
            ostream->write((const uchar *)m_metadata_buf_.GetString(), m_metadata_buf_.GetLength());
}
