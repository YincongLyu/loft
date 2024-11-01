//
// Created by Coonger on 2024/10/18.
//
#include "basic_ostream.h"

bool Binlog_ofile::write(const loft::uchar *buffer, loft::my_off_t length) {
    assert(m_pipeline_head_ != nullptr);

    if (length == 0) {
        return true;
    }

    m_pipeline_head_->write(reinterpret_cast<const char *>(buffer), length);

    if (!m_pipeline_head_->good()) {
        return false;
    }

    m_position_ += length;
    return true;
}

bool Binlog_ofile::seek(loft::my_off_t position) {
    assert(m_pipeline_head_ != nullptr);
    m_pipeline_head_->seekp(position);
    if (!m_pipeline_head_->good()) {
        return false;
    }
    m_position_ = position;
    return true;
}

bool Binlog_ofile::sync() {
    assert(m_pipeline_head_ != nullptr);
    m_pipeline_head_->flush();
    return m_pipeline_head_->good();
}

bool Binlog_ofile::flush() {
    return sync();
}
