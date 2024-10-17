//
// Created by Coonger on 2024/10/17.
//

// copy from sql/basic_ostream.h
#ifndef LOFT_BASIC_OSTREAM_H
#define LOFT_BASIC_OSTREAM_H

#include "constants.h"
#include <fstream>
#include <cassert>
#include <memory>

/**
   Basic_ostream 抽象类提供 write(), seek(), sync(), flush() 接口，用于写入数据到 buffer 中
*/
class Basic_ostream {
  public:
    // Write data to buffer, return true on success, false on failure
    virtual bool write(const loft::uchar *buffer, loft::my_off_t length) = 0;
    virtual bool seek(loft::my_off_t position) = 0;
    virtual bool sync() = 0;
    virtual bool flush() = 0;

    virtual ~Basic_ostream() = default;
};


class Binlog_ofile : public Basic_ostream {
  public:
    ~Binlog_ofile() override {
        close();
    }

    // Use std::fstream directly, ensuring successful opening
    bool open(const char *binlog_name) {
        std::unique_ptr<std::fstream> file_ostream = std::make_unique<std::fstream>(binlog_name, std::ios::out | std::ios::binary);
        if (!file_ostream->is_open()) {
            return false;
        }

        m_pipeline_head_ = std::move(file_ostream);
        return true;
    }

    void close() {
        if (m_pipeline_head_) {
            m_pipeline_head_->close();
            m_pipeline_head_.reset();
            m_position_ = 0;
        }
    }

    bool write(const loft::uchar *buffer, loft::my_off_t length) override {
        assert(m_pipeline_head_ != nullptr);
        m_pipeline_head_->write(reinterpret_cast<const char *>(buffer), length);

        if (!m_pipeline_head_->good()) {
            return false;
        }

        m_position_ += length;
        return true;
    }

    bool seek(loft::my_off_t position) override {
        assert(m_pipeline_head_ != nullptr);
        m_pipeline_head_->seekp(position);
        if (!m_pipeline_head_->good()) {
            return false;
        }
        m_position_ = position;
        return true;
    }

    bool sync() override {
        assert(m_pipeline_head_ != nullptr);
        m_pipeline_head_->flush();
        return m_pipeline_head_->good();
    }

    bool flush() override {
        return sync();
    }

    // Helper functions
    loft::my_off_t position() const { return m_position_; }

    // Provide reference instead of moving the stream out
    std::fstream &get_pipeline_head() {
        return *m_pipeline_head_;
    }

    bool is_empty() const { return m_position_ == 0; }

    bool is_open() const { return m_pipeline_head_ != nullptr; }

  private:
    loft::my_off_t m_position_ = 0;
    std::unique_ptr<std::fstream> m_pipeline_head_;
};

#endif // LOFT_BASIC_OSTREAM_H
