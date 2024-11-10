//
// Created by Coonger on 2024/10/17.
//

// copy from sql/basic_ostream.h
#ifndef LOFT_BASIC_OSTREAM_H
#define LOFT_BASIC_OSTREAM_H

#include "constants.h"
#include "rc.h"
#include <cassert>
#include <fstream>
#include <memory>

/**
   Basic_ostream 抽象类提供 write(), seek(), sync(), flush()
   接口，用于写入数据到 buffer 中
*/
class Basic_ostream {
  public:
    // Write data to buffer, return true on success, false on failure
    virtual bool write(const loft::uchar *buffer, loft::my_off_t length) = 0;
    virtual RC seek(loft::my_off_t position) = 0;
    virtual RC sync() = 0;
    virtual RC flush() = 0;
    virtual loft::my_off_t get_position() = 0;

    virtual ~Basic_ostream() = default;
};

class Binlog_ofile : public Basic_ostream {
  public:
    Binlog_ofile(const char *binlog_name, RC &rc);

    ~Binlog_ofile() override { close(); }

    bool write(const loft::uchar *buffer, loft::my_off_t length) override;
    RC seek(loft::my_off_t position) override;
    RC sync() override;
    RC flush() override;

    // Helper functions
    loft::my_off_t get_position() override { return m_position_; };

    bool is_empty() const { return m_position_ == 0; }

    bool is_open() const { return m_pipeline_head_ != nullptr; }

  private:
    bool open(const char *binlog_name) {
        std::unique_ptr<std::fstream> file_ostream =
            std::make_unique<std::fstream>(
                binlog_name, std::ios::out | std::ios::binary
            );
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

  private:
    loft::my_off_t m_position_;
    std::unique_ptr<std::fstream> m_pipeline_head_;
};

#endif // LOFT_BASIC_OSTREAM_H
