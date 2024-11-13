//
// Created by Coonger on 2024/10/17.
//

#pragma once

#include <atomic>

#include "events/abstract_event.h"
#include "events/control_events.h"

#include "basic_ostream.h"
#include "constants.h"

#include "common/logging.h"
#include "common/rc.h"
#include "common/init_setting.h"


/**
  Transaction Coordinator Log.

  提供三种实现：
  1. one using an in-memory structure,
  2. one dummy that does not do anything 不保证事务，只写 log 到 file
  3. one using the binary log for transaction coordination. [only impl it]
*/
class TC_LOG {
  public:
    TC_LOG() = default;
    virtual ~TC_LOG() = default;

    enum enum_result { RESULT_SUCCESS, RESULT_ABORTED, RESULT_INCONSISTENT };

    virtual RC open() = 0;
    virtual RC close() = 0;

    // TODO 可能后续会加上 THD 参数
    // TODO 暂时不考虑 prepare, commit, rollback 函数
};

// 暂时不考虑 index 文件、lock
class MYSQL_BIN_LOG : TC_LOG {
  public:
    MYSQL_BIN_LOG(const char *file_name, uint64_t file_size, RC &rc);
    ~MYSQL_BIN_LOG() override = default;

  public:
    //********************* common file operation *************************
    RC open() override;  // 构造函数
    RC close() override; // 析构函数调用

    void flush() { m_binlog_file_->flush(); }

    //********************* file write operation *************************
    bool write_event_to_binlog(AbstractEvent *ev);

//    void add_bytes_written(loft::my_off_t inc) { bytes_written_ += inc; }
    bool remain_bytes_safe() {
        return m_binlog_file_->get_position() < max_size_ - WRITE_THRESHOLD;
    }
    uint64_t get_bytes_written() { return m_binlog_file_->get_position(); }

    void reset_bytes_written() { bytes_written_ = 0; }

    void update_binlog_end_pos(const char *file, my_off_t pos);

  private:
    enum enum_log_state_ { LOG_OPENED, LOG_CLOSED, LOG_TO_BE_OPENED };

    std::atomic<enum_log_state_> atomic_log_state_; // 描述文件打开状态

    char file_name_[FN_REFLEN]; // binlog 文件名
    // 当前 binlog file 写到一定大小时，触发写入 rotate event
    uint64_t max_size_; // binlog 文件最大大小

    my_off_t bytes_written_; // binlog 文件当前写入大小

    std::unique_ptr<Binlog_ofile> m_binlog_file_;
};

