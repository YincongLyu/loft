//
// Created by Coonger on 2024/10/17.
//

#ifndef LOFT_BINLOG_H
#define LOFT_BINLOG_H

#include "abstract_event.h"
#include "basic_ostream.h"
#include "constants.h"
#include "control_events.h"
#include <atomic>

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

    virtual bool open(const char *file_name, uint64_t file_size) = 0;
    virtual void close() = 0;

    // TODO 可能后续会加上 THD 参数
    // TODO 暂时不考虑 prepare, commit, rollback 函数
};

// 暂时不考虑 index 文件、lock
class MYSQL_BIN_LOG : TC_LOG {
  public:
    explicit MYSQL_BIN_LOG(Basic_ostream *ostream)
        : m_binlog_file_(dynamic_cast<Binlog_ofile *>(ostream)) {}

    ~MYSQL_BIN_LOG() override = default;

  private:
    enum enum_log_state_ { LOG_OPENED, LOG_CLOSED, LOG_TO_BE_OPENED };

    // 描述文件打开状态
    std::atomic<enum_log_state_> atomic_log_state{LOG_CLOSED};
    // binlog 文件名
    char log_file_name_[FN_REFLEN];
    // 当前 binlog file 写到一定大小时，触发写入 rotate event
    uint64_t max_size_arg;
    loft::my_off_t bytes_written_{0};

    char db_[NAME_LEN + 1];
    // 写 binlog 的输出流
    Binlog_ofile *m_binlog_file_;

  public:
    //********************* common file operation *************************
    bool open(const char *file_name, uint64_t file_size) override; // 构造函数
    void close() override;                                         // 析构函数

    // 告诉编译器，这个变量很重要
    [[nodiscard]]
    bool is_open() const {
        return atomic_log_state != LOG_CLOSED;
    }

    /*
     * 在 master 上生成 binlog，Format_description_log_event 是
     * null，所以这里直接去掉 fde 参数 若是 slave 上根据 master 的binlog 生成的
     * relay log，则不为 null
     */
    int create_file(const char *file_name, uint64_t file_size);

    //********************* file write operation *************************
    bool write_event_to_binlog(AbstractEvent *ev);

    void add_bytes_written(loft::my_off_t inc) { bytes_written_ += inc; }

    void reset_bytes_written() { bytes_written_ = 0; }

    void update_binlog_end_pos(const char *file, loft::my_off_t pos);
};

#endif // LOFT_BINLOG_H
