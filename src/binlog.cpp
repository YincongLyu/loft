//
// Created by Coonger on 2024/10/17.
//
#include "binlog.h"
#include "constants.h"

MYSQL_BIN_LOG::MYSQL_BIN_LOG(const char *file_name, uint64_t file_size, RC &rc)
    : max_size_(file_size)
    , atomic_log_state_(LOG_CLOSED)
    , bytes_written_(0) {
    LOFT_ASSERT(file_name, "file_name is null");

    std::strncpy(file_name_, file_name, FN_REFLEN - 1);
    file_name_[FN_REFLEN - 1] = '\0'; // Null-terminate to prevent overflow

    rc = RC::FILE_CREATE;
}

RC MYSQL_BIN_LOG::open() {
    // Step 1: 打开文件流

    RC ret;
    m_binlog_file_ = std::make_unique<Binlog_ofile>(file_name_, ret);

    if (ret == RC::IOERR_OPEN) {
        atomic_log_state_ = LOG_CLOSED;
        LOG_ERROR("Failed to open binlog file.");
        return ret;
    }

    atomic_log_state_ = LOG_OPENED;

    // Step 2: 如果打开的是一个空文件，就会先写一个 magic number
    if (m_binlog_file_->is_empty()) {
        bool w_ok = m_binlog_file_->write(
            reinterpret_cast<const loft::uchar *>(BINLOG_MAGIC),
            BIN_LOG_HEADER_SIZE
        );

        if (!w_ok) {
            LOG_ERROR("Failed to write magic number to binlog.");
            return RC::IOERR_WRITE;
        }

        add_bytes_written(BIN_LOG_HEADER_SIZE);
    }

    return RC::FILE_OPEN;
}

RC MYSQL_BIN_LOG::close() {
    if (atomic_log_state_ == LOG_OPENED) {
        atomic_log_state_ = LOG_CLOSED;
    }

    reset_bytes_written();
    return RC::FILE_CLOSE;
}

bool MYSQL_BIN_LOG::write_event_to_binlog(AbstractEvent *ev) {
    return ev->write(this->m_binlog_file_.get());
}

void MYSQL_BIN_LOG::update_binlog_end_pos(
    const char *file, loft::my_off_t pos
) {
    // TODO
    return;
}
