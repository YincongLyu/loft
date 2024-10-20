//
// Created by Coonger on 2024/10/17.
//
#include "binlog.h"
#include "constants.h"

// 只是 init 了 当前 binlog 实例的 file_name，直到 open 的时候再根据 init 的
// log_flie_name_ 创建文件流
int MYSQL_BIN_LOG::create_file(const char *file_name, uint64_t file_size) {
    max_size_arg = file_size;

    // Ensure the file name is set properly and within limits
    if (file_name) {
        std::strncpy(log_file_name_, file_name, FN_REFLEN - 1);
        log_file_name_[FN_REFLEN - 1] =
            '\0'; // Null-terminate to prevent overflow

        // Instantiate the m_binlog_file_ object
        if (!m_binlog_file_) {
            m_binlog_file_ = new Binlog_ofile();
        }
        return 0; // Success
    }
    return -1; // Failure to set the file name
}

// 默认打开的就是一个空文件, 就会先写 magic number
bool MYSQL_BIN_LOG::open(const char *file_name, uint64_t file_size) {
    // Step 1: Create the binlog file and initialize member variables
    int create_result = create_file(file_name, file_size);
    if (create_result != 0) {
        std::cerr << "Failed to create binlog file." << std::endl;
        return false;
    }

    // Instantiate the m_binlog_file_ object if not already done
    if (!m_binlog_file_) {
        m_binlog_file_ = new Binlog_ofile();
    }

    // Step 2: Attempt to open the binlog file using the Binlog_ofile object
    bool ret = m_binlog_file_->open(log_file_name_);
    if (ret) {
        atomic_log_state = LOG_OPENED;
    } else {
        std::cerr << "Failed to open binlog file: " << log_file_name_
                  << std::endl;
        atomic_log_state = LOG_CLOSED;
        return false;
    }

    // Step 3: Write the magic number if the file is empty
    if (m_binlog_file_->is_empty()) {
        if (m_binlog_file_->write(
                reinterpret_cast<const loft::uchar *>(BINLOG_MAGIC),
                BIN_LOG_HEADER_SIZE
            )) {
            bytes_written_ += BIN_LOG_HEADER_SIZE;
        } else {
            std::cerr << "Failed to write magic number to binlog." << std::endl;
            return false;
        }
    }

    return true;
}

void MYSQL_BIN_LOG::close() {
    if (atomic_log_state == LOG_OPENED) {
        // Close the file if it's currently open
        if (m_binlog_file_) {
            m_binlog_file_->close();
            delete m_binlog_file_;
            m_binlog_file_ = nullptr;
        }
        atomic_log_state = LOG_CLOSED;
    }
    // Reset internal tracking states
    reset_bytes_written();
}

bool MYSQL_BIN_LOG::write_event_to_binlog(AbstractEvent *ev) {
    return ev->write(this->m_binlog_file_);
}

void MYSQL_BIN_LOG::update_binlog_end_pos(
    const char *file, loft::my_off_t pos
) {
    // TODO
    return;
}
