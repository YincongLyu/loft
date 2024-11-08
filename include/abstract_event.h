//
// Created by Coonger on 2024/10/16.
//

#ifndef LOFT_ABSTRACT_EVENT_H
#define LOFT_ABSTRACT_EVENT_H

#include "basic_ostream.h"
#include <constants.h>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <sys/time.h>
#include <vector>
using namespace loft;

enum Log_event_type {
    /**
      Every time you add a type, you have to
      - Assign it a number explicitly. Otherwise it will cause trouble
        if a event type before is deprecated and removed directly from
        the enum.
      - Fix Format_description_event::Format_description_event().
    */
    UNKNOWN_EVENT = 0,
    /*
      Deprecated since mysql_helper 8.0.2. It is just a placeholder,
      should not be used anywhere else.
    */
    START_EVENT_V3 = 1,
    QUERY_EVENT = 2,
    STOP_EVENT = 3,
    ROTATE_EVENT = 4,
    INTVAR_EVENT = 5,

    SLAVE_EVENT = 7,

    APPEND_BLOCK_EVENT = 9,
    DELETE_FILE_EVENT = 11,

    RAND_EVENT = 13,
    USER_VAR_EVENT = 14,
    FORMAT_DESCRIPTION_EVENT = 15,
    XID_EVENT = 16,
    BEGIN_LOAD_QUERY_EVENT = 17,
    EXECUTE_LOAD_QUERY_EVENT = 18,

    TABLE_MAP_EVENT = 19,

    /**
      The V1 event numbers are used from 5.1.16 until mysql_helper-5.6.
    */
    WRITE_ROWS_EVENT_V1 = 23,
    UPDATE_ROWS_EVENT_V1 = 24,
    DELETE_ROWS_EVENT_V1 = 25,

    /**
      Something out of the ordinary happened on the master
     */
    INCIDENT_EVENT = 26,

    /**
      Heartbeat event to be send by master at its idle time
      to ensure master's online status to slave
    */
    HEARTBEAT_LOG_EVENT = 27,

    /**
      In some situations, it is necessary to send over ignorable
      data to the slave: data that a slave can handle in case there
      is code for handling it, but which can be ignored if it is not
      recognized.
    */
    IGNORABLE_LOG_EVENT = 28,
    ROWS_QUERY_LOG_EVENT = 29,

    /** Version 2 of the Row events */
    WRITE_ROWS_EVENT = 30,
    UPDATE_ROWS_EVENT = 31,
    DELETE_ROWS_EVENT = 32,

    GTID_LOG_EVENT = 33,
    ANONYMOUS_GTID_LOG_EVENT = 34,

    PREVIOUS_GTIDS_LOG_EVENT = 35,

    TRANSACTION_CONTEXT_EVENT = 36,

    VIEW_CHANGE_EVENT = 37,

    /* Prepared XA transaction terminal event similar to Xid */
    XA_PREPARE_LOG_EVENT = 38,

    /**
      Extension of UPDATE_ROWS_EVENT, allowing partial values according
      to binlog_row_value_options.
    */
    PARTIAL_UPDATE_ROWS_EVENT = 39,

    TRANSACTION_PAYLOAD_EVENT = 40,

    HEARTBEAT_LOG_EVENT_V2 = 41,
    /**
      Add new events here - right above this comment!
      Existing events (except ENUM_END_EVENT) should never change their numbers
    */
    ENUM_END_EVENT /* end marker */
};

enum enum_binlog_checksum_alg {
    BINLOG_CHECKSUM_ALG_OFF = 0,
    BINLOG_CHECKSUM_ALG_CRC32 = 1,
    BINLOG_CHECKSUM_ALG_ENUM_END,
    BINLOG_CHECKSUM_ALG_UNDEF = 255
};

class AbstractEvent;

class EventCommonHeader {
  public:
    explicit EventCommonHeader(Log_event_type type_code_arg = ENUM_END_EVENT)
        : type_code_(type_code_arg)
        , data_written_(0) // 不计算出来，mysqlbinlog 验证不通过
        , log_pos_(0)
        , flags_(0) { // 默认正常关闭
        when_.tv_sec = 0;
        when_.tv_usec = 0;
    }

  public:
    struct timeval when_{};
    Log_event_type type_code_;
    uint32_t unmasked_server_id_{};
    size_t data_written_;
    unsigned long long log_pos_;
    uint16_t flags_;
};

class EventCommonFooter {
  public:
    EventCommonFooter() : checksum_alg_(BINLOG_CHECKSUM_ALG_UNDEF) {}

    explicit EventCommonFooter(enum_binlog_checksum_alg checksum_alg_arg)
        : checksum_alg_(checksum_alg_arg) {}

  public:
    enum_binlog_checksum_alg checksum_alg_;
};

class AbstractEvent {
  public:
    static const int LOG_EVENT_TYPES = (ENUM_END_EVENT - 1);

    // 每个 event 的 post-header 长度
    enum enum_post_header_length {
        // where 3.23, 4.x and 5.0 agree
        QUERY_HEADER_MINIMAL_LEN = (4 + 4 + 1 + 2),
        // where 5.0 differs: 2 for length of N-bytes vars.
        QUERY_HEADER_LEN = (QUERY_HEADER_MINIMAL_LEN + 2),
        STOP_HEADER_LEN = 0,
        START_V3_HEADER_LEN = (2 + ST_SERVER_VER_LEN + 4),
        // this is FROZEN (the Rotate post-header is frozen)
        ROTATE_HEADER_LEN = 8,
        INTVAR_HEADER_LEN = 0,
        APPEND_BLOCK_HEADER_LEN = 4,
        DELETE_FILE_HEADER_LEN = 4,
        RAND_HEADER_LEN = 0,
        USER_VAR_HEADER_LEN = 0,
        FORMAT_DESCRIPTION_HEADER_LEN =
            (START_V3_HEADER_LEN + 1 + LOG_EVENT_TYPES),
        XID_HEADER_LEN = 0,
        BEGIN_LOAD_QUERY_HEADER_LEN = APPEND_BLOCK_HEADER_LEN,
        ROWS_HEADER_LEN_V1 = 8,
        TABLE_MAP_HEADER_LEN = 8,
        EXECUTE_LOAD_QUERY_EXTRA_HEADER_LEN = (4 + 4 + 4 + 1),
        EXECUTE_LOAD_QUERY_HEADER_LEN =
            (QUERY_HEADER_LEN + EXECUTE_LOAD_QUERY_EXTRA_HEADER_LEN),
        INCIDENT_HEADER_LEN = 2,
        HEARTBEAT_HEADER_LEN = 0,
        IGNORABLE_HEADER_LEN = 0,
        ROWS_HEADER_LEN_V2 = 10,
        TRANSACTION_CONTEXT_HEADER_LEN = 18,
        VIEW_CHANGE_HEADER_LEN = 52,
        XA_PREPARE_HEADER_LEN = 0,
        TRANSACTION_PAYLOAD_HEADER_LEN = 0,
    }; // end enum_post_header_length

    explicit AbstractEvent(Log_event_type type_code) { type_code_ = type_code; }

    enum Log_event_type get_type_code() { return type_code_; }

    unsigned long get_time();

    virtual ~AbstractEvent() = 0;

    AbstractEvent(const AbstractEvent &) = default;
    AbstractEvent(AbstractEvent &&) = default;
    AbstractEvent &operator=(const AbstractEvent &) = default;
    AbstractEvent &operator=(AbstractEvent &&) = default;

    uint32_t write_common_header_to_memory(uchar *buf);
    bool write_common_header(Basic_ostream *ostream, size_t event_data_length);
    bool write_common_footer(Basic_ostream *ostream);

    /*
        - 对于复杂的 event，event_data_size 在 实现 具体的 write() 时 计算
            - Format_description_event
        - 对于简单的 event，可以调用 event_data_size()
            - Rows_log_event::get_data_size()
    */
    virtual size_t get_data_size() { return 0; }

    //    virtual bool write(Basic_ostream *ostream) = 0;

    virtual bool write(Basic_ostream *ostream) {
        return
            write_common_header(ostream, get_data_size())
            && write_data_header(ostream) && write_data_body(ostream);

    }

    /*
        - Rows_log_event
        - Table_map_log_event
        - Gtid_log_event
     */
    virtual bool write_data_header(Basic_ostream *) { return true; }

    /*
        - Rows_log_event
        - Table_map_log_event
        - Previous_gtids_log_event
        - Gtid_log_event
     */

    virtual bool write_data_body(Basic_ostream *) { return true; }

    LEX_CSTRING get_invoker_user() {
        return {"", 0};
    }
    LEX_CSTRING get_invoker_host() {
        std::string host = "127.0.0.1";
        return {host.c_str(), host.length()};
    }

  public:
    EventCommonHeader *common_header_;
    EventCommonFooter *common_footer_;

    enum Log_event_type type_code_ = UNKNOWN_EVENT;
    uint32_t server_id_ = 1000; // 配置项读入
    /* The number of seconds the query took to run on the master. */
    ulong exec_time_ = 2;
    uint32_t slave_proxy_id_ = 10000;
    bool query_start_usec_used_ = true;
//    uint16_t p_default_collation_for_utf8mb4_number_ = 255;
    bool need_binlog_invoker_ = true;
};

#endif // LOFT_ABSTRACT_EVENT_H
