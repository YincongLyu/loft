//
// Created by Coonger on 2024/10/20.
//
#include "statement_events.h"
#include "abstract_event.h"
#include "little_endian.h"
#include "template_utils.h"

#include "logging.h"

/******************************************************************************
                     Query_event methods
******************************************************************************/

/**
  The constructor used by MySQL master to create a query event, to be
  written to the binary log.
*/
Query_event::Query_event(
    const char *query_arg,
    const char *catalog_arg,
    const char *db_arg,
    uint64_t ddl_xid_arg,
    uint32_t query_length,
    unsigned long thread_id_arg,
    unsigned long long sql_mode_arg,
    unsigned long auto_increment_increment_arg,
    unsigned long auto_increment_offset_arg,
    unsigned int number,
    unsigned long long table_map_for_update_arg,
    int errcode
)
    : AbstractEvent(QUERY_EVENT)
    , query_(query_arg)
    , db_(db_arg)
    , ddl_xid(ddl_xid_arg)
    , catalog_(catalog_arg)
    , user_(nullptr)
    , user_len_(0)
    , host_(nullptr)
    , host_len_(0)
    , thread_id_(thread_id_arg)
    , db_len_(0)
    , error_code_(errcode)
    , status_vars_len_(0)
    , q_len_(query_length)
    , flags2_inited(true)
    , sql_mode_inited(true)
    , charset_inited(true)
    , sql_mode(sql_mode_arg)
    , auto_increment_increment(
          static_cast<uint16_t>(auto_increment_increment_arg)
      )
    , auto_increment_offset(static_cast<uint16_t>(auto_increment_offset_arg))
    , time_zone_len(0)
    , lc_time_names_number(number)
    , charset_database_number(0)
    , table_map_for_update(table_map_for_update_arg)
    , explicit_defaults_ts(TERNARY_UNSET)
    , mts_accessed_dbs(0)
    , default_collation_for_utf8mb4_number_(255)
    , sql_require_primary_key(0xff)
    , default_table_encryption(0xff) {

//    time_zone_str_ = TIME_ZONE;
//    time_zone_len = strlen(time_zone_str_);
    if (db_arg == nullptr) {
        db_len_ = 0;
    } else {
        db_len_ = strlen(db_arg);
    }

//    catalog_len = db_len_;
    catalog_len = 0;
    query_exec_time_ = EXEC_TIME;
    LOG_INFO("db_len_ = %zu, query_len = %zu", db_len_, q_len_);

    this->common_header_ = new EventCommonHeader();
    //    this->common_footer_ = new EventCommonFooter(BINLOG_CHECKSUM_ALG_OFF);
}

/**
  Utility function for the next method (Query_log_event::write()) .
*/
static void write_str_with_code_and_len(
    uchar **dst, const char *src, size_t len, uint code
) {
    /*
      only 1 byte to store the length of catalog, so it should not
      surpass 255
    */
    assert(len <= 255);
    assert(src);
    *((*dst)++) = code;
    *((*dst)++) = (uchar)len;
    memmove(*dst, src, len);
    (*dst) += len;
}

/**
  Query_log_event::write().

@note
        In this event we have to modify the header to have the correct
            EVENT_LEN_OFFSET as we don't yet know how many status variables we
    will print!
    */

bool Query_event::write(Basic_ostream *ostream) {
    uchar buf[AbstractEvent::QUERY_HEADER_LEN + MAX_SIZE_LOG_EVENT_STATUS];
    uchar *start, *start_of_status;
    size_t event_length;

    if (!query_) {
        return true; // Something wrong with event
    }

    /*
        thread id 的设置和 session 有关，又和 create 临时表有关
        这里不用考虑
    */
    int4store(buf + Q_THREAD_ID_OFFSET, thread_id_);
    int4store(buf + Q_EXEC_TIME_OFFSET, query_exec_time_);
    buf[Q_DB_LEN_OFFSET] = (unsigned char)db_len_;
    int2store(buf + Q_ERR_CODE_OFFSET, error_code_);

    /*
      You MUST always write status vars in increasing order of code. This
      guarantees that a slightly older slave will be able to parse those he
      knows.
    */
    start_of_status = start = buf + AbstractEvent::QUERY_HEADER_LEN;

    if (ddl_xid == INVALID_XID) {
        goto cal_status_var;
    }

    if (flags2_inited) {
        *start++ = Q_FLAGS2_CODE;
        int4store(start, flags2);
        start += 4;
    }
    if (sql_mode_inited) {
        *start++ = Q_SQL_MODE_CODE;
        int8store(start, sql_mode);
        start += 8;
    }
    if (catalog_len) // i.e. this var is inited (false for 4.0 events)
    {
        write_str_with_code_and_len(
            &start, catalog_, catalog_len, Q_CATALOG_NZ_CODE
        );
    }

    if (auto_increment_increment != 1 || auto_increment_offset != 1) {
        *start++ = Q_AUTO_INCREMENT;
        int2store(start, static_cast<uint16_t>(auto_increment_increment));
        int2store(start + 2, static_cast<uint16_t>(auto_increment_offset));
        start += 4;
    }
    if (charset_inited) {
        *start++ = Q_CHARSET_CODE;
        // 使用标准的字符集ID
        int2store(start, client_charset_); // 比如 utf8mb4 = 45
        int2store(
            start + 2, connection_collation_
        );                                       // 比如 utf8mb4_general_ci = 45
        int2store(start + 4, server_collation_); // 服务器默认字符集排序规则
        //        memcpy(start, charset, 6);
        start += 6;
    }
    if (time_zone_len) {
        /* In the TZ sys table, column Name is of length 64 so this should be ok
         */
        assert(time_zone_len <= MAX_TIME_ZONE_NAME_LENGTH);
        write_str_with_code_and_len(
            &start, time_zone_str_, time_zone_len, Q_TIME_ZONE_CODE
        );
    }
    if (lc_time_names_number) {
        assert(lc_time_names_number <= 0xFF);
        *start++ = Q_LC_TIME_NAMES_CODE;
        int2store(start, lc_time_names_number);
        start += 2;
    }
    if (charset_database_number) {
        *start++ = Q_CHARSET_DATABASE_CODE;
        int2store(start, charset_database_number);
        start += 2;
    }
    if (table_map_for_update) {
        *start++ = Q_TABLE_MAP_FOR_UPDATE_CODE;
        int8store(start, table_map_for_update);
        start += 8;
    }

//    if (need_binlog_invoker_) {
//        LEX_CSTRING invoker_user{nullptr, 0};
//        LEX_CSTRING invoker_host{nullptr, 0};
//        memset(&invoker_user, 0, sizeof(invoker_user));
//        memset(&invoker_host, 0, sizeof(invoker_host));
//
//        invoker_user = get_invoker_user();
//        invoker_host = get_invoker_host();
//
//        *start++ = Q_INVOKER;
//
//        /*
//          Store user length and user. The max length of use is 16, so 1 byte is
//          enough to store the user's length.
//         */
//        *start++ = (uchar)invoker_user.length;
//        memcpy(start, invoker_user.str, invoker_user.length);
//        start += invoker_user.length;
//
//        /*
//          Store host length and host. The max length of host is 255, so 1 byte
//          is enough to store the host's length.
//         */
//        *start++ = (uchar)invoker_host.length;
//        if (invoker_host.length > 0) {
//            memcpy(start, invoker_host.str, invoker_host.length);
//        }
//        start += invoker_host.length;
//    }

    // *****************db name ******************

    if (db_ != nullptr) {
        *start++ = Q_UPDATED_DB_NAMES;
        uchar dbs = 254;
        *start++ = dbs; // 写入数据库数量，现指定为1，实际上要从外边读入
        const char *db_name = db_;
        strcpy((char *)start, db_name);
        start +=
            strlen(db_name) + 1; // 写入数据库名，并在末尾添加 '\0' 作为结束符
    } else {
        *start++ = 254; // 空数据库，只能是 create db，
                        // 默认填充OVER_MAX_DBS_IN_EVENT_MTS
    }

    // 强制记录 时间戳
    if (query_start_usec_used_) {
        *start++ = Q_MICROSECONDS;
        int3store(start, common_header_->when_.tv_usec);
        start += 3;
    }

    if (ddl_xid != INVALID_XID) {
        *start++ = Q_DDL_LOGGED_WITH_XID;
        int8store(start, ddl_xid);
        start += 8;
    }

    if (default_collation_for_utf8mb4_number_) {
        assert(default_collation_for_utf8mb4_number_ <= 0xFF);
        *start++ = Q_DEFAULT_COLLATION_FOR_UTF8MB4;
        int2store(start, default_collation_for_utf8mb4_number_);
        start += 2;
    }

    goto cal_status_var;

cal_status_var:
    /* Store length of status variables */
    status_vars_len_ = static_cast<uint>(start - start_of_status);
    assert(status_vars_len_ <= MAX_SIZE_LOG_EVENT_STATUS);
    int2store(buf + Q_STATUS_VARS_LEN_OFFSET, status_vars_len_);

    /*
      get_post_header_size_for_derived() 不考虑 load event
    */

    event_length = static_cast<uint>(start - buf) + db_len_ + 1 + q_len_;

    return write_common_header(ostream, event_length)
           && ostream->write((uchar *)buf, AbstractEvent::QUERY_HEADER_LEN)
           && ostream->write(start_of_status, (uint)(start - start_of_status))
           && ostream->write(
               db_ ? pointer_cast<const uchar *>(db_)
                   : pointer_cast<const uchar *>(""),
               db_len_ + 1
           )
           && ostream->write(pointer_cast<const uchar *>(query_), q_len_);
}
