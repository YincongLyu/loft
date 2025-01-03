//
// Created by Coonger on 2024/10/20.
//
#include "events/statement_events.h"
#include "events/abstract_event.h"

#include "common/logging.h"

#include "utils/little_endian.h"
#include "utils/template_utils.h"

/******************************************************************************
                     Query_event methods
******************************************************************************/

Query_event::Query_event(const char *query_arg, const char *catalog_arg, const char *db_arg, uint64 ddl_xid_arg,
    uint32 query_length, uint64 thread_id_arg, uint64 sql_mode_arg, uint64 auto_increment_increment_arg,
    uint64 auto_increment_offset_arg, uint32 number, uint64 table_map_for_update_arg, int32 errcode,
    uint64 immediate_commit_timestamp_arg)
    : AbstractEvent(QUERY_EVENT),
      query_(query_arg),
      db_(db_arg),
      ddl_xid(ddl_xid_arg),
      catalog_(catalog_arg),
      user_(nullptr),
      user_len_(0),
      host_(nullptr),
      host_len_(0),
      thread_id_(thread_id_arg),
      db_len_(0),
      error_code_(errcode),
      status_vars_len_(0),
      q_len_(query_length),
      flags2_inited(true),
      sql_mode_inited(true),
      charset_inited(true),
      sql_mode(sql_mode_arg),
      auto_increment_increment(static_cast<uint16_t>(auto_increment_increment_arg)),
      auto_increment_offset(static_cast<uint16_t>(auto_increment_offset_arg)),
      time_zone_len(0),
      lc_time_names_number(number),
      charset_database_number(0),
      table_map_for_update(table_map_for_update_arg),
      explicit_defaults_ts(TERNARY_UNSET),
      mts_accessed_dbs(0),
      default_collation_for_utf8mb4_number_(255),
      sql_require_primary_key(0xff),
      default_table_encryption(0xff)
{
  //    time_zone_str_ = TIME_ZONE;
  //    time_zone_len = strlen(time_zone_str_);
  if (db_arg == nullptr) {
    db_len_ = 0;
  } else {
    db_len_ = strlen(db_arg);
  }

  //    catalog_len = db_len_;
  catalog_len      = 0;
  query_exec_time_ = EXEC_TIME;
  LOG_INFO("db_len_ = %zu, query_len = %zu", db_len_, q_len_);

  calculate_status_vars_len();

  time_t i_ts          = static_cast<time_t>(immediate_commit_timestamp_arg / 1000000);
  this->common_header_ = std::make_unique<EventCommonHeader>(i_ts);
  //  this->common_header_ = std::make_unique<EventCommonHeader>(immediate_commit_timestamp_arg);
  //    this->common_footer_ = new EventCommonFooter(BINLOG_CHECKSUM_ALG_OFF);
}

/**
  Utility function for the next method (Query_log_event::write()) .
*/
static void write_str_with_code_and_len(uchar **dst, const char *src, size_t len, uint code)
{
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

bool Query_event::write(Basic_ostream *ostream)
{
  uchar  buf[AbstractEvent::QUERY_HEADER_LEN + MAX_SIZE_LOG_EVENT_STATUS];
  uchar *start, *start_of_status;
  size_t event_length;

  if (!query_) {
    return true;  // Something wrong with event
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
  if (catalog_len)  // i.e. this var is inited (false for 4.0 events)
  {
    write_str_with_code_and_len(&start, catalog_, catalog_len, Q_CATALOG_NZ_CODE);
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
    int2store(start, client_charset_);            // 比如 utf8mb4 = 45
    int2store(start + 2, connection_collation_);  // 比如 utf8mb4_general_ci = 45
    int2store(start + 4, server_collation_);      // 服务器默认字符集排序规则
    //        memcpy(start, charset, 6);
    start += 6;
  }
  if (time_zone_len) {
    /* In the TZ sys table, column Name is of length 64 so this should be ok
     */
    assert(time_zone_len <= MAX_TIME_ZONE_NAME_LENGTH);
    write_str_with_code_and_len(&start, time_zone_str_, time_zone_len, Q_TIME_ZONE_CODE);
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

  // *****************db name ******************

  if (db_ != nullptr) {
    *start++            = Q_UPDATED_DB_NAMES;
    uchar dbs           = 254;
    *start++            = dbs;  // 写入数据库数量，现指定为1，实际上要从外边读入
    const char *db_name = db_;
    strcpy((char *)start, db_name);
    start += strlen(db_name) + 1;  // 写入数据库名，并在末尾添加 '\0' 作为结束符
  } else {
    *start++ = 254;  // 空数据库，只能是 create db，
                     // 默认填充OVER_MAX_DBS_IN_EVENT_MTS
  }

  // 强制记录 时间戳
  if (query_start_usec_used_) {
    *start++ = Q_MICROSECONDS;
    int3store(start, common_header_->timestamp_ % 1000000);  // 提取微秒部分, 不用 struct timeval 存了
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

  return write_common_header(ostream, event_length) && ostream->write((uchar *)buf, AbstractEvent::QUERY_HEADER_LEN) &&
         ostream->write(start_of_status, (uint)(start - start_of_status)) &&
         ostream->write(db_ ? pointer_cast<const uchar *>(db_) : pointer_cast<const uchar *>(""), db_len_ + 1) &&
         ostream->write(pointer_cast<const uchar *>(query_), q_len_);
}

size_t Query_event::write_data_header_to_buffer(uchar *buffer)
{
  // 写入 Query 事件固定头部
  int4store(buffer + Q_THREAD_ID_OFFSET, thread_id_);
  int4store(buffer + Q_EXEC_TIME_OFFSET, query_exec_time_);
  buffer[Q_DB_LEN_OFFSET] = (unsigned char)db_len_;
  int2store(buffer + Q_ERR_CODE_OFFSET, error_code_);

  return AbstractEvent::QUERY_HEADER_LEN;
}

size_t Query_event::write_data_body_to_buffer(uchar *buffer)
{
  uchar *current_pos     = buffer;
  uchar *start_of_status = current_pos;

  // 写入状态变量
  if (ddl_xid != INVALID_XID) {
    if (flags2_inited) {
      *current_pos++ = Q_FLAGS2_CODE;
      int4store(current_pos, flags2);
      current_pos += 4;
    }
    if (sql_mode_inited) {
      *current_pos++ = Q_SQL_MODE_CODE;
      int8store(current_pos, sql_mode);
      current_pos += 8;
    }
    if (catalog_len) {
      write_str_with_code_and_len(&current_pos, catalog_, catalog_len, Q_CATALOG_NZ_CODE);
    }

    if (auto_increment_increment != 1 || auto_increment_offset != 1) {
      *current_pos++ = Q_AUTO_INCREMENT;
      int2store(current_pos, static_cast<uint16_t>(auto_increment_increment));
      int2store(current_pos + 2, static_cast<uint16_t>(auto_increment_offset));
      current_pos += 4;
    }
    if (charset_inited) {
      *current_pos++ = Q_CHARSET_CODE;
      int2store(current_pos, client_charset_);
      int2store(current_pos + 2, connection_collation_);
      int2store(current_pos + 4, server_collation_);
      current_pos += 6;
    }
    if (time_zone_len) {
      write_str_with_code_and_len(&current_pos, time_zone_str_, time_zone_len, Q_TIME_ZONE_CODE);
    }
    if (lc_time_names_number) {
      *current_pos++ = Q_LC_TIME_NAMES_CODE;
      int2store(current_pos, lc_time_names_number);
      current_pos += 2;
    }
    if (charset_database_number) {
      *current_pos++ = Q_CHARSET_DATABASE_CODE;
      int2store(current_pos, charset_database_number);
      current_pos += 2;
    }
    if (table_map_for_update) {
      *current_pos++ = Q_TABLE_MAP_FOR_UPDATE_CODE;
      int8store(current_pos, table_map_for_update);
      current_pos += 8;
    }

    // 写入数据库名
    if (db_ != nullptr) {
      *current_pos++ = Q_UPDATED_DB_NAMES;
      *current_pos++ = 254;  // 数据库数量
      strcpy((char *)current_pos, db_);
      current_pos += strlen(db_) + 1;
    } else {
      *current_pos++ = 254;
    }

    if (query_start_usec_used_) {
      *current_pos++ = Q_MICROSECONDS;
      int3store(current_pos, common_header_->timestamp_ % 1000000);
      current_pos += 3;
    }

    if (ddl_xid != INVALID_XID) {
      *current_pos++ = Q_DDL_LOGGED_WITH_XID;
      int8store(current_pos, ddl_xid);
      current_pos += 8;
    }

    if (default_collation_for_utf8mb4_number_) {
      *current_pos++ = Q_DEFAULT_COLLATION_FOR_UTF8MB4;
      int2store(current_pos, default_collation_for_utf8mb4_number_);
      current_pos += 2;
    }
  }

  // 更新状态变量长度
  status_vars_len_ = current_pos - start_of_status;
  int2store(buffer - AbstractEvent::QUERY_HEADER_LEN + Q_STATUS_VARS_LEN_OFFSET, status_vars_len_);

  // 写入数据库名
  if (db_) {
    memcpy(current_pos, db_, db_len_);
  }
  current_pos += db_len_;
  *current_pos++ = 0;  // 数据库名结束符

  // 写入查询语句
  memcpy(current_pos, query_, q_len_);
  current_pos += q_len_;

  return current_pos - buffer;
}

void Query_event::calculate_status_vars_len()
{
  size_t len = 0;

  if (ddl_xid != INVALID_XID) {
    if (flags2_inited)
      len += 1 + 4;
    if (sql_mode_inited)
      len += 1 + 8;
    if (catalog_len)
      len += 1 + catalog_len;
    if (auto_increment_increment != 1 || auto_increment_offset != 1)
      len += 1 + 4;
    if (charset_inited)
      len += 1 + 6;
    if (time_zone_len)
      len += 1 + time_zone_len;
    if (lc_time_names_number)
      len += 1 + 2;
    if (charset_database_number)
      len += 1 + 2;
    if (table_map_for_update)
      len += 1 + 8;
    if (db_)
      len += 2 + strlen(db_) + 1;
    else
      len += 1;
    if (query_start_usec_used_)
      len += 1 + 3;
    if (ddl_xid != INVALID_XID)
      len += 1 + 8;
    if (default_collation_for_utf8mb4_number_)
      len += 1 + 2;
  }

  status_vars_len_ = len;
}
