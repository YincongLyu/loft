// COPY from 'mysql/libbinlogevents/include/statement_events.h'

#ifndef STATEMENT_EVENT_INCLUDED
#define STATEMENT_EVENT_INCLUDED

#include "../constants.h"
#include "control_events.h"
// #include "mysql/udf_registration_types.h"

namespace binary_log {

const uint64_t INVALID_XID = 0xffffffffffffffffULL;

class Query_event : public Binary_log_event {
 public:
  /** query event post-header */
  enum Query_event_post_header_offset {
    Q_THREAD_ID_OFFSET = 0,
    Q_EXEC_TIME_OFFSET = 4,
    Q_DB_LEN_OFFSET = 8,
    Q_ERR_CODE_OFFSET = 9,
    Q_STATUS_VARS_LEN_OFFSET = 11,
    Q_DATA_OFFSET = QUERY_HEADER_LEN
  };

  /* these are codes, not offsets; not more than 256 values (1 byte). */
  enum Query_event_status_vars {
    Q_FLAGS2_CODE = 0,
    Q_SQL_MODE_CODE,
    /*
      Q_CATALOG_CODE is catalog with end zero stored; it is used only by MySQL
      5.0.x where 0<=x<=3. We have to keep it to be able to replicate these
      old masters.
    */
    Q_CATALOG_CODE,
    Q_AUTO_INCREMENT,
    Q_CHARSET_CODE,
    Q_TIME_ZONE_CODE,
    /*
      Q_CATALOG_NZ_CODE is catalog withOUT end zero stored; it is used by MySQL
      5.0.x where x>=4. Saves one byte in every Query_event in binlog,
      compared to Q_CATALOG_CODE. The reason we didn't simply re-use
      Q_CATALOG_CODE is that then a 5.0.3 slave of this 5.0.x (x>=4)
      master would crash (segfault etc) because it would expect a 0 when there
      is none.
    */
    Q_CATALOG_NZ_CODE,
    Q_LC_TIME_NAMES_CODE,
    Q_CHARSET_DATABASE_CODE,
    Q_TABLE_MAP_FOR_UPDATE_CODE,
    /* It is just a placeholder after 8.0.2*/
    Q_MASTER_DATA_WRITTEN_CODE,
    Q_INVOKER,
    /*
      Q_UPDATED_DB_NAMES status variable collects information of accessed
      databases i.e. the total number and the names to be propagated to the
      slave in order to facilitate the parallel applying of the Query events.
    */
    Q_UPDATED_DB_NAMES,
    Q_MICROSECONDS,
    /*
     A old (unused now) code for Query_log_event status similar to G_COMMIT_TS.
   */
    Q_COMMIT_TS,
    /*
     An old (unused after migration to Gtid_event) code for
     Query_log_event status, similar to G_COMMIT_TS2.
   */
    Q_COMMIT_TS2,
    /*
      The master connection @@session.explicit_defaults_for_timestamp which
      is recorded for queries, CREATE and ALTER table that is defined with
      a TIMESTAMP column, that are dependent on that feature.
      For pre-WL6292 master's the associated with this code value is zero.
    */
    Q_EXPLICIT_DEFAULTS_FOR_TIMESTAMP,
    /*
      The variable carries xid info of 2pc-aware (recoverable) DDL queries.
    */
    Q_DDL_LOGGED_WITH_XID,
    /*
      This variable stores the default collation for the utf8mb4 character set.
      Used to support cross-version replication.
    */
    Q_DEFAULT_COLLATION_FOR_UTF8MB4,

    /*
      Replicate sql_require_primary_key.
    */
    Q_SQL_REQUIRE_PRIMARY_KEY,

    /*
      Replicate default_table_encryption.
    */
    Q_DEFAULT_TABLE_ENCRYPTION
  };
  const char *query;
  const char *db;
  const char *catalog;
  const char *time_zone_str;

 protected:
  const char *user;
  size_t user_len;
  const char *host;
  size_t host_len;

  /* Required by the MySQL server class Log_event::Query_event */
  unsigned long data_len;
  /*
    Copies data into the buffer in the following fashion
    +--------+-----------+------+------+---------+----+-------+----+
    | catlog | time_zone | user | host | db name | \0 | Query | \0 |
    +--------+-----------+------+------+---------+----+-------+----+
  */
  int fill_data_buf(unsigned char *dest, unsigned long len);

 public:
  /* data members defined in order they are packed and written into the log */
  uint32_t thread_id;
  uint32_t query_exec_time;
  size_t db_len;
  uint16_t error_code;

  uint16_t status_vars_len;

  size_t q_len;

  bool flags2_inited;
  bool sql_mode_inited;
  bool charset_inited;

  uint32_t flags2;
  /* In connections sql_mode is 32 bits now but will be 64 bits soon */
  uint64_t sql_mode;
  uint16_t auto_increment_increment, auto_increment_offset;
  char charset[6];
  size_t time_zone_len; /* 0 means uninited */
  /*
    Binlog format 3 and 4 start to differ (as far as class members are
    concerned) from here.
  */
  size_t catalog_len;            // <= 255 char; 0 means uninited
  uint16_t lc_time_names_number; /* 0 means en_US */
  uint16_t charset_database_number;
  /*
    map for tables that will be updated for a multi-table update query
    statement, for other query statements, this will be zero.
  */
  uint64_t table_map_for_update;

  enum enum_ternary {
    TERNARY_UNSET,
    TERNARY_OFF,
    TERNARY_ON
  } explicit_defaults_ts;
  /*
    number of updated databases by the query and their names. This info
    is requested by both Coordinator and Worker.
  */
  unsigned char mts_accessed_dbs;
  char mts_accessed_db_names[MAX_DBS_IN_EVENT_MTS][NAME_LEN];
  /* XID value when the event is a 2pc-capable DDL */
  uint64_t ddl_xid;
  /* Default collation for the utf8mb4 set. Used in cross-version replication */
  uint16_t default_collation_for_utf8mb4_number;
  uint8_t sql_require_primary_key;

  uint8_t default_table_encryption;

  /**
    The constructor will be used while creating a Query_event, to be
    written to the binary log. 
    [!!unused]
  */
  Query_event(const char *query_arg, const char *catalog_arg,
              const char *db_arg, uint32_t query_length,
              unsigned long thread_id_arg, unsigned long long sql_mode_arg,
              unsigned long auto_increment_increment_arg,
              unsigned long auto_increment_offset_arg, unsigned int number,
              unsigned long long table_map_for_update_arg, int errcode);

  /**
    The constructor receives a buffer and instantiates a Query_event filled in
    with the data from the buffer

    <pre>
    The fixed event data part buffer layout is as follows:
    +---------------------------------------------------------------------+
    | thread_id | query_exec_time | db_len | error_code | status_vars_len |
    +---------------------------------------------------------------------+
    </pre>

    <pre>
    The fixed event data part buffer layout is as follows:
    +--------------------------------------------+
    | Zero or more status variables | db | query |
    +--------------------------------------------+
    </pre>

    @param event_type  Required to determine whether the event type is
                       QUERY_EVENT or EXECUTE_LOAD_QUERY_EVENT
  */
  Query_event(const char *buf, const Format_description_event *fde,
              Log_event_type event_type);
  /**
    The simplest constructor that could possibly work.  This is used for
    creating static objects that have a special meaning and are invisible
    to the log.
  */
  Query_event(Log_event_type type_arg = QUERY_EVENT);
  ~Query_event() override = default;

};

/*
  Check how many bytes are available on buffer.

  @param buf_start    Pointer to buffer start.
  @param buf_current  Pointer to the current position on buffer.
  @param buf_len      Buffer length.

  @return             Number of bytes available on event buffer.
*/
template <class T>
T available_buffer(const char *buf_start, const char *buf_current, T buf_len) {
  /* Sanity check */
  if (buf_current < buf_start ||
      buf_len < static_cast<T>(buf_current - buf_start))
    return static_cast<T>(0);

  return static_cast<T>(buf_len - (buf_current - buf_start));
}

/**
  Check if jump value is within buffer limits.

  @param jump         Number of positions we want to advance.
  @param buf_start    Pointer to buffer start
  @param buf_current  Pointer to the current position on buffer.
  @param buf_len      Buffer length.

  @retval      True   If jump value is within buffer limits.
  @retval      False  Otherwise.
*/
template <class T>
bool valid_buffer_range(T jump, const char *buf_start, const char *buf_current,
                        T buf_len) {
  return (jump <= available_buffer(buf_start, buf_current, buf_len));
}


}  // end namespace binary_log
/**
  @} (end of group Replication)
*/
#endif /* STATEMENT_EVENTS_INCLUDED */
