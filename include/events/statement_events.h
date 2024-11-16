//
// Created by Coonger on 2024/10/20.
//

#pragma once

#include "events/abstract_event.h"

#include "common/macros.h"
#include "common/init_setting.h"

class Query_event : public AbstractEvent
{

public:
  Query_event(const char *query_arg, const char *catalog_arg, const char *db_arg, uint64 ddl_xid_arg,
      uint32 query_length, uint64 thread_id_arg, uint64 sql_mode_arg, uint64 auto_increment_increment_arg,
      uint64 auto_increment_offset_arg, uint32 number, uint64 table_map_for_update_arg, int32 errcode,
      uint64 immediate_commit_timestamp_arg);

  ~Query_event() override = default;

  DISALLOW_COPY(Query_event);

  bool write(Basic_ostream *ostream) override;

public:
  /** query event post-header */
  enum Query_event_post_header_offset
  {
    Q_THREAD_ID_OFFSET       = 0,
    Q_EXEC_TIME_OFFSET       = 4,
    Q_DB_LEN_OFFSET          = 8,
    Q_ERR_CODE_OFFSET        = 9,
    Q_STATUS_VARS_LEN_OFFSET = 11,
    Q_DATA_OFFSET            = QUERY_HEADER_LEN
  };

  /* these are codes, not offsets; not more than 256 values (1 byte). */
  // 和 event-body 有关
  enum Query_event_status_vars
  {
    Q_FLAGS2_CODE = 0,
    Q_SQL_MODE_CODE,

    Q_CATALOG_CODE,
    Q_AUTO_INCREMENT,
    Q_CHARSET_CODE,
    Q_TIME_ZONE_CODE,

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
      slave in order to facilitate the parallel applying of the Query
      events.
    */
    Q_UPDATED_DB_NAMES,
    Q_MICROSECONDS,

    Q_COMMIT_TS,
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

    Q_DEFAULT_COLLATION_FOR_UTF8MB4,

    Q_SQL_REQUIRE_PRIMARY_KEY,

    Q_DEFAULT_TABLE_ENCRYPTION
  };

  const char *query_;
  const char *db_;
  const char *catalog_;
  const char *time_zone_str_;

protected:
  const char *user_;
  size_t      user_len_;
  const char *host_;
  size_t      host_len_;

  /* Required by the MySQL server class Log_event::Query_event */
  unsigned long data_len_;

public:
  /* data members defined in order they are packed and written into the log */
  uint32_t thread_id_;
  uint32_t query_exec_time_;
  size_t   db_len_;
  uint16_t error_code_;

  uint16_t status_vars_len_;

  size_t q_len_;

  bool flags2_inited;
  bool sql_mode_inited;
  bool charset_inited = true;  // 三个编码集有关

  uint32_t flags2;
  /* In connections sql_mode is 32 bits now but will be 64 bits soon */
  uint64_t sql_mode;
  uint16_t auto_increment_increment, auto_increment_offset;
  //    char charset[6];
  size_t time_zone_len; /* 0 means uninited */
  /*
    Binlog format 3 and 4 start to differ (as far as class members are
    concerned) from here.
  */
  size_t   catalog_len;          // <= 255 char; 0 means uninited
  uint16_t lc_time_names_number; /* 0 means en_US */
  uint16_t charset_database_number;
  /*
    map for tables that will be updated for a multi-table update query
    statement, for other query statements, this will be zero.
  */
  uint64_t table_map_for_update;

  /*
    The following member gets set to OFF or ON value when the
    Query-log-event is marked as dependent on
    @@explicit_defaults_for_timestamp. That is the member is relevant
    to queries that declare TIMESTAMP column attribute, like CREATE
    and ALTER.
    The value is set to @c TERNARY_OFF when @@explicit_defaults_for_timestamp
    encoded value is zero, otherwise TERNARY_ON.
  */
  enum enum_ternary
  {
    TERNARY_UNSET,
    TERNARY_OFF,
    TERNARY_ON
  } explicit_defaults_ts;

  // 在类的成员变量中定义
  uint16_t client_charset_       = 255;  // 默认可以设置为33 (utf8mb4)
  uint16_t connection_collation_ = 255;  // MySQL 8.0 默认 (utf8mb4_general_ci) utf8mb4_0900_ai_ci
  uint16_t server_collation_     = 255;  // 默认可以设置为255 utf8mb4_0900_ai_ci

  /*
    number of updated databases by the query and their names. This info
    is requested by both Coordinator and Worker.
  */
  unsigned char mts_accessed_dbs;
  char          mts_accessed_db_names[MAX_DBS_IN_EVENT_MTS][NAME_LEN];
  /* XID value when the event is a 2pc-capable DDL */
  uint64_t ddl_xid;
  /* Default collation for the utf8mb4 set. Used in cross-version replication
   */
  uint16_t default_collation_for_utf8mb4_number_;
  uint8_t  sql_require_primary_key;

  uint8_t default_table_encryption;
};
