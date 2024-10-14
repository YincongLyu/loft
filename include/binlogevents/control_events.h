// COPY from 'mysql/libbinlogevents/include/control_events.h'

#ifndef CONTROL_EVENT_INCLUDED
#define CONTROL_EVENT_INCLUDED

#include <sys/types.h>
#include <time.h>
#include <vector>

#include "binlog_event.h"
#include "uuid.h"



namespace binary_log {

class Format_description_event : public Binary_log_event {
 public:
  time_t created;
  uint16_t binlog_version;
  char server_version[ST_SERVER_VER_LEN];

  bool dont_set_created;

  uint8_t common_header_len;

  std::vector<uint8_t> post_header_len;
  unsigned char server_version_split[ST_SERVER_VER_SPLIT_LEN];

  Format_description_event(uint8_t binlog_ver, const char *server_ver);
  /**
    The layout of Format_description_event data part is as follows:

    <pre>
    +=====================================+
    | event  | binlog_version   19 : 2    | = 4
    | data   +----------------------------+
    |        | server_version   21 : 50   |
    |        +----------------------------+
    |        | create_timestamp 71 : 4    |
    |        +----------------------------+
    |        | header_length    75 : 1    |
    |        +----------------------------+
    |        | post-header      76 : n    | = array of n bytes, one byte
    |        | lengths for all            |   per event type that the
    |        | event types                |   server knows about
    +=====================================+
  */
  Format_description_event(const char *buf,
                           const Format_description_event *fde);

  Format_description_event(const Format_description_event &) = default;
  Format_description_event &operator=(const Format_description_event &) =
      default;
  uint8_t number_of_event_types;

  unsigned long get_product_version() const;

  bool is_version_before_checksum() const;
 
  void calc_server_version_split();

  ~Format_description_event() override;

  bool header_is_valid() const {
    return ((common_header_len >= LOG_EVENT_MINIMAL_HEADER_LEN) &&
            (!post_header_len.empty()));
  }

  bool version_is_valid() const {
    /* It is invalid only when all version numbers are 0 */
    return server_version_split[0] != 0 || server_version_split[1] != 0 ||
           server_version_split[2] != 0;
  }
};

struct gtid_info {
  int32_t rpl_gtid_sidno;
  int64_t rpl_gtid_gno;
};

class Gtid_event : public Binary_log_event {
 public:
  /*
    The transaction's logical timestamps used for MTS: see
    Transaction_ctx::last_committed and
    Transaction_ctx::sequence_number for details.
    Note: Transaction_ctx is in the MySQL server code.
  */
  long long int last_committed;
  long long int sequence_number;
  /** GTID flags constants */
  unsigned const char FLAG_MAY_HAVE_SBR = 1;
  /** Transaction might have changes logged with SBR */
  bool may_have_sbr_stmts;
  /** Timestamp when the transaction was committed on the originating master. */
  unsigned long long int original_commit_timestamp;
  /** Timestamp when the transaction was committed on the nearest master. */
  unsigned long long int immediate_commit_timestamp;
  bool has_commit_timestamps;
  /** The length of the transaction in bytes. */
  unsigned long long int transaction_length;

 public:
  /**

    +----------+---+---+-------+--------------+---------+----------+
    |gtid flags|SID|GNO|TS_TYPE|logical ts(:s)|commit ts|trx length|
    +----------+---+---+-------+------------------------+----------+
   
    TS_TYPE is from {G_COMMIT_TS2} singleton set of values
    Details on commit timestamps in Gtid_event(const char*...)

  */

  Gtid_event(const char *buf, const Format_description_event *fde);
  /**
    Constructor.
  */
  explicit Gtid_event(long long int last_committed_arg,
                      long long int sequence_number_arg,
                      bool may_have_sbr_stmts_arg,
                      unsigned long long int original_commit_timestamp_arg,
                      unsigned long long int immediate_commit_timestamp_arg,
                      uint32_t original_server_version_arg,
                      uint32_t immediate_server_version_arg)
      : Binary_log_event(GTID_LOG_EVENT),
        last_committed(last_committed_arg),
        sequence_number(sequence_number_arg),
        may_have_sbr_stmts(may_have_sbr_stmts_arg),
        original_commit_timestamp(original_commit_timestamp_arg),
        immediate_commit_timestamp(immediate_commit_timestamp_arg),
        transaction_length(0),
        original_server_version(original_server_version_arg),
        immediate_server_version(immediate_server_version_arg) {}

  /*
    Commit group ticket consists of: 1st bit, used internally for
    synchronization purposes ("is in use"),  followed by 63 bits for
    the ticket value.
  */
  static constexpr int COMMIT_GROUP_TICKET_LENGTH = 8;
  /*
    Default value of commit_group_ticket, which means it is not
    being used.
  */
  static constexpr uint64_t kGroupTicketUnset = 0;

 protected:
  static const int ENCODED_FLAG_LENGTH = 1;
  static const int ENCODED_SID_LENGTH = 16;  // Uuid::BYTE_LENGTH;
  static const int ENCODED_GNO_LENGTH = 8;
  /// Length of typecode for logical timestamps.
  static const int LOGICAL_TIMESTAMP_TYPECODE_LENGTH = 1;
  /// Length of two logical timestamps.
  static const int LOGICAL_TIMESTAMP_LENGTH = 16;
  // Type code used before the logical timestamps.
  static const int LOGICAL_TIMESTAMP_TYPECODE = 2;

  static const int IMMEDIATE_COMMIT_TIMESTAMP_LENGTH = 7;
  static const int ORIGINAL_COMMIT_TIMESTAMP_LENGTH = 7;
  // Length of two timestamps (from original/immediate masters)
  static const int FULL_COMMIT_TIMESTAMP_LENGTH =
      IMMEDIATE_COMMIT_TIMESTAMP_LENGTH + ORIGINAL_COMMIT_TIMESTAMP_LENGTH;
  // We use 7 bytes out of which 1 bit is used as a flag.
  static const int ENCODED_COMMIT_TIMESTAMP_LENGTH = 55;
  // Minimum and maximum lengths of transaction length field.
  static const int TRANSACTION_LENGTH_MIN_LENGTH = 1;
  static const int TRANSACTION_LENGTH_MAX_LENGTH = 9;
  /// Length of original_server_version
  static const int ORIGINAL_SERVER_VERSION_LENGTH = 4;
  /// Length of immediate_server_version
  static const int IMMEDIATE_SERVER_VERSION_LENGTH = 4;
  /// Length of original and immediate server version
  static const int FULL_SERVER_VERSION_LENGTH =
      ORIGINAL_SERVER_VERSION_LENGTH + IMMEDIATE_SERVER_VERSION_LENGTH;
  // We use 4 bytes out of which 1 bit is used as a flag.
  static const int ENCODED_SERVER_VERSION_LENGTH = 31;

  /* 返回提交时间戳的长度. 原始提交时间戳 vs 即时提交时间戳 */
  int get_commit_timestamp_length() const {
    if (original_commit_timestamp != immediate_commit_timestamp)
      return FULL_COMMIT_TIMESTAMP_LENGTH;
    return ORIGINAL_COMMIT_TIMESTAMP_LENGTH; // 有时候不需要完整的时间戳信息，节约存储空间
  }

  /**
    返回服务器版本的长度，原始版本 vs 即时版本信息
  */
  int get_server_version_length() const {
    if (original_server_version != immediate_server_version)
      return FULL_SERVER_VERSION_LENGTH;
    return IMMEDIATE_SERVER_VERSION_LENGTH;
  }

  gtid_info gtid_info_struct;
  Uuid Uuid_parent_struct;

  /* 在所有 序列化后的 GTID event 中的最小 全局事务号 */
  static const int64_t MIN_GNO = 1;
  /// One-past-the-max value of GNO
  static const int64_t GNO_END = INT64_MAX;

 public:
  int64_t get_gno() const { return gtid_info_struct.rpl_gtid_gno; }
  // Uuid get_uuid() const { return Uuid_parent_struct; }
  /// Total length of post header
  static const int POST_HEADER_LENGTH =
      ENCODED_FLAG_LENGTH +               /* flags */
      ENCODED_SID_LENGTH +                /* SID length */
      ENCODED_GNO_LENGTH +                /* GNO length */
      LOGICAL_TIMESTAMP_TYPECODE_LENGTH + /* length of typecode */
      LOGICAL_TIMESTAMP_LENGTH;           /* length of two logical timestamps */

  /*
    We keep the commit timestamps in the body section because they can be of
    variable length.
    On the originating master, the event has only one timestamp as the two
    timestamps are equal. On every other server we have two timestamps.
  */
  static const int MAX_DATA_LENGTH =
      FULL_COMMIT_TIMESTAMP_LENGTH + TRANSACTION_LENGTH_MAX_LENGTH +
      FULL_SERVER_VERSION_LENGTH +
      COMMIT_GROUP_TICKET_LENGTH; /* 64-bit unsigned integer */

  static const int MAX_EVENT_LENGTH =
      LOG_EVENT_HEADER_LEN + POST_HEADER_LENGTH + MAX_DATA_LENGTH;
  /**
   Set the transaction length information.

    This function should be used when the full transaction length (including
    the Gtid event length) is known.

    @param transaction_length_arg The transaction length.
  */
  void set_trx_length(unsigned long long int transaction_length_arg) {
    transaction_length = transaction_length_arg;
  }

  /** The version of the server where the transaction was originally executed */
  uint32_t original_server_version;
  /** The version of the immediate server */
  uint32_t immediate_server_version;

  /** Ticket number used to group sessions together during the BGC. */
  uint64_t commit_group_ticket{kGroupTicketUnset};

  /**
    Returns the length of the packed `commit_group_ticket` field. It may be
    8 bytes or 0 bytes, depending on whether or not the value is
    instantiated.

    @return The length of the packed `commit_group_ticket` field
  */
  int get_commit_group_ticket_length() const;

  /**
   Set the commit_group_ticket and update the transaction length if
   needed, that is, if the commit_group_ticket was not set already
   account it on the transaction size.

   @param value The commit_group_ticket value.
  */
  void set_commit_group_ticket_and_update_transaction_length(
      uint64_t value);
};

class Previous_gtids_event : public Binary_log_event {
 public:
  /**
    Decodes the gtid_executed in the last binlog file

    <pre>
    The buffer layout is as follows
    +--------------------------------------------+
    | Gtids executed in the last binary log file |
    +--------------------------------------------+
    </pre>
    @param buf  Contains the serialized event.
    @param fde  An FDE event (see Rotate_event constructor for more info).
  */
  Previous_gtids_event(const char *buf, const Format_description_event *fde);
  /**
    This is the minimal constructor, and set the
    type_code as PREVIOUS_GTIDS_LOG_EVENT in the header object in
    Binary_log_event
  */
  Previous_gtids_event() : Binary_log_event(PREVIOUS_GTIDS_LOG_EVENT) {}
#ifndef HAVE_MYSYS
  // TODO(WL#7684): Implement the method print_event_info and print_long_info
  //               for all the events supported  in  MySQL Binlog
  void print_event_info(std::ostream &) override {}
  void print_long_info(std::ostream &) override {}
#endif
 protected:
  size_t buf_size;
  const unsigned char *buf;
};

class Xid_event : public Binary_log_event {
 public:

  explicit Xid_event(uint64_t xid_arg)
      : Binary_log_event(XID_EVENT), xid(xid_arg) {}

  Xid_event(const char *buf, const Format_description_event *fde);
  uint64_t xid;
};

class Rotate_event : public Binary_log_event {
 public:
  const char *new_log_ident;
  size_t ident_len;
  unsigned int flags;
  uint64_t pos;

  enum {
    /* Values taken by the flag member variable */
    DUP_NAME = 2,  // if constructor should dup the string argument
    RELAY_LOG = 4  // rotate event for the relay log
  };

  enum {
    /* Rotate event post_header */
    R_POS_OFFSET = 0,
    R_IDENT_OFFSET = 8
  };

  Rotate_event(const char *new_log_ident_arg, size_t ident_len_arg,
               unsigned int flags_arg, uint64_t pos_arg)
      : Binary_log_event(ROTATE_EVENT),
        new_log_ident(new_log_ident_arg),
        ident_len(ident_len_arg ? ident_len_arg : strlen(new_log_ident_arg)),
        flags(flags_arg),
        pos(pos_arg) {}

  /**

    <pre>
    +-----------------------------------------------------------------------+
    | common_header | post_header | position of the first event | file name |
    +-----------------------------------------------------------------------+
    </pre>

    @param buf  Contains the serialized event.
    @param fde  An FDE event, used to get the following information:
                  -binlog_version
                  -server_version
                  -post_header_len
                  -common_header_len
                The content of this object depends on the binlog-version
                currently in use.
  */
  Rotate_event(const char *buf, const Format_description_event *fde);

  ~Rotate_event() override {
    // if (flags & DUP_NAME) bapi_free(const_cast<char *>(new_log_ident));
  }
};

}  // end namespace binary_log

#endif /* CONTROL_EVENTS_INCLUDED */
