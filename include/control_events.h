//
// Created by Coonger on 2024/10/16.
//

#ifndef LOFT_CONTROL_EVENTS_H
#define LOFT_CONTROL_EVENTS_H

#include "abstract_event.h"
#include <cstring>
#include <vector>

/*

###### #####  ######
#      #    # #
#####  #    # #####
#      #    # #
#      #    # #
#      #####  ######

*/
class Format_description_event : public AbstractEvent {
  public:
    uint16_t binlog_version_;
    /* 每个版本的固定值，不可修改，否则在 replication 时会出错, 目前暂时为 empty
     */
    char server_version_[ST_SERVER_VER_LEN]{};
    time_t create_timestamp_;
    uint8_t common_header_len_;
    std::vector<uint8_t> post_header_len_;

    /**
      Constructor. 同时 初始化 common-header 和 common-footer 对象
    */
    Format_description_event(uint8_t binlog_ver, const char *server_ver);

    Format_description_event(const Format_description_event &) = default;
    Format_description_event &
    operator=(const Format_description_event &) = default;
    uint8_t number_of_event_types;

    ~Format_description_event() override;

    bool write(Basic_ostream *ostream) override;


};

/*

#####  #####  ###### #    #          ####  ##### # #####          ######
#    # #    # #      #    #         #    #   #   # #    #         #
#    # #    # #####  #    #         #        #   # #    #         #####
#####  #####  #      #    #         #  ###   #   # #    #         #
#      #   #  #       #  #          #    #   #   # #    #         #
#      #    # ######   ##            ####    #   # #####          ######
                            #######                       #######
                                                                                                                                                                                                  ~
 */

class Previous_gtids_event : public AbstractEvent {
  public:
    Previous_gtids_event() : AbstractEvent(PREVIOUS_GTIDS_LOG_EVENT) {}

  protected:
    size_t buf_size;
    const unsigned char *buf;
};

/*


 ####  ##### # #####          ######
#    #   #   # #    #         #
#        #   # #    #         #####
#  ###   #   # #    #         #
#    #   #   # #    #         #
 ####    #   # #####          ######
                      #######
                                                                                                                                                                                          ~
*/

struct gtid_info {
    int32_t rpl_gtid_sidno;
    int64_t rpl_gtid_gno;
};

class Gtid_event : public AbstractEvent {
  public:
    long long int last_committed;
    long long int sequence_number;
    /** GTID flags constants */
    const unsigned char FLAG_MAY_HAVE_SBR = 1;
    /** Transaction might have changes logged with SBR */
    bool may_have_sbr_stmts;
    /** Timestamp when the transaction was committed on the originating master.
     */
    unsigned long long int original_commit_timestamp;
    /** Timestamp when the transaction was committed on the nearest master. */
    unsigned long long int immediate_commit_timestamp;
    bool has_commit_timestamps;
    /** The length of the transaction in bytes. */
    unsigned long long int transaction_length;

  public:
    /**
      Constructor.
    */
    explicit Gtid_event(
        long long int last_committed_arg,
        long long int sequence_number_arg,
        bool may_have_sbr_stmts_arg,
        unsigned long long int original_commit_timestamp_arg,
        unsigned long long int immediate_commit_timestamp_arg,
        uint32_t original_server_version_arg,
        uint32_t immediate_server_version_arg
    )
        : AbstractEvent(GTID_LOG_EVENT)
        , last_committed(last_committed_arg)
        , sequence_number(sequence_number_arg)
        , may_have_sbr_stmts(may_have_sbr_stmts_arg)
        , original_commit_timestamp(original_commit_timestamp_arg)
        , immediate_commit_timestamp(immediate_commit_timestamp_arg)
        , transaction_length(0)
        , original_server_version(original_server_version_arg)
        , immediate_server_version(immediate_server_version_arg) {}

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
    static constexpr std::uint64_t kGroupTicketUnset = 0;

  protected:
    static const int ENCODED_FLAG_LENGTH = 1;
    static const int ENCODED_SID_LENGTH = 16; // Uuid::BYTE_LENGTH;
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

    /* We have only original commit timestamp if both timestamps are equal. */
    int get_commit_timestamp_length() const {
        if (original_commit_timestamp != immediate_commit_timestamp) {
            return FULL_COMMIT_TIMESTAMP_LENGTH;
        }
        return ORIGINAL_COMMIT_TIMESTAMP_LENGTH;
    }

    /**
      We only store the immediate_server_version if both server versions are the
      same.
    */
    int get_server_version_length() const {
        if (original_server_version != immediate_server_version) {
            return FULL_SERVER_VERSION_LENGTH;
        }
        return IMMEDIATE_SERVER_VERSION_LENGTH;
    }

    gtid_info gtid_info_struct;
    //    Uuid Uuid_parent_struct;

    /* Minimum GNO expected in a serialized GTID event */
    static const int64_t MIN_GNO = 1;
    /// One-past-the-max value of GNO
    static const std::int64_t GNO_END = INT64_MAX;

  public:
    std::int64_t get_gno() const { return gtid_info_struct.rpl_gtid_gno; }

    //    Uuid get_uuid() const { return Uuid_parent_struct; }
    /// Total length of post header
    static const int POST_HEADER_LENGTH =
        ENCODED_FLAG_LENGTH +               /* flags */
        ENCODED_SID_LENGTH +                /* SID length */
        ENCODED_GNO_LENGTH +                /* GNO length */
        LOGICAL_TIMESTAMP_TYPECODE_LENGTH + /* length of typecode */
        LOGICAL_TIMESTAMP_LENGTH; /* length of two logical timestamps */

    /*
      We keep the commit timestamps in the body section because they can be of
      variable length.
      On the originating master, the event has only one timestamp as the two
      timestamps are equal. On every other server we have two timestamps.
    */
    static const int MAX_DATA_LENGTH =
        FULL_COMMIT_TIMESTAMP_LENGTH + TRANSACTION_LENGTH_MAX_LENGTH
        + FULL_SERVER_VERSION_LENGTH
        + COMMIT_GROUP_TICKET_LENGTH; /* 64-bit unsigned integer */

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

    /** The version of the server where the transaction was originally executed
     */
    uint32_t original_server_version;
    /** The version of the immediate server */
    uint32_t immediate_server_version;

    /** Ticket number used to group sessions together during the BGC. */
    std::uint64_t commit_group_ticket{kGroupTicketUnset};

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
    void
    set_commit_group_ticket_and_update_transaction_length(std::uint64_t value);
};

/*
 *
#    # # #####          ######
 #  #  # #    #         #
  ##   # #    #         #####
  ##   # #    #         #
 #  #  # #    #         #
#    # # #####          ######
                #######
                                                                                                                                                                                                ~                                                                                                                                                                                                        ~
 */
class Xid_event : public AbstractEvent {
  public:
    explicit Xid_event(uint64_t xid_arg)
        : AbstractEvent(XID_EVENT)
        , xid(xid_arg) {}

  private:
    uint64_t xid;
};

/**

#####   ####  #####   ##   ##### ######         ######
#    # #    #   #    #  #    #   #              #
#    # #    #   #   #    #   #   #####          #####
#####  #    #   #   ######   #   #              #
#   #  #    #   #   #    #   #   #              #
#    #  ####    #   #    #   #   ######         ######
                                        #######

 */
class Rotate_event : public AbstractEvent {
  public:
    const char *new_log_ident_;
    size_t ident_len_;
    unsigned int flags_;
    uint64_t pos_;

    enum {
        /* Values taken by the flag member variable */
        DUP_NAME = 2, // if constructor should dup the string argument
        RELAY_LOG = 4 // rotate event for the relay log
    };

    enum {
        /* Rotate event post_header */
        R_POS_OFFSET = 0,
        R_IDENT_OFFSET = 8
    };

    Rotate_event(
        const char *new_log_ident_arg,
        size_t ident_len_arg,
        unsigned int flags_arg,
        uint64_t pos_arg
    )
        : AbstractEvent(ROTATE_EVENT)
        , new_log_ident_(new_log_ident_arg)
        , ident_len_(ident_len_arg ? ident_len_arg : strlen(new_log_ident_arg))
        , flags_(flags_arg)
        , pos_(pos_arg) {}

    ~Rotate_event() override {
        // if (flags & DUP_NAME) bapi_free(const_cast<char *>(new_log_ident));
    }
};

#endif // LOFT_CONTROL_EVENTS_H
