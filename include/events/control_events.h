//
// Created by Coonger on 2024/10/16.
//

#pragma once

#include "events/abstract_event.h"
#include "utils/rpl_gtid.h"

#include <sys/time.h> // gettimeofday()
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
    Format_description_event(uint8 binlog_ver, const char *server_ver);
    ~Format_description_event() override;

    DISALLOW_COPY(Format_description_event);

    // ********* impl virtual function *********************
    size_t get_data_size() override {
        return AbstractEvent::FORMAT_DESCRIPTION_HEADER_LEN;
    }
    bool write(Basic_ostream *ostream) override;

  private:
    time_t get_fde_create_time();
  public:
    uint16 binlog_version_;
    /* 每个版本的固定值，不可修改，否则在 replication 时会出错, 目前暂时为 empty
     */
    char server_version_[ST_SERVER_VER_LEN]{};
    time_t create_timestamp_;
    uint8 common_header_len_;
    std::vector<uint8> post_header_len_;

    uint8 number_of_event_types;

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
    // TODO add class Gtid_set
    Previous_gtids_event(const Gtid_set *set);
    ~Previous_gtids_event() override;

    DISALLOW_COPY(Previous_gtids_event);

    // ********* impl virtual function *********************
    size_t get_data_size() override { return buf_size_; }

    bool write(Basic_ostream *ostream) override;
    bool write_data_body(Basic_ostream *ostream) override;

    const uchar *get_buf() { return buf_; }

    /**
      格式化输出 prev gtid set 信息
    */
    //    char *get_str(size_t *length, const Gtid_set::String_format
    //    *string_format) const;
    // Add all GTIDs from this event to the given Gtid_set.
    //    int add_to_set(Gtid_set *gtid_set) const;

    size_t get_encoded_length() const;

  protected:
    size_t buf_size_;
    const uchar *buf_;
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
    Gtid_event(
        int64 last_committed_arg,
        int64 sequence_number_arg,
        bool may_have_sbr_stmts_arg,
        uint64 original_commit_timestamp_arg,
        uint64 immediate_commit_timestamp_arg,
        uint32 original_server_version_arg,
        uint32 immediate_server_version_arg
    );

    ~Gtid_event() override;
    DISALLOW_COPY(Gtid_event);

    // ********* impl virtual function *********************
    size_t get_data_size() override;
    bool write(Basic_ostream *ostream) override;
    bool write_data_header(Basic_ostream *ostream) override;
    bool write_data_body(Basic_ostream *ostream) override;

  private:
    /**
    固定长度：Gtid_log_event::POST_HEADER_LENGTH.
  */
    uint32_t write_post_header_to_memory(uchar *buffer);

    /**
      @return The number of bytes written, i.e.,
              If the transaction did not originated on this server
                Gtid_event::IMMEDIATE_COMMIT_TIMESTAMP_LENGTH.
              else
                FULL_COMMIT_TIMESTAMP_LENGTH.
    */
    uint32_t write_body_to_memory(uchar *buffer);

  public:
    long long int last_committed_;
    long long int sequence_number_;
    /** GTID flags constants */
    const unsigned char FLAG_MAY_HAVE_SBR = 1;
    /** Transaction might have changes logged with SBR */
    bool may_have_sbr_stmts_;
    /** Timestamp when the transaction was committed on the originating master.
     */
    unsigned long long int original_commit_timestamp_;
    /** Timestamp when the transaction was committed on the nearest master. */
    unsigned long long int immediate_commit_timestamp_;
    bool has_commit_timestamps{};
    /** The length of the transaction in bytes. */
    unsigned long long int transaction_length_;

    Gtid_specification spec_;
    /// SID for this GTID.
    rpl_sid sid_;

    /*
       第一个 bit 表示是否 启用 sync
       后 63 bit 表示 ticket value
    */
    static constexpr int COMMIT_GROUP_TICKET_LENGTH = 8;

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
        if (original_commit_timestamp_ != immediate_commit_timestamp_) {
            return FULL_COMMIT_TIMESTAMP_LENGTH;
        }
        return ORIGINAL_COMMIT_TIMESTAMP_LENGTH;
    }

    /**
      We only store the immediate_server_version if both server versions are the
      same.
    */
    int get_server_version_length() const {
        if (original_server_version_ != immediate_server_version_) {
            return FULL_SERVER_VERSION_LENGTH;
        }
        return IMMEDIATE_SERVER_VERSION_LENGTH;
    }

    gtid_info gtid_info_struct{};
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
        transaction_length_ = transaction_length_arg;
    }

    /** The version of the server where the transaction was originally executed
     */
    uint32_t original_server_version_;
    /** The version of the immediate server */
    uint32_t immediate_server_version_;
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
    Xid_event(uint64_t xid_arg, uint64 immediate_commit_timestamp_arg = 0);
    ~Xid_event() override = default;
    DISALLOW_COPY(Xid_event);

    // ********* impl virtual function *********************
    size_t get_data_size() override { return sizeof(xid_); }

    bool write(Basic_ostream *ostream) override;

  private:
    uint64_t xid_;
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
    Rotate_event(
        const std::string &new_log_ident_arg,
        size_t ident_len_arg,
        uint32 flags_arg,
        uint64 pos_arg,
        uint64 immediate_commit_timestamp_arg = 0
    );

    ~Rotate_event() override = default; // 使用 string 自动管理 file_name 内存
    DISALLOW_COPY(Rotate_event);

    // ********* impl virtual function *********************
    size_t get_data_size() override { return ident_len_ + ROTATE_HEADER_LEN; }

    bool write(Basic_ostream *ostream) override;

  public:
    const std::string new_log_ident_; // nxt binlog file_name
    size_t ident_len_;          // nxt file_name length
    uint32 flags_;
    uint64 pos_;

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
};

