#include "control_events.h"
#include "little_endian.h"
#include <iostream>

Format_description_event::Format_description_event(
    uint8_t binlog_ver, const char *server_ver
)
    : AbstractEvent(FORMAT_DESCRIPTION_EVENT)
    , binlog_version_(BINLOG_VERSION) {
    binlog_version_ = binlog_ver;
    if (binlog_ver == 4) { /* MySQL 5.0 and above*/
        memset(server_version_, 0, ST_SERVER_VER_LEN);
        // 直接写入
        strcpy(server_version_, server_ver);
        create_timestamp_ = 1681837923;

        common_header_len_ = LOG_EVENT_HEADER_LEN;
        number_of_event_types = LOG_EVENT_TYPES;

        static uint8_t server_event_header_length[] = {
            0,
            QUERY_HEADER_LEN,
            STOP_HEADER_LEN,
            ROTATE_HEADER_LEN,
            INTVAR_HEADER_LEN,
            0,
            0,
            0,
            APPEND_BLOCK_HEADER_LEN,
            0,
            DELETE_FILE_HEADER_LEN,
            0,
            RAND_HEADER_LEN,
            USER_VAR_HEADER_LEN,
            FORMAT_DESCRIPTION_HEADER_LEN,
            XID_HEADER_LEN,
            BEGIN_LOAD_QUERY_HEADER_LEN,
            EXECUTE_LOAD_QUERY_HEADER_LEN,
            TABLE_MAP_HEADER_LEN,
            0,
            0,
            0,
            ROWS_HEADER_LEN_V1, /* WRITE_ROWS_EVENT_V1*/
            ROWS_HEADER_LEN_V1, /* UPDATE_ROWS_EVENT_V1*/
            ROWS_HEADER_LEN_V1, /* DELETE_ROWS_EVENT_V1*/
            INCIDENT_HEADER_LEN,
            0, /* HEARTBEAT_LOG_EVENT*/
            IGNORABLE_HEADER_LEN,
            IGNORABLE_HEADER_LEN,
            ROWS_HEADER_LEN_V2,
            ROWS_HEADER_LEN_V2,
            ROWS_HEADER_LEN_V2,
            Gtid_event::POST_HEADER_LENGTH, /*GTID_EVENT*/
            Gtid_event::POST_HEADER_LENGTH, /*ANONYMOUS_GTID_EVENT*/
            IGNORABLE_HEADER_LEN,
            TRANSACTION_CONTEXT_HEADER_LEN,
            VIEW_CHANGE_HEADER_LEN,
            XA_PREPARE_HEADER_LEN,
            ROWS_HEADER_LEN_V2,
            TRANSACTION_PAYLOAD_EVENT,
            0 /* HEARTBEAT_LOG_EVENT_V2*/
        };

        post_header_len_.insert(
            post_header_len_.begin(), server_event_header_length,
            server_event_header_length + number_of_event_types
        );

    } else { /* Includes binlog version < 4 */
    }

}

Format_description_event::~Format_description_event() = default;

// write common header 在设计时有个问题，要先留出 common-header 的空间，先写 event 本身，才可以填 data_written_ 和 log_pos_
// 目前先 顺序写
bool Format_description_event::write_common_header(Basic_ostream *ostream, size_t data_length) {
    common_header_ = new EventCommonHeader(get_type_code(), this);
    return this->common_header_->write(ostream, data_length);
}

// 只负责写 event-data：包括 post-header 和 event-body
bool Format_description_event::write_event(Basic_ostream *ostream) {
    // TODO 暂时写固定数据，先确定要写 哪些字段

    // fde 只有 post-header
    uchar buff[AbstractEvent::FORMAT_DESCRIPTION_HEADER_LEN];

    int2store(buff + ST_BINLOG_VER_OFFSET, binlog_version_);
    memcpy((char *)buff + ST_SERVER_VER_OFFSET, server_version_,
           ST_SERVER_VER_LEN);
    create_timestamp_ = get_time();
    int4store(buff + ST_CREATED_OFFSET, static_cast<uint32_t>(create_timestamp_));
    buff[ST_COMMON_HEADER_LEN_OFFSET] = LOG_EVENT_HEADER_LEN; // store 1 byte

    size_t number_of_events = static_cast<int>(post_header_len_.size());

    memcpy((char *)buff + ST_COMMON_HEADER_LEN_OFFSET + 1,
           &post_header_len_.front(), number_of_events);


  return ostream->write(buff, sizeof (buff));

}

bool Format_description_event::write_common_footer(Basic_ostream *ostream) {
    // footer  根据 当前外设的 algo 构造出来, 先默认是  BINLOG_CHECKSUM_ALG_OFF
    common_footer_ = new EventCommonFooter(BINLOG_CHECKSUM_ALG_OFF);
    return this->common_footer_->write(ostream);
}
