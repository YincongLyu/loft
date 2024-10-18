//
// Created by Coonger on 2024/10/17.
//
// AbstractEvent.cpp
#include "abstract_event.h"
#include "little_endian.h"

// 即使是纯虚函数，也需要在 cpp 文件中定义析构函数
AbstractEvent::~AbstractEvent() = default;

bool AbstractEvent::write(Basic_ostream *ostream) {
    return write_common_header(ostream, LOG_EVENT_HEADER_LEN)
           && write_event(ostream) && write_common_footer(ostream);
}

unsigned long AbstractEvent::get_time() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec * 1000000 + tv.tv_usec;
}

uint32_t EventCommonHeader::write_common_header_to_memory(uchar *buf) {
    // Query start time
    ulong timestamp = (ulong)event_->get_time();

    int4store(buf, timestamp);
    buf[EVENT_TYPE_OFFSET] = type_code_;
    int4store(buf + SERVER_ID_OFFSET, event_->server_id_);
    int4store(buf + EVENT_LEN_OFFSET,
              static_cast<uint32_t >(data_written_));
    int4store(buf + LOG_POS_OFFSET, static_cast<uint32_t>(log_pos_));
    int2store(buf + FLAGS_OFFSET, flags_);

    return LOG_EVENT_HEADER_LEN;
}

bool EventCommonHeader::write(Basic_ostream *ostream, size_t data_length) {
    uchar header[LOG_EVENT_HEADER_LEN];

    // 暂时不管 data_written_ 和 log_pos_

    write_common_header_to_memory(header);

    return ostream->write(header, LOG_EVENT_HEADER_LEN);
}

bool EventCommonFooter::write(Basic_ostream *ostream) {

    uchar buf[BINLOG_CHECKSUM_LEN];
    int4store(buf, 0);
    return ostream->write(buf, sizeof(buf));

}
