//
// Created by Coonger on 2024/10/17.
//

#include "events/abstract_event.h"

#include "common/logging.h"

#include "utils/little_endian.h"

// 即使是纯虚函数，也需要在 cpp 文件中定义析构函数
//AbstractEvent::~AbstractEvent() = default;


uint64 AbstractEvent::get_common_header_time() { return 0; }

uint32 AbstractEvent::write_common_header_to_memory(uchar *buf) {
    // TODO 暂时用系统时间，填充 实际上是数据源 commit time 去掉微秒转化成 ts类型，和写 log_pos 一样是在 制作完后续的 event data body 再写入的
    //    ulong timestamp = (ulong)get_time();
    //    int4store(buf, timestamp);
    int4store(buf, common_header_->timestamp_);
    buf[EVENT_TYPE_OFFSET] = type_code_;
    int4store(buf + SERVER_ID_OFFSET, SERVER_ID);
    int4store(
        buf + EVENT_LEN_OFFSET,
        static_cast<uint32_t>(common_header_->data_written_)
    );
    int4store(
        buf + LOG_POS_OFFSET, static_cast<uint32_t>(common_header_->log_pos_)
    );
    int2store(buf + FLAGS_OFFSET, common_header_->flags_);

    return LOG_EVENT_HEADER_LEN;
}

bool AbstractEvent::write_common_header(
    Basic_ostream *ostream, size_t event_data_length
) {
    uchar header[LOG_EVENT_HEADER_LEN];

    common_header_->data_written_ = sizeof(header) + event_data_length;
    // TODO 先 给crc checksum 先算上位置，但不计算真实值
    //    common_header_->data_written_ += BINLOG_CHECKSUM_LEN;
    common_header_->log_pos_ =
        ostream->get_position() + common_header_->data_written_;

    write_common_header_to_memory(header);

    LOG_INFO(
        "current event common-header write pos: %llu", ostream->get_position()
    );

    return ostream->write(header, LOG_EVENT_HEADER_LEN);
}

bool AbstractEvent::write_common_footer(Basic_ostream *ostream) {
    LOG_INFO("current event checksum write pos: %llu", ostream->get_position());

    uchar buf[BINLOG_CHECKSUM_LEN];
    int4store(buf, 0); // 后续引入 crc32 计算
    return ostream->write(buf, sizeof(buf));
}

