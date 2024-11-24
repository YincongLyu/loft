//
// Created by Coonger on 2024/10/17.
//

#include "events/abstract_event.h"

#include "common/logging.h"

#include "utils/little_endian.h"

time_t AbstractEvent::get_common_header_time()
{
  struct timeval tv;
  gettimeofday(&tv, NULL);
  return tv.tv_sec;  // Return time in seconds
}

uint32 AbstractEvent::write_common_header_to_memory(uchar *buf)
{
  // TODO 暂时用系统时间，填充 实际上是数据源 commit time 去掉微秒转化成 ts类型，和写 log_pos 一样是在 制作完后续的
  // event data body 写完才确定的时间
  //  int4store(buf, 1730311658);
  int4store(buf, common_header_->timestamp_); // 不算微秒
  buf[EVENT_TYPE_OFFSET] = type_code_;
  int4store(buf + SERVER_ID_OFFSET, SERVER_ID);
  int4store(buf + EVENT_LEN_OFFSET, static_cast<uint32_t>(common_header_->data_written_));
  int4store(buf + LOG_POS_OFFSET, static_cast<uint32_t>(common_header_->log_pos_));
  int2store(buf + FLAGS_OFFSET, common_header_->flags_);

  return LOG_EVENT_HEADER_LEN;
}

bool AbstractEvent::write_common_header(Basic_ostream *ostream, size_t event_data_length)
{
  uchar header[LOG_EVENT_HEADER_LEN];

  common_header_->data_written_ = sizeof(header) + event_data_length;
  // TODO 先 给crc checksum 先算上位置，但不计算真实值
  //    common_header_->data_written_ += BINLOG_CHECKSUM_LEN;
  common_header_->log_pos_ = ostream->get_position() + common_header_->data_written_;

  write_common_header_to_memory(header);

  LOG_INFO(
        "current event common-header write pos: %llu", ostream->get_position()
    );

  return ostream->write(header, LOG_EVENT_HEADER_LEN);
}

size_t AbstractEvent::write_common_header_to_buffer(uchar* buffer) {
  common_header_->data_written_ = LOG_EVENT_HEADER_LEN + get_data_size();
  // 先用占位符填充 log_pos_
  common_header_->log_pos_ = POSITION_PLACEHOLDER;

  int4store(buffer, common_header_->timestamp_);
  buffer[EVENT_TYPE_OFFSET] = type_code_;
  int4store(buffer + SERVER_ID_OFFSET, SERVER_ID);
  int4store(buffer + EVENT_LEN_OFFSET, common_header_->data_written_);
  int4store(buffer + LOG_POS_OFFSET, common_header_->log_pos_);
  int2store(buffer + FLAGS_OFFSET, common_header_->flags_);

  return LOG_EVENT_HEADER_LEN;
}

bool AbstractEvent::write_common_footer(Basic_ostream *ostream)
{
  LOG_INFO("current event checksum write pos: %llu", ostream->get_position());

  uchar buf[BINLOG_CHECKSUM_LEN];
  int4store(buf, 0);  // 后续引入 crc32 计算
  return ostream->write(buf, sizeof(buf));
}
