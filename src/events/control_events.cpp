#include "events/control_events.h"
#include "utils/little_endian.h"
// #include "logging.h"
// #include "macros.h"
#include <iostream>

/**************************************************************************
        Format_description_event methods
**************************************************************************/

// 在每个构造函数里，并没有实例化 common_header_ 和 common_footer_ 成员变量
Format_description_event::Format_description_event(uint8_t binlog_ver, const char *server_ver)
    : AbstractEvent(FORMAT_DESCRIPTION_EVENT), binlog_version_(BINLOG_VERSION)
{

  if (binlog_ver == 4) { /* MySQL 5.0 and above*/
    memset(server_version_, 0, ST_SERVER_VER_LEN);
    // 直接写入
    strcpy(server_version_, server_ver);
    //    create_timestamp_ = 1681837923;

    common_header_len_    = LOG_EVENT_HEADER_LEN;
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
        post_header_len_.begin(), server_event_header_length, server_event_header_length + number_of_event_types);

  } else { /* Includes binlog version < 4 */
  }

  // AbstarctEvent 在写 common_header 时，会使用成员变量， type_code_，故先不填充没事
  this->common_header_ = std::make_unique<EventCommonHeader>(get_common_header_time());
  //    this->common_footer_ = new EventCommonFooter(BINLOG_CHECKSUM_ALG_OFF);
}

Format_description_event::~Format_description_event() = default;

// 只负责写 event-data：包括 post-header 和 event-body
bool Format_description_event::write(Basic_ostream *ostream)
{
  // TODO 暂时写固定数据，先确定要写 哪些字段

  // fde 只有 post-header
  size_t rec_size = AbstractEvent::FORMAT_DESCRIPTION_HEADER_LEN + BINLOG_CHECKSUM_ALG_DESC_LEN;
  uchar  buff[rec_size];

  int2store(buff + ST_BINLOG_VER_OFFSET, binlog_version_);
  memcpy((char *)buff + ST_SERVER_VER_OFFSET, server_version_, ST_SERVER_VER_LEN);
  create_timestamp_ = get_fde_create_time();
  int4store(buff + ST_CREATED_OFFSET, static_cast<uint32_t>(create_timestamp_));
  buff[ST_COMMON_HEADER_LEN_OFFSET] = LOG_EVENT_HEADER_LEN;  // store 1 byte

  size_t number_of_events = static_cast<int>(post_header_len_.size());

  memcpy((char *)buff + ST_COMMON_HEADER_LEN_OFFSET + 1, &post_header_len_.front(), number_of_events);

  buff[FORMAT_DESCRIPTION_HEADER_LEN] = (uint8_t)BINLOG_CHECKSUM_ALG_OFF;

  return write_common_header(ostream, rec_size) && ostream->write(buff, rec_size);
}
time_t Format_description_event::get_fde_create_time()
{
  struct timeval tv;
  gettimeofday(&tv, NULL);
  return tv.tv_sec;  // Return time in seconds
}

/**************************************************************************
        Previous_gtids_event methods
**************************************************************************/

Previous_gtids_event::Previous_gtids_event(const Gtid_set *set) : AbstractEvent(PREVIOUS_GTIDS_LOG_EVENT)
{
  // TODO 后续根据 gtid_set 大小改造

  buf_size_     = set->get_encoded_length();  // 固定为 8
  uchar *buffer = (uchar *)malloc((sizeof(uchar) * buf_size_));
  set->encode(buffer);
  buf_ = buffer;

  //  this->common_header_ = std::make_unique<EventCommonHeader>();
  //    this->common_footer_ = new EventCommonFooter(BINLOG_CHECKSUM_ALG_OFF);
}

bool Previous_gtids_event::write(Basic_ostream *ostream)
{
  // 无 post-header
  return write_common_header(ostream, get_data_size()) && write_data_body(ostream);
}

bool Previous_gtids_event::write_data_body(Basic_ostream *ostream)
{
  std::cout << "current event data_body write pos: " << ostream->get_position() << std::endl;

  return ostream->write(buf_, buf_size_);
}

Previous_gtids_event::~Previous_gtids_event() = default;

/**************************************************************************
        Gtid_event methods
**************************************************************************/

Gtid_event::Gtid_event(int64 last_committed_arg, int64 sequence_number_arg, bool may_have_sbr_stmts_arg,
    uint64 original_commit_timestamp_arg, uint64 immediate_commit_timestamp_arg, uint32 original_server_version_arg,
    uint32 immediate_server_version_arg)
    : AbstractEvent(GTID_LOG_EVENT),
      last_committed_(last_committed_arg),
      sequence_number_(sequence_number_arg),
      may_have_sbr_stmts_(may_have_sbr_stmts_arg),
      original_commit_timestamp_(original_commit_timestamp_arg),
      immediate_commit_timestamp_(immediate_commit_timestamp_arg),
      transaction_length_(0),
      original_server_version_(original_server_version_arg),
      immediate_server_version_(immediate_server_version_arg)
{
  // 默认当前 txn 是 Anonymous
  spec_.set_anonymous();
  spec_.gtid_.clear();
  sid_.clear();

  time_t i_ts          = static_cast<time_t>(immediate_commit_timestamp_arg / 1000000);
  this->common_header_ = std::make_unique<EventCommonHeader>(i_ts);
  Log_event_type event_type =
      (spec_.type_ == ANONYMOUS_GTID ? Log_event_type::ANONYMOUS_GTID_LOG_EVENT : Log_event_type::GTID_LOG_EVENT);
  this->type_code_ = event_type;

  //    this->common_footer_ = new EventCommonFooter(BINLOG_CHECKSUM_ALG_OFF);
}

size_t Gtid_event::get_data_size()
{
  // 默认 txn_length = 0, 省略 net_length_size(transaction_length) 大小
  // 只有考虑 commit_group_ticket 参数，才会计算 txn_length
  return POST_HEADER_LENGTH + get_commit_timestamp_length() + 1 + get_server_version_length();
}

uint32_t Gtid_event::write_post_header_to_memory(uchar *buffer)
{
  uchar *ptr_buffer = buffer;

  /* Encode the GTID flags */
  uchar gtid_flags = 0;  // 1 byte
  gtid_flags |= may_have_sbr_stmts_ ? Gtid_event::FLAG_MAY_HAVE_SBR : 0;
  *ptr_buffer = gtid_flags;
  ptr_buffer += ENCODED_FLAG_LENGTH;

  sid_.copy_to(ptr_buffer);  // 16 byte
  ptr_buffer += ENCODED_SID_LENGTH;

  int8store(ptr_buffer, spec_.gtid_.gno_);  // 8 byte
  ptr_buffer += ENCODED_GNO_LENGTH;

  *ptr_buffer = LOGICAL_TIMESTAMP_TYPECODE;
  ptr_buffer += LOGICAL_TIMESTAMP_TYPECODE_LENGTH;  // 1 byte

  int8store(ptr_buffer, last_committed_);       // 8 byte
  int8store(ptr_buffer + 8, sequence_number_);  // 8 byte
  ptr_buffer += LOGICAL_TIMESTAMP_LENGTH;

  assert(ptr_buffer == (buffer + POST_HEADER_LENGTH));

  return POST_HEADER_LENGTH;
}

bool Gtid_event::write_data_header(Basic_ostream *ostream)
{
  uchar buffer[POST_HEADER_LENGTH];
  write_post_header_to_memory(buffer);
  return ostream->write((uchar *)buffer, POST_HEADER_LENGTH);
}

// FULL_COMMIT_TIMESTAMP_LENGTH + 0 + FULL_SERVER_VERSION_LENGTH + 0
uint32_t Gtid_event::write_body_to_memory(uchar *buffer)
{
  uchar *ptr_buffer = buffer;

  unsigned long long immediate_commit_timestamp_with_flag = immediate_commit_timestamp_;

  if (immediate_commit_timestamp_ != original_commit_timestamp_) {
    immediate_commit_timestamp_with_flag |= (1ULL << ENCODED_COMMIT_TIMESTAMP_LENGTH);
  } else {  // Clear highest bit(MSB)
    immediate_commit_timestamp_with_flag &= ~(1ULL << ENCODED_COMMIT_TIMESTAMP_LENGTH);
  }

  int7store(ptr_buffer, immediate_commit_timestamp_with_flag);
  ptr_buffer += IMMEDIATE_COMMIT_TIMESTAMP_LENGTH;

  if (immediate_commit_timestamp_ != original_commit_timestamp_) {
    int7store(ptr_buffer, original_commit_timestamp_);
    ptr_buffer += ORIGINAL_COMMIT_TIMESTAMP_LENGTH;
  }

  // Write the transaction length information, 即使 txn_len = 0, 也会占一个
  // byte
  uchar *ptr_after_length = net_store_length(ptr_buffer, transaction_length_);
  ptr_buffer              = ptr_after_length;

  uint32_t immediate_server_version_with_flag = immediate_server_version_;

  if (immediate_server_version_ != original_server_version_) {
    immediate_server_version_with_flag |= (1ULL << ENCODED_SERVER_VERSION_LENGTH);
  } else {  // Clear MSB
    immediate_server_version_with_flag &= ~(1ULL << ENCODED_SERVER_VERSION_LENGTH);
  }

  int4store(ptr_buffer, immediate_server_version_with_flag);
  ptr_buffer += IMMEDIATE_SERVER_VERSION_LENGTH;

  if (immediate_server_version_ != original_server_version_) {
    int4store(ptr_buffer, original_server_version_);
    ptr_buffer += ORIGINAL_SERVER_VERSION_LENGTH;
  }

  return ptr_buffer - buffer;
}

bool Gtid_event::write_data_body(Basic_ostream *ostream)
{
  uchar    buffer[MAX_DATA_LENGTH];
  uint32_t len = write_body_to_memory(buffer);

  //    LOFT_VERIFY(
  //        len == 19, "empty gtid event data body len is not correct"
  //    ); // 7 + 7 + 1 + 4 = 19 byte

  return ostream->write((uchar *)buffer, len);
}

bool Gtid_event::write(Basic_ostream *ostream) { return AbstractEvent::write(ostream); }

Gtid_event::~Gtid_event() = default;

/**************************************************************************
        Xid_event methods
**************************************************************************/
Xid_event::Xid_event(uint64_t xid_arg, uint64 immediate_commit_timestamp_arg) : AbstractEvent(XID_EVENT), xid_(xid_arg)
{
  //    this->common_header_ = new EventCommonHeader();
  time_t i_ts          = static_cast<time_t>(immediate_commit_timestamp_arg / 1000000);
  this->common_header_ = std::make_unique<EventCommonHeader>(i_ts);
  //  this->common_header_ = std::make_unique<EventCommonHeader>(immediate_commit_timestamp_arg);
  //    this->common_footer_ = new EventCommonFooter(BINLOG_CHECKSUM_ALG_OFF);
}

bool Xid_event::write(Basic_ostream *ostream)
{
  return write_common_header(ostream, get_data_size()) && ostream->write((uchar *)&xid_, sizeof(xid_));
}

/**************************************************************************
        Rotate_event methods
**************************************************************************/

// FIXME 现在是直接把 pos = 4，如果前一个文件空间不足，直接忽略文件后面的部分

Rotate_event::Rotate_event(const std::string &new_log_ident_arg, size_t ident_len_arg, uint32 flags_arg, uint64 pos_arg)
    : AbstractEvent(ROTATE_EVENT),
      new_log_ident_(new_log_ident_arg),
      ident_len_(ident_len_arg ? ident_len_arg : new_log_ident_arg.length()),
      flags_(flags_arg) /* DUP_NAME */
      ,
      pos_(pos_arg)
{ /* 4 byte */

  this->common_header_ = std::make_unique<EventCommonHeader>(get_common_header_time());
  //  this->common_header_ = std::make_unique<EventCommonHeader>(immediate_commit_timestamp_arg);
  //    this->common_footer_ = new EventCommonFooter(BINLOG_CHECKSUM_ALG_OFF);
}

bool Rotate_event::write(Basic_ostream *ostream)
{
  uchar buf[AbstractEvent::ROTATE_HEADER_LEN];
  int8store(buf + R_POS_OFFSET, pos_);
  return write_common_header(ostream, get_data_size()) &&
         ostream->write((uchar *)buf, AbstractEvent::ROTATE_HEADER_LEN) &&
         ostream->write(pointer_cast<const uchar *>(new_log_ident_.c_str()), (uint)ident_len_);
}
