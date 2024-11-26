//
// Created by Coonger on 2024/10/17.
//

#include <gtest/gtest.h>

#include "events/control_events.h"
#include "events/rows_event.h"
#include "events/statement_events.h"

#include "common/logging.h"
#include "common/macros.h"

#include "binlog.h"
#include "utils/table_id.h"
#include "log_file.h"

/**
 * @brief 打开一个 binlog 文件， 如果有内容，则不会写入 magic number 和 fde 事件
 *          测试 last_file()
 */
TEST(CONTROL_EVENT_FORMAT_TEST, OPEN_LAST_FILE_FDE) {

    auto logFileManager = std::make_unique<LogFileManager>();
    logFileManager->init(DEFAULT_BINLOG_FILE_DIR, DEFAULT_BINLOG_FILE_NAME_PREFIX, DEFAULT_BINLOG_FILE_SIZE);

    // 2. 打开最后一个 binlog 文件，准备写
    auto fileWriter = logFileManager->get_file_writer();
    logFileManager->last_file(*fileWriter); // 可能还有空间写，如果没有空间写，则会调用 next_file()

    auto files = logFileManager->get_log_files();

    EXPECT_EQ(files.size(), 1);
    for (auto &file : files) {
      std::cout << file.second << std::endl;
    }
    // 第二次调用 last_file()， 还是 1 个文件
    logFileManager->last_file(*fileWriter);
    EXPECT_EQ(files.size(), 1);
    for (auto &file : files) {
      std::cout << file.second << std::endl;
    }

    fileWriter->close();
}

/**
 * @brief 打开一个 binlog 文件， 如果是一个新的，则会自动写入 magic number 和 fde 事件
 *          测试 next_file()，并且可以看到 ON.000001 这个文件的末尾有写入 rotate event
 */
TEST(CONTROL_EVENT_FORMAT_TEST, OPEN_NEXT_FILE_ROTATE) {

  auto logFileManager = std::make_unique<LogFileManager>();
  logFileManager->init(DEFAULT_BINLOG_FILE_DIR, DEFAULT_BINLOG_FILE_NAME_PREFIX, DEFAULT_BINLOG_FILE_SIZE);

  // 2. 重新打开一个 新的 binlog 文件，准备写
  auto fileWriter = logFileManager->get_file_writer();
  logFileManager->last_file(*fileWriter); // last_file 里有 open 操作，除非主动 fileWriter.open()
  logFileManager->next_file(*fileWriter);

  auto files = logFileManager->get_log_files();

  EXPECT_EQ(files.size(), 2);
  for (auto &file : files) {
    std::cout << file.second << std::endl;
  }

  fileWriter->close();
}

TEST(CONTROL_EVENT_FORMAT_TEST, FORMAT_DESCRIPTION_EVENT) {
    const char *test_file_name = "test_magic_fde";
    uint64_t test_file_size = 1024;

    RC ret;
    auto binlog = std::make_unique<MYSQL_BIN_LOG>(test_file_name, test_file_size, ret);
    LOFT_VERIFY(ret != RC::SUCCESS, "Failed to create binlog file.");

    ret = binlog->open();
    LOFT_VERIFY(ret != RC::SUCCESS, "Failed to open binlog file");

    auto fde = std::make_unique<Format_description_event>(4, "8.0.32-debug");
    binlog->write_event_to_binlog(fde.get());

    binlog->close();
}

/**
 * @brief 测试 binlog 写入 GTID 事件
 */
TEST(CONTROL_EVENT_FORMAT_TEST, GTID_EVENT) {
    const char *test_file_name = "test_gtid";
    uint64_t test_file_size = 1024;

    RC ret;
    auto binlog = std::make_unique<MYSQL_BIN_LOG>(test_file_name, test_file_size, ret);
    LOFT_VERIFY(ret != RC::SUCCESS, "Failed to create binlog file.");

    ret = binlog->open();
    LOFT_VERIFY(ret != RC::SUCCESS, "Failed to open binlog file");

    long long int last_committed_arg = 30;
    long long int sequence_number_arg = 31;
    bool may_have_sbr_stmts_arg = true;
    unsigned long long int original_commit_timestamp_arg = 1722493959000068;
    unsigned long long int immediate_commit_timestamp_arg = 1722493961117679;

    auto ge = std::make_unique<Gtid_event>(last_committed_arg, sequence_number_arg, may_have_sbr_stmts_arg,
                                           original_commit_timestamp_arg, immediate_commit_timestamp_arg,
                                           ORIGINAL_SERVER_VERSION, IMMEDIATE_SERVER_VERSION);
    binlog->write_event_to_binlog(ge.get());

    binlog->close();
}

/**
 * @brief 测试 binlog 写入 Query 事件
 */
TEST(STATEMENT_EVENT_FORMAT_TEST, QUERY_EVENT) {
    const char *test_file_name = "test_query";
    uint64_t test_file_size = 1024;

    RC ret;
    auto binlog = std::make_unique<MYSQL_BIN_LOG>(test_file_name, test_file_size, ret);
    LOFT_VERIFY(ret != RC::SUCCESS, "Failed to create binlog file.");

    ret = binlog->open();
    LOFT_VERIFY(ret != RC::SUCCESS, "Failed to open binlog file");

    const char *query_arg = "create table t1 (id int)";
    const char *catalog_arg = nullptr;
    const char *db_arg = "t1"; // 假设没有的话，mysqlbinlog默认理解成
                               // mysql，所以会 use 'mysql'
    catalog_arg = db_arg;
    uint64_t ddl_xid_arg = 31;
    size_t query_length = strlen(query_arg);
    unsigned long thread_id_arg = 10000;             // 随意
    /// 这三个参数，暂时没用到
    unsigned long long sql_mode_arg = 0;             // 随意
    unsigned long auto_increment_increment_arg = 0;  // 随意
    unsigned long auto_increment_offset_arg = 0;     // 随意
    ///
    unsigned int number = 0;                         // 时区，0 表示 en-US
    unsigned long long table_map_for_update_arg = 0; // 只涉及单表 update，所以填 0
    int errcode = 0; // 默认不出错
    uint64 immediate_commit_timestamp_arg = 1722493961117679;

    auto qe = std::make_unique<Query_event>(  query_arg, catalog_arg, db_arg, ddl_xid_arg, query_length,
                                            thread_id_arg, sql_mode_arg, auto_increment_increment_arg,
                                            auto_increment_offset_arg, number, table_map_for_update_arg, errcode, immediate_commit_timestamp_arg);

    binlog->write_event_to_binlog(qe.get());

    binlog->close();
}

/**
 * @brief 测试 binlog 写入 Table_map 事件
 */
TEST(ROWS_EVENT_FORMAT_TEST, TABLE_MAP_EVENT) {
    const char *test_file_name = "test_table_map";
    uint64_t test_file_size = 1024;

    RC ret;
    auto binlog = std::make_unique<MYSQL_BIN_LOG>(test_file_name, test_file_size, ret);
    LOFT_VERIFY(ret != RC::SUCCESS, "Failed to create binlog file.");

    ret = binlog->open();
    LOFT_VERIFY(ret != RC::SUCCESS, "Failed to open binlog file");

    // 1. 查询 table_name 是否访问过， 如果没有， 就创建一个 Table_id 对象
    Table_id tid(13);
    // 2. 读 field's size()
    uint64 colCnt = 1;
    const char *dbName = "t1";
    const char *tblName = "t1";

    std::vector<mysql::FieldRef>         field_vec;
    auto field_obj = mysql::make_field(
        "a1", 0, false, false, 0, MYSQL_TYPE_LONG, 0, 0);
    field_vec.emplace_back(field_obj);

    uint64 immediate_commit_timestamp_arg = 1722493961117679;

    auto table_map_event =
        std::make_unique<Table_map_event>(tid, colCnt, dbName, strlen(dbName), tblName, strlen(tblName), field_vec, immediate_commit_timestamp_arg);
    binlog->write_event_to_binlog(table_map_event.get());

    binlog->close();
}

/**
 * @brief 测试 binlog 写入 insert sql 的 write row 事件
 *          insert t1 values(1); 向 t1 表中插入一行，有 1 个 column，int 类型
 */
TEST(ROWS_EVENT_FORMAT_TEST, WRITE_EVENT) {
  const char *test_file_name = "test_insert_row";
  uint64_t test_file_size = 1024;

  RC ret;
  auto binlog = std::make_unique<MYSQL_BIN_LOG>(test_file_name, test_file_size, ret);
  LOFT_VERIFY(ret != RC::SUCCESS, "Failed to create binlog file.");

  ret = binlog->open();
  LOFT_VERIFY(ret != RC::SUCCESS, "Failed to open binlog file");

  // TODO
  // 1. 查询 table_name 是否访问过， 如果没有， 就创建一个 Table_id 对象
  Table_id tid(13);
  // 2. 读 field's size()
  uint64 colCnt = 1;
  const char *dbName = "t1";
  const char *tblName = "t1";

  std::vector<mysql::FieldRef>         field_vec;
  auto field_obj = mysql::make_field(
      "a1", 0, false, false, 0, MYSQL_TYPE_LONG, 0, 0);
  field_vec.emplace_back(field_obj);

  uint64 immediate_commit_timestamp_arg = 1722493961117679;

  auto table_map_event =
      std::make_unique<Table_map_event>(tid, colCnt, dbName, strlen(dbName), tblName, strlen(tblName), field_vec, immediate_commit_timestamp_arg);
  binlog->write_event_to_binlog(table_map_event.get());

  auto insertRow = std::make_unique<Rows_event>(tid, colCnt, 1, WRITE_ROWS_EVENT, immediate_commit_timestamp_arg);  // 初始化 一个 rows_event 对象

  int data1 = 1;
  std::vector<int> rows{1};
  std::vector<uint8> rows_null{0};
  insertRow->set_rows_after(std::move(rows));
  insertRow->set_null_after(std::move(rows_null));
  insertRow->write_data_after(&data1, MYSQL_TYPE_LONG, 4, 0, 0, 0);


  binlog->write_event_to_binlog(insertRow.get());

  binlog->close();
}

/**
 * @brief 测试 binlog 写入 update sql 的 write row 事件
 *         update t1 set a1 = 10 where a1 = 1; 向 t1 表中更新一行，有一个 column，int 类型
 */
TEST(ROWS_EVENT_FORMAT_TEST, UPDATE_EVENT) {
  const char *test_file_name = "test_update_row";
  uint64_t test_file_size = 1024;

  RC ret;
  auto binlog = std::make_unique<MYSQL_BIN_LOG>(test_file_name, test_file_size, ret);
  LOFT_VERIFY(ret != RC::SUCCESS, "Failed to create binlog file.");

  ret = binlog->open();
  LOFT_VERIFY(ret != RC::SUCCESS, "Failed to open binlog file");

  // TODO
  Table_id tid(13);
  // 2. 读 field's size()
  uint64 colCnt = 1;
  const char *dbName = "t1";
  const char *tblName = "t1";

  std::vector<mysql::FieldRef>         field_vec;
  auto field_obj = mysql::make_field(
      "a1", 0, false, false, 0, MYSQL_TYPE_LONG, 0, 0);
  field_vec.emplace_back(field_obj);

  uint64 immediate_commit_timestamp_arg = 1722493961117679;

  auto table_map_event =
      std::make_unique<Table_map_event>(tid, colCnt, dbName, strlen(dbName), tblName, strlen(tblName), field_vec, immediate_commit_timestamp_arg);
  binlog->write_event_to_binlog(table_map_event.get());

  auto updateRow = std::make_unique<Rows_event>(tid, colCnt, 1, UPDATE_ROWS_EVENT, immediate_commit_timestamp_arg);  // 初始化 一个 rows_event 对象

  int newData1 = 10;
  std::vector<int> rows_after{1};
  std::vector<uint8> rows_null_after{0};
  updateRow->set_rows_after(std::move(rows_after));
  updateRow->set_null_after(std::move(rows_null_after));
  updateRow->write_data_after(&newData1, MYSQL_TYPE_LONG, 4, 0, 0, 0);

  int conditionData = 1;
  std::vector<int> rows_before{1};
  std::vector<uint8> rows_null_before{0};
  updateRow->set_rows_before(std::move(rows_before));
  updateRow->set_null_before(std::move(rows_null_before));
  updateRow->write_data_before(&conditionData, MYSQL_TYPE_LONG, 4, 0, 0, 0);


  binlog->write_event_to_binlog(updateRow.get());

  binlog->close();
}

/**
 * @brief 测试 binlog 写入 delete sql 的 write row 事件
 *          delete from t1 where a1 = 10
 */
TEST(ROWS_EVENT_FORMAT_TEST, DELETE_EVENT) {
  const char *test_file_name = "test_delete_row";
  uint64_t test_file_size = 1024;

  RC ret;
  auto binlog = std::make_unique<MYSQL_BIN_LOG>(test_file_name, test_file_size, ret);
  LOFT_VERIFY(ret != RC::SUCCESS, "Failed to create binlog file.");

  ret = binlog->open();
  LOFT_VERIFY(ret != RC::SUCCESS, "Failed to open binlog file");

  // TODO
  Table_id tid(13);
  // 2. 读 field's size()
  uint64 colCnt = 1;
  const char *dbName = "t1";
  const char *tblName = "t1";

  std::vector<mysql::FieldRef>         field_vec;
  auto field_obj = mysql::make_field(
      "a1", 0, false, false, 0, MYSQL_TYPE_LONG, 0, 0);
  field_vec.emplace_back(field_obj);

  uint64 immediate_commit_timestamp_arg = 1722493961117679;

  auto table_map_event =
      std::make_unique<Table_map_event>(tid, colCnt, dbName, strlen(dbName), tblName, strlen(tblName), field_vec, immediate_commit_timestamp_arg);
  binlog->write_event_to_binlog(table_map_event.get());

  auto deleteRow = std::make_unique<Rows_event>(tid, colCnt, 1, DELETE_ROWS_EVENT, immediate_commit_timestamp_arg);  // 初始化 一个 rows_event 对象

  int conditionData = 10;
  std::vector<int> rows_before{1};
  std::vector<uint8> rows_null_before{0};
  deleteRow->set_rows_before(std::move(rows_before));
  deleteRow->set_null_before(std::move(rows_null_before));
  deleteRow->write_data_before(&conditionData, MYSQL_TYPE_LONG, 4, 0, 0, 0);

  binlog->close();
}

/**
 * @brief 测试 binlog 写入 Xid 事件
 */
TEST(CONTROL_EVENT_FORMAT_TEST, XID_EVENT) {
  const char *test_file_name = "test_xid";
  uint64_t test_file_size = 1024;

  RC ret;
  auto binlog = std::make_unique<MYSQL_BIN_LOG>(test_file_name, test_file_size, ret);
  LOFT_VERIFY(ret != RC::SUCCESS, "Failed to create binlog file.");

  ret = binlog->open();
  LOFT_VERIFY(ret != RC::SUCCESS, "Failed to open binlog file");

  uint64 txSeq = 35;
  uint64 immediate_commit_timestamp_arg = 1722493961117679;

  auto xe = std::make_unique<Xid_event>(txSeq, immediate_commit_timestamp_arg);
  binlog->write_event_to_binlog(xe.get());

  binlog->close();
}

/**
 * @brief 测试 binlog 写入 Rotate 事件
 */
TEST(CONTROL_EVENT_FORMAT_TEST, ROTATE_EVENT) {
    const char *test_file_name = "test_rotate";
    uint64_t test_file_size = 1024;

    RC ret;
    auto binlog = std::make_unique<MYSQL_BIN_LOG>(test_file_name, test_file_size, ret);
    LOFT_VERIFY(ret != RC::SUCCESS, "Failed to create binlog file.");

    ret = binlog->open();
    LOFT_VERIFY(ret != RC::SUCCESS, "Failed to open binlog file");


    std::string next_binlog_file_name = "ON.000021";
    LOG_INFO("next binlog file_name len: %zu", next_binlog_file_name.length());

    auto re = std::make_unique<Rotate_event>(next_binlog_file_name.c_str(), next_binlog_file_name.length(),
                                             Rotate_event::DUP_NAME, 4);
    binlog->write_event_to_binlog(re.get());

    binlog->close();
}

