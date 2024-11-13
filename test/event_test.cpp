//
// Created by Coonger on 2024/10/17.
//

#include <gtest/gtest.h>
#include <fstream>
#include <iomanip>

#include "events/control_events.h"
#include "events/rows_event.h"
#include "events/statement_events.h"

#include "common/logging.h"
#include "common/macros.h"

#include "binlog.h"
#include "utils/table_id.h"

// 利用 自带的 Basic_ostream 写
TEST(WRITE_BINLOG_FILE_TEST, DISABLED_WRITE_MAGIC_NUMBER) {
    const char *test_file_name = "test_magic";
    uint64_t test_file_size = 1024;

    RC ret;
    auto binlog = std::make_unique<MYSQL_BIN_LOG>(test_file_name, test_file_size, ret);
    LOFT_VERIFY(ret != RC::FILE_CREATE, "Failed to create binlog file.");

    ret = binlog->open();
    LOFT_VERIFY(ret != RC::FILE_OPEN, "Failed to open binlog file");


    // Step 3: Verify that the magic number was written (assuming the logic
    // writes it correctly)

    // Step 4: Close the binlog file
    binlog->close();
    std::cout << "Binlog file closed successfully." << std::endl;
}

TEST(CONTROL_EVENT_FORMAT_TEST, DISABLED_FORMAT_DESCRIPTION_EVENT) {
    const char *test_file_name = "test_magic_fde";
    uint64_t test_file_size = 1024;

    RC ret;
    auto binlog = std::make_unique<MYSQL_BIN_LOG>(test_file_name, test_file_size, ret);
    LOFT_VERIFY(ret != RC::FILE_CREATE, "Failed to create binlog file.");

    ret = binlog->open();
    LOFT_VERIFY(ret != RC::FILE_OPEN, "Failed to open binlog file");

    auto fde = std::make_unique<Format_description_event>(4, "8.0.26");
    binlog->write_event_to_binlog(fde.get());

    binlog->close();
}

TEST(CONTROL_EVENT_FORMAT_TEST, DISABLED_OPEN_NEW_BINLOG) {
    const char *test_file_name = "test_2_default_event";
    uint64_t test_file_size = 1024;

    RC ret;
    auto binlog = std::make_unique<MYSQL_BIN_LOG>(test_file_name, test_file_size, ret);
    LOFT_VERIFY(ret != RC::FILE_CREATE, "Failed to create binlog file.");

    ret = binlog->open();
    LOFT_VERIFY(ret != RC::FILE_OPEN, "Failed to open binlog file");

    auto fde = std::make_unique<Format_description_event>(4, "8.0.26");

    const Gtid_set *gtid_set = new Gtid_set(new Sid_map());
    auto pge = std::make_unique<Previous_gtids_event>(gtid_set);

    binlog->write_event_to_binlog(fde.get());
    std::cout << "write fde successfully." << std::endl;
    binlog->write_event_to_binlog(pge.get());

    binlog->close();
}

TEST(CONTROL_EVENT_FORMAT_TEST, GTID_EVENT) {
    const char *test_file_name = "test_gtid";
    uint64_t test_file_size = 1024;

    RC ret;
    auto binlog = std::make_unique<MYSQL_BIN_LOG>(test_file_name, test_file_size, ret);
    LOFT_VERIFY(ret != RC::FILE_CREATE, "Failed to create binlog file.");

    ret = binlog->open();
    LOFT_VERIFY(ret != RC::FILE_OPEN, "Failed to open binlog file");

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

TEST(STATEMENT_EVENT_FORMAT_TEST, DISABLED_QUERY_EVENT) {
    const char *test_file_name = "test_query";
    uint64_t test_file_size = 1024;

    RC ret;
    auto binlog = std::make_unique<MYSQL_BIN_LOG>(test_file_name, test_file_size, ret);
    LOFT_VERIFY(ret != RC::FILE_CREATE, "Failed to create binlog file.");

    ret = binlog->open();
    LOFT_VERIFY(ret != RC::FILE_OPEN, "Failed to open binlog file");

    const char *query_arg = "create table t1 (id int)";
    const char *catalog_arg = nullptr;
    const char *db_arg = "t1"; // 假设没有的话，mysqlbinlog默认理解成
                               // mysql，所以会 use 'mysql'
    catalog_arg = db_arg;
    uint64_t ddl_xid_arg = 31;
    size_t query_length = strlen(query_arg);
    unsigned long thread_id_arg = 10000;
    unsigned long long sql_mode_arg = 0;             // 随意
    unsigned long auto_increment_increment_arg = 0;  // 随意
    unsigned long auto_increment_offset_arg = 0;     // 随意
    unsigned int number = 0;                         // 一定要
    unsigned long long table_map_for_update_arg = 0; // 随意
    int errcode = 0;

    auto qe = std::make_unique<Query_event>(  query_arg, catalog_arg, db_arg, ddl_xid_arg, query_length,
                                            thread_id_arg, sql_mode_arg, auto_increment_increment_arg,
                                            auto_increment_offset_arg, number, table_map_for_update_arg, errcode);

    binlog->write_event_to_binlog(qe.get());

    binlog->close();
}

TEST(ROWS_EVENT_FORMAT_TEST, TABLE_MAP_EVENT) {
    const char *test_file_name = "test_table_map";
    uint64_t test_file_size = 1024;

    RC ret;
    auto binlog = std::make_unique<MYSQL_BIN_LOG>(test_file_name, test_file_size, ret);
    LOFT_VERIFY(ret != RC::FILE_CREATE, "Failed to create binlog file.");

    ret = binlog->open();
    LOFT_VERIFY(ret != RC::FILE_OPEN, "Failed to open binlog file");

    // 1. 查询 table_name 是否访问过， 如果没有， 就创建一个 Table_id 对象，插入
    // mgr 的uamp
    Table_id tid(13);
    // 2. 读 field's size()
    unsigned long colCnt = 27;
    const char *dbName = "t1";
    size_t dbLen = strlen(dbName);
    const char *tblName = "t1";
    size_t tblLen = strlen(tblName);

    //    Table_map_event tme(tid, colCnt, dbName, dbLen, tblName, tblLen);
    //
    //    binlog.write_event_to_binlog(&tme);

    binlog->close();
}

TEST(CONTROL_EVENT_FORMAT_TEST, DISABLED_ROTATE_EVENT) {
    const char *test_file_name = "test_rotate";
    uint64_t test_file_size = 1024;

    RC ret;
    auto binlog = std::make_unique<MYSQL_BIN_LOG>(test_file_name, test_file_size, ret);
    LOFT_VERIFY(ret != RC::FILE_CREATE, "Failed to create binlog file.");

    ret = binlog->open();
    LOFT_VERIFY(ret != RC::FILE_OPEN, "Failed to open binlog file");


    std::string next_binlog_file_name = "ON.000021";
    LOG_INFO("next binlog file_name len: %zu", next_binlog_file_name.length());

    auto re = std::make_unique<Rotate_event>(next_binlog_file_name.c_str(), next_binlog_file_name.length(),
                                             Rotate_event::DUP_NAME, 4);
    binlog->write_event_to_binlog(re.get());

    binlog->close();
}

