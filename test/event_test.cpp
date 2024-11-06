//
// Created by Coonger on 2024/10/17.
//
#include "binlog.h"
#include "control_events.h"
#include "rows_event.h"
#include "statement_events.h"
#include "table_id.h"

#include "logging.h"
#include "macros.h"

#include <fstream>
#include <gtest/gtest.h>
#include <iomanip>

// 利用 自带的 Basic_ostream 写
TEST(WRITE_BINLOG_FILE_TEST, DISABLED_WRITE_MAGIC_NUMBER) {
    // Create a MYSQL_BIN_LOG instance
    MYSQL_BIN_LOG binlog(new Binlog_ofile());

    // Define the test parameters
    const char *test_file_name = "test_magic";
    uint64_t test_file_size = 1024;

    // Step 2: Open the binlog file
    if (!binlog.open(test_file_name, test_file_size)) {
        std::cerr << "Failed to open binlog file." << std::endl;
    }

    std::cout << "Binlog file opened successfully." << std::endl;

    // Step 3: Verify that the magic number was written (assuming the logic
    // writes it correctly)

    // Step 4: Close the binlog file
    binlog.close();
    std::cout << "Binlog file closed successfully." << std::endl;
}

TEST(CONTROL_EVENT_FORMAT_TEST, DISABLED_FORMAT_DESCRIPTION_EVENT) {
    MYSQL_BIN_LOG binlog(new Binlog_ofile());
    const char *test_file_name = "test_magic_fde";
    uint64_t test_file_size = 1024;

    // Step 2: Open the binlog file
    if (!binlog.open(test_file_name, test_file_size)) {
        std::cerr << "Failed to open binlog file." << std::endl;
    }

    std::cout << "Binlog file opened successfully." << std::endl;

    Format_description_event fde(4, "8.0.26");
    binlog.write_event_to_binlog(&fde);

    binlog.close();
}

TEST(CONTROL_EVENT_FORMAT_TEST, DISABLED_OPEN_NEW_BINLOG) {
    MYSQL_BIN_LOG binlog(new Binlog_ofile());
    const char *test_file_name = "test_2_default_event";
    uint64_t test_file_size = 1024;

    if (!binlog.open(test_file_name, test_file_size)) {
        std::cerr << "Failed to open binlog file." << std::endl;
    }

    std::cout << "Binlog file opened successfully." << std::endl;

    Format_description_event fde(4, "8.0.26");
    binlog.write_event_to_binlog(&fde);

    std::cout << "write fde successfully." << std::endl;
    const Gtid_set *gtid_set = new Gtid_set(new Sid_map());
    Previous_gtids_event pge(gtid_set);
    binlog.write_event_to_binlog(&pge);

    binlog.close();
}

TEST(CONTROL_EVENT_FORMAT_TEST, DISABLED_GTID_EVENT) {
    MYSQL_BIN_LOG binlog(new Binlog_ofile());
    const char *test_file_name = "test_gtid";
    uint64_t test_file_size = 1024;

    if (!binlog.open(test_file_name, test_file_size)) {
        std::cerr << "Failed to open binlog file." << std::endl;
    }

    std::cout << "Binlog file opened successfully." << std::endl;

    long long int last_committed_arg = 30;
    long long int sequence_number_arg = 31;
    bool may_have_sbr_stmts_arg = true;
    unsigned long long int original_commit_timestamp_arg = 1722493959000068;
    unsigned long long int immediate_commit_timestamp_arg = 1722493961117679;
    uint32_t original_server_version = 80026;
    uint32_t imm_server_version = 80026;
    Gtid_event ge(
        last_committed_arg, sequence_number_arg, may_have_sbr_stmts_arg,
        original_commit_timestamp_arg, immediate_commit_timestamp_arg,
        original_server_version, imm_server_version
    );
    binlog.write_event_to_binlog(&ge);

    binlog.close();
}

TEST(STATEMENT_EVENT_FORMAT_TEST, DISABLED_QUERY_EVENT) {
    MYSQL_BIN_LOG binlog(new Binlog_ofile());
    const char *test_file_name = "test_query";
    uint64_t test_file_size = 1024;

    if (!binlog.open(test_file_name, test_file_size)) {
        std::cerr << "Failed to open binlog file." << std::endl;
    }

    std::cout << "Binlog file opened successfully." << std::endl;

    const char *query_arg = "create table t1 (id int)";
    const char *catalog_arg = nullptr;
    const char *db_arg = "t1"; // 假设没有的话，mysqlbinlog默认理解成
                               // mysql，所以会 use 'mysql'
    catalog_arg = db_arg;
    uint64_t ddl_xid_arg = 31;
    uint32_t query_length = strlen(query_arg);
    unsigned long thread_id_arg = 10000;
    unsigned long long sql_mode_arg = 0;             // 随意
    unsigned long auto_increment_increment_arg = 0;  // 随意
    unsigned long auto_increment_offset_arg = 0;     // 随意
    unsigned int number = 0;                         // 一定要
    unsigned long long table_map_for_update_arg = 0; // 随意
    int errcode = 0;

    Query_event *qe = new Query_event(
        query_arg, catalog_arg, db_arg, ddl_xid_arg, query_length,
        thread_id_arg, sql_mode_arg, auto_increment_increment_arg,
        auto_increment_offset_arg, number, table_map_for_update_arg, errcode
    );

    binlog.write_event_to_binlog(qe);

    LOG_INFO("this is a info log");
    LOG_DEBUG("this is a debug log");

    binlog.close();
}

TEST(ROWS_EVENT_FORMAT_TEST, TABLE_MAP_EVENT) {
    MYSQL_BIN_LOG binlog(new Binlog_ofile());
    const char *test_file_name = "test_table_map";
    uint64_t test_file_size = 1024;

    if (!binlog.open(test_file_name, test_file_size)) {
        std::cerr << "Failed to open binlog file." << std::endl;
    }

    std::cout << "Binlog file opened successfully." << std::endl;

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

    binlog.close();
}

TEST(CONTROL_EVENT_FORMAT_TEST, DISABLED_ROTATE_EVENT) {
    MYSQL_BIN_LOG binlog(new Binlog_ofile());
    const char *test_file_name = "test_rotate";
    uint64_t test_file_size = 1024;

    if (!binlog.open(test_file_name, test_file_size)) {
        std::cerr << "Failed to open binlog file." << std::endl;
    }

    std::cout << "Binlog file opened successfully." << std::endl;


    std::string next_binlog_file_name = "ON.000021";
    Rotate_event* rotateEvent = new Rotate_event(next_binlog_file_name.c_str(), next_binlog_file_name.length(),
                                                Rotate_event::DUP_NAME, 4);
    binlog.write_event_to_binlog(rotateEvent);

    binlog.close();

}


TEST(EVENT_FORMAT_TEST, DISABLED_PRINT_BINARY_FILE_TO_HEX) {
    std::string filename = "test_magic_fde";
    std::ifstream file(filename, std::ios::binary);
    if (!file) {
        std::cerr << "无法打开文件: " << filename << std::endl;
        return;
    }

    unsigned char buffer[16];
    int bytesRead;
    int lineCount = 0;

    while ((bytesRead =
                file.read(reinterpret_cast<char *>(buffer), sizeof(buffer))
                    .gcount())
           > 0) {
        std::cout << std::hex << std::setfill('0') << std::setw(4)
                  << lineCount * 16 << ": ";

        for (int i = 0; i < bytesRead; ++i) {
            std::cout << std::setw(2) << static_cast<int>(buffer[i]) << ' ';
        }

        std::cout << std::endl;
        ++lineCount;
    }

    file.close();
}
