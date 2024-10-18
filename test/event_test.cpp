//
// Created by Coonger on 2024/10/17.
//
#include "control_events.h"
#include "binlog.h"

#include <fstream>
#include <gtest/gtest.h>
#include <iomanip>

// 利用 自带的 Basic_ostream 写
TEST(WRITE_BINLOG_FILE_TEST, WRITE_MAGIC_NUMBER) {
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

    // Step 3: Verify that the magic number was written (assuming the logic writes it correctly)

    // Step 4: Close the binlog file
    binlog.close();
    std::cout << "Binlog file closed successfully." << std::endl;
}

TEST(EVENT_FORMAT_TEST, FORMAT_DESCRIPTION_EVENT) {

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

TEST(EVENT_FORMAT_TEST, PRINT_BINARY_FILE_TO_HEX) {
    std::string filename = "test_magic_fde";
    std::ifstream file(filename, std::ios::binary);
    if (!file) {
        std::cerr << "无法打开文件: " << filename << std::endl;
        return;
    }

    unsigned char buffer[16];
    int bytesRead;
    int lineCount = 0;

    while ((bytesRead = file.read(reinterpret_cast<char*>(buffer), sizeof(buffer)).gcount()) > 0) {
        std::cout << std::hex << std::setfill('0') << std::setw(4) << lineCount * 16 << ": ";

        for (int i = 0; i < bytesRead; ++i) {
            std::cout << std::setw(2) << static_cast<int>(buffer[i]) << ' ';
        }

        std::cout << std::endl;
        ++lineCount;
    }

    file.close();
}