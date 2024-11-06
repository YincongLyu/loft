#include "binlog.h"
#include "ddl_generated.h"
#include "file_manager.h"
#include "logging.h"
#include "macros.h"

#include <gtest/gtest.h>
#include <iostream>
#include <vector>

using namespace loft;

TEST(DDL_TEST, DISABLED_CREATE_TABLE) {
    LogFormatTransformManager mgr;
    // 读二进制
    auto create_db_sql = mgr.readSQLN(1);

    // 获取指向vector数据的指针
    const char *buf = create_db_sql.get();

    // 使用GetDDL函数获取DDL对象
    const DDL *ddl = GetDDL(buf);

    if (ddl) {
        // 使用 check_point() 方法获取 flatbuffers::String*
        auto checkpoint = ddl->check_point();
        std::cout << checkpoint->c_str() << '\n';
    } else {
        // 处理错误情况
    }
}

TEST(DDL_TEST, DISABLED_CREATE_DB_LEN) {
    LogFormatTransformManager mgr;
    // 待解析的 数据
    auto [data, fileSize] = mgr.readFileAsBinary();

    auto reader = std::make_unique<MyReader>(data.get(), fileSize);

    auto sql_len = reader->read<uint32_t>();

    EXPECT_EQ(sql_len, 248);
}

TEST(SQL_TEST, DISABLED_DDL) {
    // 1. 新建一个 binlog 文件，开启写功能
    MYSQL_BIN_LOG binlog(new Binlog_ofile());
    const char *test_file_name = "test_query_ddl";
    uint64_t test_file_size = 1024;

    LOFT_ASSERT(
        binlog.open(test_file_name, test_file_size),
        "Failed to open binlog file."
    );

    // 2. mgr 转换工具负责 读待解析的中间 flatbuffer 数据，然后调用
    // 响应的转换函数

    LogFormatTransformManager mgr;
    // 2.1 把待解析的 数据 装入 reader 中，后续可以利用 read/cpy 方法
    auto [data, fileSize] = mgr.readFileAsBinary();
    auto reader = std::make_unique<MyReader>(data.get(), fileSize);

    // 目前是读前 2 条，确定是 ddl sql
    int EPOCH = 2;
    for (int k = 0; k < EPOCH; k++) {
        auto sql_len = reader->read<uint32_t>();

        // 2.2. 进入转换流程，先初始化一片 内存空间， copy 出来
        std::vector<unsigned char> buf(sql_len);
        reader->memcpy<unsigned char *>(buf.data(), sql_len);
        // note！使用 flatbuffer 获取的对象，返回的是一个 raw ptr
        // 管理内存的方法不是使用 new/delete，所以不能直接转化成 unique_ptr
        const DDL *ddl = GetDDL(buf.data());
        if (ddl) {
            // Use the raw pointer directly
            mgr.transformDDL(ddl, &binlog);
        } else {
            // Handle error cases
            std::cerr << "Failed to parse DDL object.\n";
        }
    }
    // 3. 关闭 binlog 文件流
    binlog.close();
}

TEST(DML_TEST, DISABLED_INSERT1) {
    LogFormatTransformManager mgr;
    // 待解析的 数据
    auto [data, fileSize] = mgr.readFileAsBinary();
    auto reader = std::make_unique<MyReader>(data.get(), fileSize);
    // 跳过前 2 条
    reader->forward(
        sizeof(uint32_t) * 2 + SQL_SIZE_ARRAY[0] + SQL_SIZE_ARRAY[1]
    );

    auto sql_len = reader->read<uint32_t>();

    EXPECT_EQ(sql_len, 3304);

    std::vector<unsigned char> buf(sql_len);
    reader->memcpy<unsigned char *>(buf.data(), sql_len);
    // note！使用 flatbuffer 获取的对象，返回的是一个 raw ptr
    // 管理内存的方法不是使用 new/delete，所以不能直接转化成 unique_ptr
    const DDL *ddl = GetDDL(buf.data());
    const DML *dml = GetDML(buf.data());

    if (ddl) {
        // Use the raw pointer directly
        mgr.transformDDL(ddl);
    } else {
        // Handle error cases
        std::cerr << "Failed to parse DDL object.\n";

    const DML *dml = GetDML(buf.data());
    // ************* 填数据 begin ************************
    auto dbName = dml->db_name();
    EXPECT_EQ(std::strcmp(dbName->c_str(), "t1"), 0);

    auto fields = dml->fields();
    EXPECT_EQ(fields->size(), 27);

    std::vector<std::unique_ptr<Field>> field_vec;
    for (const auto &field : *fields) {
        field->name();
        auto fieldMeta = field->meta();
        fieldMeta->length();
        fieldMeta->is_unsigned();
        fieldMeta->nullable();
        fieldMeta->data_type(); // 根据 这里的类型，构建 对应的 Field 对象
        fieldMeta->precision();
    }

    // insert 没有 keys 要判断一下
    auto keys = dml->keys();
    if (keys) {
        std::cout << "insert sql not comes here" << std::endl;
    }

    auto lastCommit = dml->last_commit();
    EXPECT_EQ(lastCommit, 33);

    auto immediateCommitTs = dml->msg_time();

    auto newData = dml->new_data();

    auto opType = dml->op_type();
    EXPECT_EQ(std::strcmp(opType->c_str(), "I"), 0);

    auto table = dml->table_();
    EXPECT_EQ(std::strcmp(table->c_str(), "t1"), 0);

    auto seqNo = dml->tx_seq();
    EXPECT_EQ(seqNo, 35);

    auto originalCommitTs = dml->tx_time();
}

TEST(SQL_TEST, DML) {
    // 1. 新建一个 binlog 文件，开启写功能
    MYSQL_BIN_LOG binlog(new Binlog_ofile());
    const char *test_file_name = "test_query_dml";
    uint64_t test_file_size = 1024;

    LOFT_ASSERT(
        binlog.open(test_file_name, test_file_size),
        "Failed to open binlog file."
    );

    LogFormatTransformManager mgr;
    // 2.1 把待解析的 数据 装入 reader 中，后续可以利用 read/cpy 方法
    auto [data, fileSize] = mgr.readFileAsBinary();
    auto reader = std::make_unique<MyReader>(data.get(), fileSize);
    // 跳过前 2 条
    reader->forward(
        sizeof(uint32_t) * 2 + SQL_SIZE_ARRAY[0] + SQL_SIZE_ARRAY[1]
    );

    // 读第 3 条，确定是 insert sql
    int EPOCH = 1;
    for (int k = 0; k < EPOCH; k++) {
        auto sql_len = reader->read<uint32_t>();

        // 2.2. 进入转换流程，先初始化一片 内存空间， copy 出来
        std::vector<unsigned char> buf(sql_len);
        reader->memcpy<unsigned char *>(buf.data(), sql_len);
        // note！使用 flatbuffer 获取的对象，返回的是一个 raw ptr
        // 管理内存的方法不是使用 new/delete，所以不能直接转化成 unique_ptr
        const DML *dml = GetDML(buf.data());

        LOFT_ASSERT(dml, "Failed to parse DML object.");
        // Use the`  raw pointer directly
        mgr.transformDML(dml, &binlog);
    }
    // 3. 关闭 binlog 文件流
    binlog.flush();
    binlog.close();
}
