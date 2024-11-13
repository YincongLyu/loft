#include <gtest/gtest.h>
#include <iostream>
#include <vector>

#include "format/ddl_generated.h"

#include "common/logging.h"
#include "common/macros.h"

#include "binlog.h"
#include "file_manager.h"


using namespace loft; // flatbuffer namespace

//TEST(DDL_TEST, DISABLED_CREATE_TABLE) {
//    LogFormatTransformManager mgr;
//    // 读二进制
//    auto create_db_sql = mgr.readSQLN(1);
//
//    // 获取指向vector数据的指针
//    const char *buf = create_db_sql.get();
//
//    // 使用GetDDL函数获取DDL对象
//    const DDL *ddl = GetDDL(buf);
//
//    if (ddl) {
//        // 使用 check_point() 方法获取 flatbuffers::String*
//        auto checkpoint = ddl->check_point();
//        std::cout << checkpoint->c_str() << '\n';
//    } else {
//        // 处理错误情况
//    }
//}

TEST(DDL_TEST, DISABLED_CREATE_DB_LEN) {
    LogFormatTransformManager mgr;
    // 待解析的 数据
    auto [data, fileSize] = mgr.readFileAsBinary();

    auto reader = std::make_unique<MyReader>(data.get(), fileSize);

    auto sql_len = reader->read<uint32_t>();

    EXPECT_EQ(sql_len, 248);
}

TEST(SQL_TEST, DISABLED_DDL_CREATE_DB_TABLE) {
    // 1. 新建一个 binlog 文件，开启写功能
    const char *test_file_name = "test1";
    uint64_t test_file_size = 1024;

    RC ret;
    auto binlog = std::make_unique<MYSQL_BIN_LOG>(test_file_name, test_file_size, ret);
    LOFT_VERIFY(ret != RC::FILE_CREATE, "Failed to create binlog file.");

    ret = binlog->open();
    LOFT_VERIFY(ret != RC::FILE_OPEN, "Failed to open binlog file");

    Format_description_event fde(BINLOG_VERSION, SERVER_VERSION_STR);
    binlog->write_event_to_binlog(&fde);

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
            mgr.transformDDL(ddl, binlog.get());
        } else {
            // Handle error cases
            std::cerr << "Failed to parse DDL object.\n";
        }
    }
    // 3. 关闭 binlog 文件流
    binlog->close();
}

TEST(SQL_TEST, DISABLED_CREATE_DB_TABLE_INSERT1) {
    // 1. 新建一个 binlog 文件，开启写功能
    const char *test_file_name = "test1";
    uint64_t test_file_size = 1024;

    RC ret;
    auto binlog = std::make_unique<MYSQL_BIN_LOG>(test_file_name, test_file_size, ret);
    LOFT_VERIFY(ret != RC::FILE_CREATE, "Failed to create binlog file.");

    ret = binlog->open();
    LOFT_VERIFY(ret != RC::FILE_OPEN, "Failed to open binlog file");

    Format_description_event fde(BINLOG_VERSION, SERVER_VERSION_STR);
    binlog->write_event_to_binlog(&fde);


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
            mgr.transformDDL(ddl, binlog.get());
        } else {
            // Handle error cases
            std::cerr << "Failed to parse DDL object.\n";
        }
    }
    //////////////////////////////////////
    // 接着读第 3 条 insert1
    auto insert_len = reader->read<uint32_t>();

    // 2.2. 进入转换流程，先初始化一片 内存空间， copy 出来
    std::vector<unsigned char> buf(insert_len);
    reader->memcpy<unsigned char *>(buf.data(), insert_len);

    const DML *dml = GetDML(buf.data());

    LOFT_ASSERT(dml, "Failed to parse DML object.");
    // Use the`  raw pointer directly
    mgr.transformDML(dml, binlog.get());


    // 3. 关闭 binlog 文件流
    binlog->close();
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

TEST(SQL_TEST, DISABLED_DML_INSERT) {
    // 1. 新建一个 binlog 文件，开启写功能
    const char *test_file_name = "test1";
    uint64_t test_file_size = 1024;

    RC ret;
    auto binlog = std::make_unique<MYSQL_BIN_LOG>(test_file_name, test_file_size, ret);
    LOFT_VERIFY(ret != RC::FILE_CREATE, "Failed to create binlog file.");

    ret = binlog->open();
    LOFT_VERIFY(ret != RC::FILE_OPEN, "Failed to open binlog file");
    // 先写 fde
    Format_description_event fde(BINLOG_VERSION, SERVER_VERSION_STR);
    binlog->write_event_to_binlog(&fde);

    LogFormatTransformManager mgr;
    // 2.1 把待解析的 数据 装入 reader 中，后续可以利用 read/cpy 方法
    auto [data, fileSize] = mgr.readFileAsBinary();
    auto reader = std::make_unique<MyReader>(data.get(), fileSize);

    // 跳过前 2 条
    const int SKIP_CNT = 2;
    size_t skip_sql_sz = 0;
    for (int i = 0; i < SKIP_CNT; i++)
        skip_sql_sz += SQL_SIZE_ARRAY[i];
    reader->forward(sizeof(uint32_t) * SKIP_CNT + skip_sql_sz);

    // 读第 345 条，确定是 insert sql
    int EPOCH = 3;
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
        mgr.transformDML(dml, binlog.get());
    }

    // 3. 关闭 binlog 文件流
    binlog->flush();
    binlog->close();
}

TEST(SQL_TEST, DISABLED_DML_UPDATE) {
    // 1. 新建一个 binlog 文件，开启写功能
    const char *test_file_name = "test1";
    uint64_t test_file_size = 1024;

    RC ret;
    auto binlog = std::make_unique<MYSQL_BIN_LOG>(test_file_name, test_file_size, ret);
    LOFT_VERIFY(ret != RC::FILE_CREATE, "Failed to create binlog file.");

    ret = binlog->open();
    LOFT_VERIFY(ret != RC::FILE_OPEN, "Failed to open binlog file");

    // 先写 fde
    Format_description_event fde(BINLOG_VERSION, SERVER_VERSION_STR);
    binlog->write_event_to_binlog(&fde);

    LogFormatTransformManager mgr;
    // 2.1 把待解析的 数据 装入 reader 中，后续可以利用 read/cpy 方法
    auto [data, fileSize] = mgr.readFileAsBinary();
    auto reader = std::make_unique<MyReader>(data.get(), fileSize);

    // 跳过前 5 条
    const int SKIP_CNT = 5;
    size_t skip_sql_sz = 0;
    for (int i = 0; i < SKIP_CNT; i++)
        skip_sql_sz += SQL_SIZE_ARRAY[i];
    reader->forward(sizeof(uint32_t) * SKIP_CNT + skip_sql_sz);

    // 读第 67 条，确定是 update sql
    int EPOCH = 2;
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
        mgr.transformDML(dml, binlog.get());
    }

    // 3. 关闭 binlog 文件流
    binlog->flush();
    binlog->close();
}

TEST(SQL_TEST, DISABLED_DML_DELETE) {
    // 1. 新建一个 binlog 文件，开启写功能
    const char *test_file_name = "test1";
    uint64_t test_file_size = 1024;

    RC ret;
    auto binlog = std::make_unique<MYSQL_BIN_LOG>(test_file_name, test_file_size, ret);
    LOFT_VERIFY(ret != RC::FILE_CREATE, "Failed to create binlog file.");

    ret = binlog->open();
    LOFT_VERIFY(ret != RC::FILE_OPEN, "Failed to open binlog file");
    // 先写 fde
    Format_description_event fde(BINLOG_VERSION, SERVER_VERSION_STR);
    binlog->write_event_to_binlog(&fde);

    LogFormatTransformManager mgr;
    // 2.1 把待解析的 数据 装入 reader 中，后续可以利用 read/cpy 方法
    auto [data, fileSize] = mgr.readFileAsBinary();
    auto reader = std::make_unique<MyReader>(data.get(), fileSize);

    // 跳过前 7 条
    const int SKIP_CNT = 7;
    size_t skip_sql_sz = 0;
    for (int i = 0; i < SKIP_CNT; i++)
        skip_sql_sz += SQL_SIZE_ARRAY[i];
    reader->forward(sizeof(uint32_t) * SKIP_CNT + skip_sql_sz);

    // 读第 8 条，确定是 delete sql
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
        mgr.transformDML(dml, binlog.get());
    }

    // 3. 关闭 binlog 文件流
    binlog->flush();
    binlog->close();
}

TEST(SQL_TEST, DISABLED_DDL_DROP_DB_TABLE) {
    // 1. 新建一个 binlog 文件，开启写功能
    const char *test_file_name = "test1";
    uint64_t test_file_size = 1024;

    RC ret;
    auto binlog = std::make_unique<MYSQL_BIN_LOG>(test_file_name, test_file_size, ret);
    LOFT_VERIFY(ret != RC::FILE_CREATE, "Failed to create binlog file.");

    ret = binlog->open();
    LOFT_VERIFY(ret != RC::FILE_OPEN, "Failed to open binlog file");

    LogFormatTransformManager mgr;
    // 2.1 把待解析的 数据 装入 reader 中，后续可以利用 read/cpy 方法
    auto [data, fileSize] = mgr.readFileAsBinary();
    auto reader = std::make_unique<MyReader>(data.get(), fileSize);

    // 跳过前 8 条
    const int SKIP_CNT = 8;
    size_t skip_sql_sz = 0;
    for (int i = 0; i < SKIP_CNT; i++)
        skip_sql_sz += SQL_SIZE_ARRAY[i];
    reader->forward(sizeof(uint32_t) * SKIP_CNT + skip_sql_sz);

    // 读第 9 和 第 10 条，已确定是 drop table 和 drop db sql
    int EPOCH = 2;
    for (int k = 0; k < EPOCH; k++) {
        auto sql_len = reader->read<uint32_t>();

        // 2.2. 进入转换流程，先初始化一片 内存空间， copy 出来
        std::vector<unsigned char> buf(sql_len);
        reader->memcpy<unsigned char *>(buf.data(), sql_len);
        // note！使用 flatbuffer 获取的对象，返回的是一个 raw ptr
        // 管理内存的方法不是使用 new/delete，所以不能直接转化成 unique_ptr
        const DDL *ddl = GetDDL(buf.data());

        LOFT_ASSERT(ddl, "Failed to parse DDL object.");

        mgr.transformDDL(ddl, binlog.get());

    }
    // 3. 关闭 binlog 文件流
    binlog->close();
}

TEST(FILE_TEST, OnlyInsertDMLSqlCnt) {
    // 从第 5 条开始 后面是 100w 条 INSERT
    const char *test_file_name = "test1";
    uint64_t test_file_size = 1024 * 1024 * 1024;

    RC ret;
    auto binlog = std::make_unique<MYSQL_BIN_LOG>(test_file_name, test_file_size, ret);
    LOFT_VERIFY(ret != RC::FILE_CREATE, "Failed to create binlog file.");

    ret = binlog->open();
    LOFT_VERIFY(ret != RC::FILE_OPEN, "Failed to open binlog file");

    auto startReadTime = std::chrono::high_resolution_clock::now(); // 记录开始时间

    auto mgr = std::make_unique<LogFormatTransformManager>();
    // 2.1 把待解析的 数据 装入 reader 中，后续可以利用 read/cpy 方法
    auto [data, fileSize] = mgr->readFileAsBinary();
    auto reader = std::make_unique<MyReader>(data.get(), fileSize);

    auto endReadTime = std::chrono::high_resolution_clock::now(); // 记录结束时间
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(endReadTime - startReadTime).count();
    LOG_DEBUG("Read File: %ld ms", duration);

    // 跳过前 4 条
    int SKIP_COUNT = 4;
    uint32_t skip_sql_sz = 0;
    for (int i = 0; i < SKIP_COUNT; i++) {
        skip_sql_sz = reader->read<uint32_t>();;
        reader->forward(skip_sql_sz);
    }

    // 统计 DML 真实有多少条
    int dml_cnt = 0;
    while (reader->valid()) {
        dml_cnt++;
        skip_sql_sz = reader->read<uint32_t>();;
        reader->forward(skip_sql_sz);
    }
    LOG_DEBUG("dml_cnt: %d", dml_cnt);

}

TEST(FILE_TEST, OnlyInsert100w) {
    // 1. 新建一个 binlog 文件，开启写功能

    const char *test_file_name = "test1";
    uint64_t test_file_size = 1024 * 1024 * 1024;

    RC ret;
    auto binlog = std::make_unique<MYSQL_BIN_LOG>(test_file_name, test_file_size, ret);
    LOFT_VERIFY(ret != RC::FILE_CREATE, "Failed to create binlog file.");

    ret = binlog->open();
    LOFT_VERIFY(ret != RC::FILE_OPEN, "Failed to open binlog file");

    auto fde = std::make_unique<Format_description_event>(BINLOG_VERSION, SERVER_VERSION_STR);
    binlog->write_event_to_binlog(fde.get());
    // ************* 计时 开始 ***********
    auto startTime = std::chrono::high_resolution_clock::now(); // 记录开始时间

    auto mgr = std::make_unique<LogFormatTransformManager>("/home/yincong/loft/data");
    // 2.1 把待解析的 数据 装入 reader 中，后续可以利用 read/cpy 方法
    auto [data, fileSize] = mgr->readFileAsBinary();
    auto reader = std::make_unique<MyReader>(data.get(), fileSize);

    // 目前是读前 3 条，确定是 ddl sql
    int DDLEPOCH = 3;
    for (int k = 0; k < DDLEPOCH; k++) {
        auto sql_len = reader->read<uint32_t>();

        // 2.2. 进入转换流程，先初始化一片 内存空间， copy 出来
        std::vector<unsigned char> buf(sql_len);
        reader->memcpy<unsigned char *>(buf.data(), sql_len);
        // note！使用 flatbuffer 获取的对象，返回的是一个 raw ptr
        // 管理内存的方法不是使用 new/delete，所以不能直接转化成 unique_ptr
        const DDL *ddl = GetDDL(buf.data());
        if (ddl) {
            // Use the raw pointer directly
            mgr->transformDDL(ddl, binlog.get());
        } else {
            // Handle error cases
            std::cerr << "Failed to parse DDL object.\n";
        }
    }

    auto ddlEndTime = std::chrono::high_resolution_clock::now(); // 记录结束时间
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(ddlEndTime - startTime).count();
    LOG_DEBUG("DDL execution time: %ld ms", duration);

    //////////////////////////////////////

    // 跳过第 4 条
    auto skip_sql_sz = reader->read<uint32_t>();;
    reader->forward(skip_sql_sz);

    // 接着读第 5-100w 条 insert1
    int DMLEPOCH = 703435;
    for (int k = 0; k < DMLEPOCH; k++) {
        auto insert_len = reader->read<uint32_t>();

        std::vector<unsigned char> buf(insert_len);
        reader->memcpy<unsigned char *>(buf.data(), insert_len);

        const DML *dml = GetDML(buf.data());

        LOFT_ASSERT(dml, "Failed to parse DML object.");
        // Use the`  raw pointer directly
        mgr->transformDML(dml, binlog.get());
    }
    auto dmlEndTime = std::chrono::high_resolution_clock::now(); // 记录结束时间
    duration = std::chrono::duration_cast<std::chrono::milliseconds>(dmlEndTime - ddlEndTime).count();
    LOG_DEBUG("DML execution time: %ld ms", duration);

    // 3. 关闭 binlog 文件流
    binlog->close();
}

TEST(Tranform_Test, OnlyWhile) {

}