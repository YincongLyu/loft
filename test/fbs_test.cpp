#include "ddl_generated.h"
#include "file_manager.h"

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

TEST(SQL_TEST, DDL) {
    LogFormatTransformManager mgr;
    // 待解析的 数据
    auto [data, fileSize] = mgr.readFileAsBinary();

    auto reader = std::make_unique<MyReader>(data.get(), fileSize);

    auto sql_len = reader->read<uint32_t>();

    // 3. 初始化一片 内存空间
    std::vector<unsigned char> buf(sql_len);
    reader->memcpy<unsigned char *>(buf.data(), sql_len);
    // note！使用 flatbuffer 获取的对象，返回的是一个 raw ptr
    // 管理内存的方法不是使用 new/delete，所以不能直接转化成 unique_ptr
    const DDL *ddl = GetDDL(buf.data());
    if (ddl) {
        // Use the raw pointer directly
        mgr.transformDDL(ddl);
    } else {
        // Handle error cases
        std::cerr << "Failed to parse DDL object.\n";
    }

}

TEST(SQL_TEST, DDL_OR_DML) {

}
