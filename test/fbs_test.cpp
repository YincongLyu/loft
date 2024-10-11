#include "ddl_generated.h"
#include "utils/file_manager.h"

#include <gtest/gtest.h>
#include <iostream>
#include <vector>
using namespace loft;

TEST(DDLTEST, CREATE_TABLE) {
    // 读二进制
    std::vector<char> create_db_sql = utils::readSQLN(1);

    // 获取指向vector数据的指针
    const char* buf = create_db_sql.data();

    // 使用GetDDL函数获取DDL对象
    const DDL* ddl = GetDDL(buf);
   
    if (ddl) {
        // 使用 check_point() 方法获取 flatbuffers::String*
        const flatbuffers::String* checkpoint = ddl->check_point();
        std::cout << checkpoint->c_str() << '\n';
    } else {
        // 处理错误情况
    }

}