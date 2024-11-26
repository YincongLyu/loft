#include <gtest/gtest.h>
#include <iostream>
#include <vector>

#include "format/ddl_generated.h"

#include "common/logging.h"
#include "common/macros.h"

#include "binlog.h"
#include "transform_manager.h"
#include "buffer_reader.h"
#include "log_file.h"
#include "utils/base64.h"

using namespace loft; // flatbuffer namespace

/**
 * @brief 1. 测试 RedoLogFileReader 的 readFromFile 方法 & BufferReader 的 read 方法
 *        2. 验证读 DDL sql，create db 字段格式能否正确解析, 缺少 dbName 和 table 字段，共 11 个字段
 */
TEST(DDL_TEST, CREATE_DB) {
  std::string file_name = "/home/yincong/loft/testDataDir/data1-10";
  auto reader = std::make_unique<RedoLogFileReader>();
  auto [data, fileSize] = reader->readFromFile(file_name);
  auto bufferReader = std::make_unique<BufferReader>(data.get(), fileSize);

  auto sql_len = bufferReader->read<uint32_t>();
  EXPECT_EQ(sql_len, 248);

  std::vector<unsigned char> buf(sql_len);
  bufferReader->memcpy<unsigned char *>(buf.data(), sql_len);
  auto ddl = GetDDL(buf.data());

  auto ckp = ddl->check_point();
  EXPECT_STREQ(ckp->c_str(), "31-1-54348795023361");

  auto dbName = ddl->db_name();
  EXPECT_TRUE(dbName == nullptr);

  auto ddlSql = ddl->ddl_sql();
  EXPECT_STREQ(ddlSql->c_str(), "create database t1");

  auto ddlType = ddl->ddl_type();
  EXPECT_STREQ(ddlType->c_str(), "CREATE TABLE");

  auto lastCommit = ddl->last_commit();
  EXPECT_EQ(lastCommit, 30);

  auto msgTime = ddl->msg_time();
  EXPECT_STREQ(msgTime->c_str(), "2024-08-01 14:32:41.000054");

  auto opType = ddl->op_type();
  EXPECT_STREQ(opType->c_str(), "DDL");

  auto scn = ddl->scn();
  EXPECT_EQ(scn, 54348795023361);

  auto seq = ddl->seq();
  EXPECT_EQ(seq, 1);

  auto table = ddl->table_();
  EXPECT_TRUE(table == nullptr);

  auto txSeq = ddl->tx_seq();
  EXPECT_EQ(txSeq, 31);

  auto txTime = ddl->tx_time();
  EXPECT_STREQ(txTime->c_str(), "2024-08-01 14:32:39.000068");

}

/**
 * @brief 测试 读 DDL sql，create table 字段格式能否正确解析，共 13 个完整字段都有数据
 */
TEST(DDL_TEST, CREATE_TABLE) {
  std::string file_name = "/home/yincong/loft/testDataDir/data1-10";
  auto reader = std::make_unique<RedoLogFileReader>();
  auto [data, fileSize] = reader->readFromFile(file_name);

  auto bufferReader = std::make_unique<BufferReader>(data.get(), fileSize);

  uint32 sql_len;
  int SKIP_CNT = 1;
  for (int k = 0; k < SKIP_CNT; k++) {
    sql_len = bufferReader->read<uint32_t>();
    bufferReader->forward(sql_len);
  }

  sql_len = bufferReader->read<uint32_t>();
  EXPECT_EQ(sql_len, 744);

  std::vector<unsigned char> buf(sql_len);
  bufferReader->memcpy<unsigned char *>(buf.data(), sql_len);
  auto ddl = GetDDL(buf.data());

  auto ckp = ddl->check_point();
  EXPECT_STREQ(ckp->c_str(), "33-1-54349172944897");

  auto dbName = ddl->db_name();
  EXPECT_STREQ(dbName->c_str(), "t1");

  auto ddlSql = ddl->ddl_sql();
  EXPECT_STREQ(ddlSql->c_str(), "create table t1(a1 int primary key, a2 char(20),a3 bit(23), a4 smallint, a5 smallint unsigned, a6 mediumint, a7 mediumint unsigned, a8 int unsigned, a9 bigint, a10 bigint unsigned, a11 float(10,5), a12 float(10,5) unsigned, a13 double(20,10), a14 double(20,10) unsigned, a15 decimal(10,5), a16 decimal(10,5) unsigned, a17 year(4), a18 enum('aa','bb','cc'), a19 set('dd','ee','ff'), a20 tinytext, a21 text, a22 mediumtext, a23 longtext, a24 tinyblob, a25 blob, a26 mediumblob, a27 longblob)");

  auto ddlType = ddl->ddl_type();
  EXPECT_STREQ(ddlType->c_str(), "CREATE TABLE");

  auto lastCommit = ddl->last_commit();
  EXPECT_EQ(lastCommit, 32);

  auto lsn = ddl->lsn();
  EXPECT_EQ(lsn, 279711);

  auto msgTime = ddl->msg_time();
  EXPECT_STREQ(msgTime->c_str(), "2024-08-01 14:32:41.000117");

  auto opType = ddl->op_type();
  EXPECT_STREQ(opType->c_str(), "DDL");

  auto scn = ddl->scn();
  EXPECT_EQ(scn, 54349172944897);

  auto seq = ddl->seq();
  EXPECT_EQ(seq, 1);

  auto table = ddl->table_();
  EXPECT_STREQ(table->c_str(), "temp");

  auto txSeq = ddl->tx_seq();
  EXPECT_EQ(txSeq, 33);

  auto txTime = ddl->tx_time();
  EXPECT_STREQ(txTime->c_str(), "2024-08-01 14:32:39.000160");

}

/**
 * @brief 测试 读 DDL sql，drop table 字段格式能否正确解析，共 13 个完整字段都有数据
 */
TEST(DDL_TEST, DROP_TABLE) {
  std::string file_name = "/home/yincong/loft/testDataDir/data1-10";
  auto reader = std::make_unique<RedoLogFileReader>();
  auto [data, fileSize] = reader->readFromFile(file_name);
  auto bufferReader = std::make_unique<BufferReader>(data.get(), fileSize);
  // 跳过前 8 条 sql
  uint32 sql_len;
  int SKIP_CNT = 8;
  for (int k = 0; k < SKIP_CNT; k++) {
    sql_len = bufferReader->read<uint32_t>();
    bufferReader->forward(sql_len);
  }

  sql_len = bufferReader->read<uint32_t>();
  EXPECT_EQ(sql_len, 264);

  std::vector<unsigned char> buf(sql_len);
  bufferReader->memcpy<unsigned char *>(buf.data(), sql_len);
  auto ddl = GetDDL(buf.data());

  auto ckp = ddl->check_point();
  EXPECT_STREQ(ckp->c_str(), "43-1-54350345428993");

  auto dbName = ddl->db_name();
  EXPECT_STREQ(dbName->c_str(), "t1");

  auto ddlSql = ddl->ddl_sql();
  EXPECT_STREQ(ddlSql->c_str(), "drop table t1");

  auto ddlType = ddl->ddl_type();
  EXPECT_STREQ(ddlType->c_str(), "DROP TABLE");

  auto lastCommit = ddl->last_commit();
  EXPECT_EQ(lastCommit, 42);

  auto lsn = ddl->lsn();
  EXPECT_EQ(lsn, 280191);

  auto msgTime = ddl->msg_time();
  EXPECT_STREQ(msgTime->c_str(), "2024-08-01 14:32:41.000156");

  auto opType = ddl->op_type();
  EXPECT_STREQ(opType->c_str(), "DDL");

  auto scn = ddl->scn();
  EXPECT_EQ(scn, 54350345428993);

  auto seq = ddl->seq();
  EXPECT_EQ(seq, 1);

  auto table = ddl->table_();
  EXPECT_STREQ(table->c_str(), "temp");

  auto txSeq = ddl->tx_seq();
  EXPECT_EQ(txSeq, 43);

  auto txTime = ddl->tx_time();
  EXPECT_STREQ(txTime->c_str(), "2024-08-01 14:32:39.000446");

}

/**
 * @brief 测试 读 DDL sql，drop table 字段格式能否正确解析，缺少 dbName, ddlType, table, 共 10 个字段
 */
TEST(DDL_TEST, DROP_DB) {
  std::string file_name = "/home/yincong/loft/testDataDir/data1-10";
  auto reader = std::make_unique<RedoLogFileReader>();
  auto [data, fileSize] = reader->readFromFile(file_name);
  auto bufferReader = std::make_unique<BufferReader>(data.get(), fileSize);
  // 跳过前 9 条 sql
  uint32 sql_len;
  int SKIP_CNT = 9;
  for (int k = 0; k < SKIP_CNT; k++) {
    sql_len = bufferReader->read<uint32_t>();
    bufferReader->forward(sql_len);
  }

  sql_len = bufferReader->read<uint32_t>();
  EXPECT_EQ(sql_len, 224);

  std::vector<unsigned char> buf(sql_len);
  bufferReader->memcpy<unsigned char *>(buf.data(), sql_len);
  auto ddl = GetDDL(buf.data());

  auto ckp = ddl->check_point();
  EXPECT_STREQ(ckp->c_str(), "46-1-54350647873537");

  auto dbName = ddl->db_name();
  EXPECT_TRUE(dbName == nullptr);

  auto ddlSql = ddl->ddl_sql();
  EXPECT_STREQ(ddlSql->c_str(), "drop database t1");

  auto ddlType = ddl->ddl_type();
  EXPECT_TRUE(ddlType == nullptr);

  auto lastCommit = ddl->last_commit();
  EXPECT_EQ(lastCommit, 45);

  auto lsn = ddl->lsn();
  EXPECT_EQ(lsn, 281581);

  auto msgTime = ddl->msg_time();
  EXPECT_STREQ(msgTime->c_str(), "2024-08-01 14:32:41.000157");

  auto opType = ddl->op_type();
  EXPECT_STREQ(opType->c_str(), "DDL");

  auto scn = ddl->scn();
  EXPECT_EQ(scn, 54350647873537);

  auto seq = ddl->seq();
  EXPECT_EQ(seq, 1);

  auto table = ddl->table_();
  EXPECT_TRUE(table == nullptr);

  auto txSeq = ddl->tx_seq();
  EXPECT_EQ(txSeq, 46);

  auto txTime = ddl->tx_time();
  EXPECT_STREQ(txTime->c_str(), "2024-08-01 14:32:39.000520");


}

/**
 * @brief 连续转换 2 条 DDL sql: create db + create table，并验证是否能回放成功
 */
TEST(SQL_TEST, DDL_CREATE_DB_TABLE) {
    // 1. 读数据到 buffer 中
    std::string filename = "/home/yincong/loft/testDataDir/data1-10";
    auto logFileManager = std::make_unique<LogFileManager>();
    logFileManager->init(DEFAULT_BINLOG_FILE_DIR, DEFAULT_BINLOG_FILE_NAME_PREFIX, DEFAULT_BINLOG_FILE_SIZE);

    auto fileReader = logFileManager->get_file_reader();
    fileReader->open(filename.c_str());
    auto [data, fileSize] = fileReader->readFromFile(filename);
    auto bufferReader     = std::make_unique<BufferReader>(data.get(), fileSize);

    // 2. 打开最后一个 binlog 文件，准备写
    auto fileWriter = logFileManager->get_file_writer();
    logFileManager->last_file(*fileWriter);

    // 目前是读前 2 条，确定是 ddl sql
    int EPOCH = 2;
    for (int k = 0; k < EPOCH; k++) {
        auto sql_len = bufferReader->read<uint32_t>();

        // 2.2. 进入转换流程，先初始化一片 内存空间， copy 出来
        std::vector<unsigned char> buf(sql_len);
        bufferReader->memcpy<unsigned char *>(buf.data(), sql_len);

        logFileManager->transform(std::move(buf), true);
    }
    // 3. 关闭 binlog 文件流
    fileWriter->close();
}
/**
 * @brief 连续转换 3 条 sql，前 2 条是 DDL sql，后 1 条是 DML sql，验证是否能回放成功
 */
TEST(SQL_TEST, CREATE_DB_TABLE_INSERT1) {
    // 1. 新建一个 binlog 文件，开启写功能
    std::string filename = "/home/yincong/loft/testDataDir/data1-10";
    auto logFileManager = std::make_unique<LogFileManager>();
    logFileManager->init(DEFAULT_BINLOG_FILE_DIR, DEFAULT_BINLOG_FILE_NAME_PREFIX, DEFAULT_BINLOG_FILE_SIZE);

    auto fileReader = logFileManager->get_file_reader();
    fileReader->open(filename.c_str());
    auto [data, fileSize] = fileReader->readFromFile(filename);
    auto bufferReader     = std::make_unique<BufferReader>(data.get(), fileSize);

    // 2. 打开最后一个 binlog 文件，准备写
    auto fileWriter = logFileManager->get_file_writer();
    logFileManager->last_file(*fileWriter);

    // 目前是读前 2 条，确定是 ddl sql
    int EPOCH = 2;
    for (int k = 0; k < EPOCH; k++) {
        auto sql_len = bufferReader->read<uint32_t>();

        // 2.2. 进入转换流程，先初始化一片 内存空间， copy 出来
        std::vector<unsigned char> buf(sql_len);
        bufferReader->memcpy<unsigned char *>(buf.data(), sql_len);

        logFileManager->transform(std::move(buf), true);
    }
    //////////////////////////////////////
    // 接着读第 3 条 insert1
    auto insert_len = bufferReader->read<uint32_t>();

    std::vector<unsigned char> buf(insert_len);
    bufferReader->memcpy<unsigned char *>(buf.data(), insert_len);
    logFileManager->transform(std::move(buf), false);

    // 3. 关闭 binlog 文件流
    fileWriter->close();
}

/**
 * @brief 验证读 DML insert2 sql，字段格式能否正确解析 [newData] 缺少 a12 a19 a20
 *        update / delete sql 的逻辑一致，其中insert2 最具有代表性（keys 和 newData 字段解析结构是相同的，都是 kvPairs，insert2的newData里有null类型）
 *        主要是验证 [fields]:嵌套 FieldMeta 和 [newData]: value有long, double string, null四个类型
 */
TEST(DML_TEST, INSERT2) {
  // 1. 读数据到 buffer 中
  std::string filename = "/home/yincong/loft/testDataDir/data1-10";
  auto logFileManager = std::make_unique<LogFileManager>();
  logFileManager->init(DEFAULT_BINLOG_FILE_DIR, DEFAULT_BINLOG_FILE_NAME_PREFIX, DEFAULT_BINLOG_FILE_SIZE);

  auto fileReader = logFileManager->get_file_reader();
  fileReader->open(filename.c_str());
  auto [data, fileSize] = fileReader->readFromFile(filename);
  auto bufferReader     = std::make_unique<BufferReader>(data.get(), fileSize);

  // 跳过前 3 条
  int SKIP_CNT = 3;
  for (int k = 0; k < SKIP_CNT; k++) {
    auto sql_len = bufferReader->read<uint32_t>();
    bufferReader->forward(sql_len);
  }

  auto sql_len = bufferReader->read<uint32_t>();
  EXPECT_EQ(sql_len, 3208);

  std::vector<unsigned char> buf(sql_len);
  bufferReader->memcpy<unsigned char *>(buf.data(), sql_len);
 const DML *dml = GetDML(buf.data());
  // ************* 填数据 begin ************************
 auto ckp = dml->check_point();
  EXPECT_STREQ(ckp->c_str(), "38-1-54349495054337");

  auto dbName = dml->db_name();
  EXPECT_EQ(std::strcmp(dbName->c_str(), "t1"), 0);

  auto dn = dml->dn();
  EXPECT_EQ(dn, 0);

  auto fields = dml->fields();
  EXPECT_EQ(fields->size(), 27);
  // ************* check 第一个 fields [字段名，fieldmeta(整数，bool，字符串)]************************
  auto field1 = fields->Get(0);
  auto fieldMeta = field1->meta();
  EXPECT_STREQ(field1->name()->c_str(), "a1");
  EXPECT_EQ(fieldMeta->length(), 0);
  EXPECT_EQ(fieldMeta->is_unsigned(), false);
  EXPECT_EQ(fieldMeta->nullable(), false);
  EXPECT_STREQ(fieldMeta->data_type()->c_str(), "INT");
  EXPECT_EQ(fieldMeta->precision(), 0);

  // insert 没有 keys 要判断一下
  auto keys = dml->keys();
  EXPECT_TRUE(keys == nullptr);

  auto lastCommit = dml->last_commit();
  EXPECT_EQ(lastCommit, 33);

  auto lsn = dml->lsn();
  EXPECT_EQ(lsn, 279792);

  auto immediateCommitTs = dml->msg_time();
  EXPECT_STREQ(immediateCommitTs->c_str(), "2024-08-01 14:32:41.000145");
  // ************* check newData ************************
  auto newData = dml->new_data();
  EXPECT_EQ(newData->size(), 27); // 注意，这里还是 27 个，只是 null 数值的没有显示，但在二进制内容中还占位

  // ************* newData[a11] 是 double 类型 ************************
  auto newData11 = newData->Get(0);
  EXPECT_STREQ(newData11->key()->c_str(), "a11");
  EXPECT_DOUBLE_EQ(newData11->value_as_DoubleVal()->value(), 3.402820110321045);

  // ************* newData[a10] 是 long 类型 ************************
  auto newData10 = newData->Get(1);
  EXPECT_STREQ(newData10->key()->c_str(), "a10");
  EXPECT_EQ(newData10->value_as_LongVal()->value(), -1);

  // ************* newData[a15] 是 string 类型，是 decimal 的字符串表示 ********
  auto newData15 = newData->Get(4);
  EXPECT_STREQ(newData15->key()->c_str(), "a15");
  EXPECT_STREQ(newData15->value_as_StringVal()->value()->c_str(), "3.40282");

  // ************* newData[a2] 是 string 类型，是 mysql 字符类型的 base64 加密表示， 还要 base64明文编码出来***
  auto newData2 = newData->Get(19);
  EXPECT_STREQ(newData2->key()->c_str(), "a2");
  const char *value = newData2->value_as_StringVal()->value()->c_str();

  char *dst = (char *)malloc(base64_needed_decoded_length(strlen(value)));
  int64_t dst_len = base64_decode(value, strlen(value), (void *)dst, nullptr, 0);
  EXPECT_STREQ(dst, "a");
  EXPECT_EQ(dst_len, 1);

  // ************* newData[a12] 是 null 类型 ************************
  auto newData12 = newData->Get(3);
  EXPECT_STREQ(newData12->key()->c_str(), "a12");
  EXPECT_TRUE(newData12->value() == nullptr);

  auto opType = dml->op_type();
  EXPECT_STREQ(opType->c_str(), "I");

  auto scn = dml->scn();
  EXPECT_EQ(scn, 54349495054337);

  auto table = dml->table_();
  EXPECT_STREQ(table->c_str(), "t1");

  auto seqNo = dml->tx_seq();
  EXPECT_EQ(seqNo, 38);

  auto originalCommitTs = dml->tx_time();
  EXPECT_STREQ(originalCommitTs->c_str(), "2024-08-01 14:32:39.000238");

  free(dst);
}

/**
 * @brief 连续转换 3 条 DML insert sql，并验证是否能回放成功
 */
TEST(SQL_TEST, DML_INSERT) {
  // 1. 新建一个 binlog 文件，开启写功能
  std::string filename = "/home/yincong/loft/testDataDir/data1-10";
  auto logFileManager = std::make_unique<LogFileManager>();
  logFileManager->init(DEFAULT_BINLOG_FILE_DIR, DEFAULT_BINLOG_FILE_NAME_PREFIX, DEFAULT_BINLOG_FILE_SIZE);

  auto fileReader = logFileManager->get_file_reader();
  fileReader->open(filename.c_str());
  auto [data, fileSize] = fileReader->readFromFile(filename);
  auto bufferReader     = std::make_unique<BufferReader>(data.get(), fileSize);

  // 2. 打开最后一个 binlog 文件，准备写
  auto fileWriter = logFileManager->get_file_writer();
  logFileManager->last_file(*fileWriter);

  // 跳过前 2 条
  uint32 sql_len;
  int SKIP_CNT = 2;
  for (int k = 0; k < SKIP_CNT; k++) {
    sql_len = bufferReader->read<uint32_t>();
    bufferReader->forward(sql_len);
  }

  // 读第 345 条，确定是 insert sql
  int EPOCH = 3;
  for (int k = 0; k < EPOCH; k++) {
      sql_len = bufferReader->read<uint32_t>();

      std::vector<unsigned char> buf(sql_len);
      bufferReader->memcpy<unsigned char *>(buf.data(), sql_len);

      logFileManager->transform(std::move(buf), false);
  }
  // 3. 关闭 binlog 文件流
  fileWriter->close();
}

/**
 * @brief 连续转换 2 条 DML update sql，并验证是否能回放成功
 */
TEST(SQL_TEST, DML_UPDATE) {
  // 1. 新建一个 binlog 文件，开启写功能
  std::string filename = "/home/yincong/loft/testDataDir/data1-10";
  auto logFileManager = std::make_unique<LogFileManager>();
  logFileManager->init(DEFAULT_BINLOG_FILE_DIR, DEFAULT_BINLOG_FILE_NAME_PREFIX, DEFAULT_BINLOG_FILE_SIZE);

  auto fileReader = logFileManager->get_file_reader();
  fileReader->open(filename.c_str());
  auto [data, fileSize] = fileReader->readFromFile(filename);
  auto bufferReader     = std::make_unique<BufferReader>(data.get(), fileSize);

  // 2. 打开最后一个 binlog 文件，准备写
  auto fileWriter = logFileManager->get_file_writer();
  logFileManager->last_file(*fileWriter);

  // 跳过前 5 条
  uint32 sql_len;
  int SKIP_CNT = 5;
  for (int k = 0; k < SKIP_CNT; k++) {
    sql_len = bufferReader->read<uint32_t>();
    bufferReader->forward(sql_len);
  }

  // 读第 67 条，确定是 update sql
  int EPOCH = 2;
  for (int k = 0; k < EPOCH; k++) {
      sql_len = bufferReader->read<uint32_t>();

      std::vector<unsigned char> buf(sql_len);
      bufferReader->memcpy<unsigned char *>(buf.data(), sql_len);

      logFileManager->transform(std::move(buf), false);
  }
  // 3. 关闭 binlog 文件流
  fileWriter->close();
}

/**
 * @brief 转换 1 条 DML insert sql，并验证是否能回放成功
 */
TEST(SQL_TEST, DML_DELETE) {
  // 1. 新建一个 binlog 文件，开启写功能
  std::string filename = "/home/yincong/loft/testDataDir/data1-10";
  auto logFileManager = std::make_unique<LogFileManager>();
  logFileManager->init(DEFAULT_BINLOG_FILE_DIR, DEFAULT_BINLOG_FILE_NAME_PREFIX, DEFAULT_BINLOG_FILE_SIZE);

  auto fileReader = logFileManager->get_file_reader();
  fileReader->open(filename.c_str());
  auto [data, fileSize] = fileReader->readFromFile(filename);
  auto bufferReader     = std::make_unique<BufferReader>(data.get(), fileSize);

  // 2. 打开最后一个 binlog 文件，准备写
  auto fileWriter = logFileManager->get_file_writer();
  logFileManager->last_file(*fileWriter);

  // 跳过前 7 条
  uint32 sql_len;
  int SKIP_CNT = 7;
  for (int k = 0; k < SKIP_CNT; k++) {
    sql_len = bufferReader->read<uint32_t>();
    bufferReader->forward(sql_len);
  }


  // 读第 8 条，确定是 delete sql
  int EPOCH = 1;
  for (int k = 0; k < EPOCH; k++) {
      sql_len = bufferReader->read<uint32_t>();

      std::vector<unsigned char> buf(sql_len);
      bufferReader->memcpy<unsigned char *>(buf.data(), sql_len);

      logFileManager->transform(std::move(buf), false);
  }

  // 3. 关闭 binlog 文件流
  fileWriter->close();
}

/**
 * @brief 连续转换 2 条 DDL sql: drop table + drop db，并验证是否能回放成功
 */
TEST(SQL_TEST, DDL_DROP_DB_TABLE) {
  // 1. 新建一个 binlog 文件，开启写功能
  std::string filename = "/home/yincong/loft/testDataDir/data1-10";
  auto logFileManager = std::make_unique<LogFileManager>();
  logFileManager->init(DEFAULT_BINLOG_FILE_DIR, DEFAULT_BINLOG_FILE_NAME_PREFIX, DEFAULT_BINLOG_FILE_SIZE);

  auto fileReader = logFileManager->get_file_reader();
  fileReader->open(filename.c_str());
  auto [data, fileSize] = fileReader->readFromFile(filename);
  auto bufferReader     = std::make_unique<BufferReader>(data.get(), fileSize);

  // 2. 打开最后一个 binlog 文件，准备写
  auto fileWriter = logFileManager->get_file_writer();
  logFileManager->last_file(*fileWriter);

  // 跳过前 8 条
  uint32 sql_len;
  int SKIP_CNT = 8;
  for (int k = 0; k < SKIP_CNT; k++) {
    sql_len = bufferReader->read<uint32_t>();
    bufferReader->forward(sql_len);
  }

  // 读第 9 和 第 10 条，已确定是 drop table 和 drop db sql
  int EPOCH = 2;
  for (int k = 0; k < EPOCH; k++) {
      sql_len = bufferReader->read<uint32_t>();

      std::vector<unsigned char> buf(sql_len);
      bufferReader->memcpy<unsigned char *>(buf.data(), sql_len);
      logFileManager->transform(std::move(buf), true);

  }
  // 3. 关闭 binlog 文件流
  fileWriter->close();
}

