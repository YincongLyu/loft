//
// Created by Coonger on 2024/11/4.
//

#pragma once

#include <cstring>       // std::string
#include <filesystem>    // std::filesystem::path>
#include <system_error>  // std::error_code
#include <utility>       // std::pair
#include <map>           // std::map

#include "transform_manager.h"
#include "binlog.h"
#include "events/abstract_event.h"
#include "common/init_setting.h"
#include "common/rc.h"

namespace loft {

/**
 * @brief 负责处理一个日志文件，包括读取和写入
 */
class RedoLogFileReader
{
public:
  RedoLogFileReader()  = default;
  ~RedoLogFileReader() {
    close();
  }

  auto open(const char *filename) -> RC;
  auto close() -> RC;
  auto readFromFile(const std::string &fileName) -> std::pair<std::unique_ptr<char[]>, size_t>;

private:
  int         fd_ = -1;
  std::string filename_;
};

/**
 * @brief 负责写入一个日志文件， 【封装 我写的 MYSQL_BIN_LOG 类】
 */
class BinLogFileWriter
{
public:
  BinLogFileWriter()  = default;
  ~BinLogFileWriter() = default;

  /**
   * @brief 打开一个日志文件
   * @param filename 日志文件名
   */
  RC open(const char *filename, size_t max_file_size);

  /// @brief 关闭当前文件
  RC close();

  /// @brief 写入一条 event
  RC write(AbstractEvent &event);

  /**
   * @brief 文件是否已经写满。按照剩余空间来判断
   */
  bool full() const;

  //    string to_string() const;

  const char *filename() const { return filename_.c_str(); }

  auto get_binlog() -> MYSQL_BIN_LOG * { return bin_log_.get(); }

private:
  std::string filename_;  /// 日志文件名
                          //    int fd_ = -1;      /// 日志文件描述符
  int last_ckp_ = 0;      /// 写入的最后一条SQL的scn, seq, ckp

  std::unique_ptr<MYSQL_BIN_LOG> bin_log_;  /// 封装的 MYSQL_BIN_LOG 类
};

/**
 * @brief 管理所有的 binlog 日志文件, 【封装我的 mgr 类】
 * @details binlog 日志文件都在某个目录下，使用固定的前缀 作为文件名如
 * ON.000001。 每个 binlog 日志文件有最大字节数要求
 */
class LogFileManager
{
public:
  LogFileManager();
  ~LogFileManager();

  // TODO 此处 实现 3 个 必要接口

  /// 接口一：
  /**
   * @brief 初始化写入 binlog 文件的目录路径 和 文件大小
   *
   * @param directory 日志文件目录
   * @param file_name_prefix 日志文件前缀
   * @param max_file_size_per_file 一个文件的最大字节数
   */
  RC init(const char *directory, const char *file_name_prefix, uint64_t max_file_size_per_file);

  /// 接口二：
  RC transform(const char *file_name, bool is_ddl);
  RC transform(std::vector<unsigned char> &&buf, bool is_ddl);

  /// 接口三：
  /**
   * @brief 从文件名称的后缀中获取这是第几个 binlog 文件，先去除前导0
   * @details 如果日志文件名不符合要求，就返回失败。实际上返回 3 个
   * scn、seq、ckp 字段，只用保存 ckp，在调用 next_file() 时，就可以得到，当前的 ckp
   */
  RC get_last_status_from_filename(const std::string &filename, uint64 &scn, uint32 &seq, std::string &ckp);

  RC get_fileno_from_filename(const std::string &filename, uint32 &fileno);

  /**
   * @brief 获取最新的一个日志文件名
   * @details
   * 如果当前有文件就获取最后一个日志文件，否则创建一个日志文件，也就是第一个日志文件
   */
  RC last_file(BinLogFileWriter &file_writer);

  /**
   * @brief 获取一个新的日志文件名
   * @details
   * 获取下一个日志文件名。通常是上一个日志文件写满了，通过这个接口生成下一个日志文件
   */
  RC next_file(BinLogFileWriter &file_writer);

  RC write_filename2index(std::string &filename);

  auto get_directory() -> const char * { return directory_.c_str(); }
  auto get_file_prefix() -> const char * { return file_prefix_; }
  auto get_file_max_size() -> size_t { return max_file_size_per_file_; }

  auto get_log_files() -> std::map<uint32, std::filesystem::path> & { return log_files_; }
  auto get_file_reader() -> RedoLogFileReader * { return file_reader_.get(); }
  auto get_file_writer() -> BinLogFileWriter * { return file_writer_.get(); }
  auto get_transform_manager() -> LogFormatTransformManager * { return transform_manager_.get(); }

private:
  const char *file_prefix_ = DEFAULT_BINLOG_FILE_NAME_PREFIX;
  const char *file_dot_    = ".";
  std::string file_suffix_;  // 这会是一个递增的后缀数字

  std::string index_suffix_ = ".index";
  int index_fd_ = -1; // init()后，就打开 index 文件

  std::filesystem::path directory_              = DEFAULT_BINLOG_FILE_DIR;   /// 日志文件存放的目录
  size_t                max_file_size_per_file_ = DEFAULT_BINLOG_FILE_SIZE;  /// 一个文件的最大字节数

  std::map<uint32, std::filesystem::path> log_files_;  /// file_no 和 日志文件名 的映射
  std::unordered_map<uint32, std::string> file_ckp_; /// file_no 和 ckp 的映射
  uint32 last_file_no_;

  std::unique_ptr<RedoLogFileReader>         file_reader_;
  std::unique_ptr<BinLogFileWriter>          file_writer_;
  std::unique_ptr<LogFormatTransformManager> transform_manager_;
};
}  // namespace loft
