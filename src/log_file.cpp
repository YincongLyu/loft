//
// Created by Coonger on 2024/11/10.
//

#include <fcntl.h>   // ::open
#include <charconv>  // std::from_chars
#include <string_view>  // std::string_view
#include <cstring> // std::strcmp

#include "log_file.h"
#include "buffer_reader.h"

auto RedoLogFileReader::open(const char *filename) -> RC {
    filename_ = filename;
    fd_ = ::open(filename, O_RDONLY);
    if (fd_ < 0) {
        LOG_ERROR("open file failed. filename=%s, error=%s", filename, strerror(errno));
        return RC::FILE_OPEN;
    }

    LOG_INFO("open file success. filename=%s, fd=%d", filename, fd_);
    return RC::SUCCESS;
}

auto RedoLogFileReader::close() -> RC {
    if (fd_ < 0) {
        return RC::FILE_NOT_OPENED;
    }

    ::close(fd_);
    fd_ = -1;
    return RC::SUCCESS;
}

auto RedoLogFileReader::readFromFile(const std::string &fileName)
    -> std::pair<std::unique_ptr<char[]>, size_t> {
    FILE *file = fopen(fileName.c_str(), "rb");
    if (file == nullptr) {
        std::cerr << "Failed to open file " << fileName << std::endl;
        return {nullptr, 0};  // 返回空指针和大小为0
    }

    const size_t bufferSize = IO_SIZE;  // 每次读取4KB数据
    char buffer[bufferSize];
    size_t readSize = 0;
    size_t oneRead = 0;

    // 动态缓冲区大小控制，通过unique_ptr管理data
    std::unique_ptr<char[]> data;
    size_t dataCapacity = 0;

    // 循环读取文件内容
    while (!feof(file)) {
        memset(buffer, 0, sizeof(buffer));
        oneRead = fread(buffer, 1, sizeof(buffer), file);
        if (ferror(file)) {
            std::cerr << "Failed to read data from " << fileName << std::endl;
            fclose(file);
            return {nullptr, 0};
        }

        // 如果当前读取大小超过 data 的容量，重新分配
        if (readSize + oneRead > dataCapacity) {
            dataCapacity = (readSize + oneRead) * 2;
            std::unique_ptr<char[]> newData(new char[dataCapacity]);

            if (data) {
                memcpy(newData.get(), data.get(), readSize);
            }
            data = std::move(newData);
        }

        memcpy(data.get() + readSize, buffer, oneRead);
        readSize += oneRead;
    }

    fclose(file);

    // 调整最终大小，使其准确匹配已读取的数据量
    std::unique_ptr<char[]> result(new char[readSize + 1]);
    memcpy(result.get(), data.get(), readSize);
    result[readSize] = '\0';

    return {std::move(result), readSize};
}
///////////////////////////////////////////////////
// fileWriter 的 open 和 close ，选择直接操作 文件流，而不是 fd
RC BinLogFileWriter::open(const char *filename, size_t max_file_size)
{
    filename_ = filename;
    // 这里仅是 初始化了文件信息，还没有 open 文件流
    RC ret;
    bin_log_ = std::make_unique<MYSQL_BIN_LOG>(filename, max_file_size, ret);
    LOFT_VERIFY(ret != RC::SUCCESS, "Failed to create binlog file.");
    // 直接返回 当前文件的 可写位置，相当于继续写
    return bin_log_->open(); // 正确返回 RC::SUCCESS
}

RC BinLogFileWriter::close()
{
    // 在 next_file 里调用，由于会先调用 close，所以这里可以直接返回
    // 只有外部第一次调用 open，才会初始化 bin_log_
    if (bin_log_ == nullptr) {
        LOG_DEBUG("At first time revoke last_file or next file");
        return RC::FILE_NOT_OPENED;
    }

    return bin_log_->close(); // 正确返回  RC::SUCCESS;
}

RC BinLogFileWriter::write(AbstractEvent &event) { return bin_log_->write_event_to_binlog(&event) ? RC::SUCCESS : RC::IOERR_EVENT_WRITE; }

///////////////////////////////////////////////////

RC LogFileManager::init(const char *directory, const char *file_name_prefix, uint64_t max_file_size_per_file) {

    directory_ = std::filesystem::absolute(std::filesystem::path(directory));
    file_prefix_ = file_name_prefix;
    max_file_size_per_file_ = max_file_size_per_file;

    // 检查目录是否存在，不存在就创建出来
    if (!std::filesystem::is_directory(directory_)) {
        LOG_INFO("directory is not exist. directory=%s", directory_.c_str());

        std::error_code ec;
        bool ret = std::filesystem::create_directories(directory_, ec);
        if (!ret) {
            LOG_ERROR("create directory failed. directory=%s, error=%s", directory_.c_str(), ec.message().c_str());
            return RC::FILE_CREATE;
        }
    }

    // 列出所有的日志文件
    for (const std::filesystem::directory_entry &dir_entry : std::filesystem::directory_iterator(directory_)) {
        if (!dir_entry.is_regular_file()) {
            continue;
        }

        std::string filename = dir_entry.path().filename().string();
        // TODO
        uint32_t fileno = 0;
        RC rc = get_fileno_from_filename(filename, fileno);
        if (LOFT_FAIL(rc)) {
            LOG_INFO("invalid log file name. filename=%s", filename.c_str());
            continue;
        }

        if (log_files_.find(fileno) != log_files_.end()) {
            LOG_INFO("duplicate log file. filename1=%s, filename2=%s",
                      filename.c_str(), log_files_.find(fileno)->second.filename().c_str());
            continue;
        }

        log_files_.emplace(fileno, dir_entry.path());
    }

    LOG_INFO("init log file manager success. directory=%s, log files=%d",
             directory_.c_str(), static_cast<int>(log_files_.size()));


    // 获得索引文件 句柄
    std::filesystem::path index_path = directory_ / (file_prefix_ + index_suffix_);
    index_fd_ = ::open(index_path.c_str(), O_RDWR | O_CREAT | O_APPEND, 0644);
    if (index_fd_ < 0) {
      LOG_ERROR("open file failed. filename=%s, error=%s", index_path.c_str(), strerror(errno));
      return RC::FILE_OPEN;
    }

    return RC::SUCCESS;
}

RC LogFileManager::get_fileno_from_filename(
    const std::string &filename, uint32_t &fileno
) {

    if (!filename.starts_with(file_prefix_)) {
        return RC::INVALID_ARGUMENT;
    }

    std::string_view lsn_str(filename.data() + strlen(file_prefix_) + 1, filename.length() - strlen(file_prefix_) - 1);
    std::from_chars_result result = std::from_chars(lsn_str.data(), lsn_str.data() + lsn_str.size(), fileno);
    if (result.ec != std::errc()) {
        LOG_INFO("invalid log file name: cannot calc lsn. filename=%s, error=%s",
                  filename.c_str(), strerror(static_cast<int>(result.ec)));
        return RC::INVALID_ARGUMENT;
    }

    return RC::SUCCESS;
}


RC LogFileManager::transform(const char *filename, bool is_ddl) {
    // reader 的成员变量等到 open 之后再初始化
    file_reader_->open(filename);

    auto [data, fileSize ] = file_reader_->readFromFile(filename);
    auto bufferReader = std::make_unique<BufferReader>(data.get(), fileSize);

    // 每次调用 transform 都默认从目录下最后一个文件继续写
    this->last_file(*file_writer_.get());
    LOG_DEBUG("last file write pos=%lu", file_writer_->get_binlog()->get_bytes_written());

    // 但这个其实很难控制，我们应该控制每个 文件可以处理 xxx 条 sql，保证总体在 限定bytes就可以，不然每次都要用 if 判断 是否需要切换文件
    if (is_ddl) {
        while (bufferReader->valid()) {
            auto sql_len = bufferReader->read<uint32_t>();

            // 2.2. 进入转换流程，先初始化一片 内存空间， copy 出来
            std::vector<unsigned char> buf(sql_len);
            bufferReader->memcpy<unsigned char *>(buf.data(), sql_len);
            // FIXME
            // 在这里每次转换一条 sql， 找时机切换到 next_file! 如果在这里每次判断，非常耗时
            if (!file_writer_->get_binlog()->remain_bytes_safe()) {
                this->next_file(*file_writer_.get());
            }
            const DDL *ddl = GetDDL(buf.data());
            transform_manager_->transformDDL(ddl, file_writer_->get_binlog());
        }
    } else {
        int dml_count = 0;
        while (bufferReader->valid()) {
            auto sql_len = bufferReader->read<uint32_t>();

            std::vector<unsigned char> buf(sql_len);
            bufferReader->memcpy<unsigned char *>(buf.data(), sql_len);
            // 在 1048 427 切换了，1024 * 1024 = 1048 576，写完之后 1048 488 差一点就超出了
            if (!file_writer_->get_binlog()->remain_bytes_safe()) {
                this->next_file(*file_writer_.get());
                LOG_DEBUG("write dml cnt=%d: revoke rotate", dml_count);
            }

            const DML *dml = GetDML(buf.data());
            transform_manager_->transformDML(dml, file_writer_->get_binlog());
            dml_count++;
//            if (dml_count % 500 == 0)
//                LOG_DEBUG("%lu", file_writer_->get_binlog()->get_bytes_written());
        }
//        LOG_DEBUG("done, dml count=%d", dml_count);
    }

    return RC::SUCCESS;
}

RC LogFileManager::transform(std::vector<unsigned char> &&buf, bool is_ddl) {

  if (!file_writer_->get_binlog()->remain_bytes_safe()) {
    this->next_file(*file_writer_.get());
  }

  if (is_ddl) {
    const DDL *ddl = GetDDL(buf.data());

    file_ckp_[last_file_no_] = ddl->check_point()->c_str();

    transform_manager_->transformDDL(ddl, file_writer_->get_binlog());
  } else {
    const DML *dml = GetDML(buf.data());
    file_ckp_[last_file_no_] = dml->check_point()->c_str();
    transform_manager_->transformDML(dml, file_writer_->get_binlog());
  }

  return RC::SUCCESS;
}

LogFileManager::LogFileManager()
    : file_reader_(std::make_unique<RedoLogFileReader>()),
    file_writer_(std::make_unique<BinLogFileWriter>()),
    transform_manager_(std::make_unique<LogFormatTransformManager>()) {
    // 其他初始化操作可以放在这里，比如加载已有日志文件的索引，设置初始状态等
}

RC LogFileManager::last_file(BinLogFileWriter &file_writer) {
    if (log_files_.empty()) {
        return next_file(file_writer);
    }

    file_writer.close();

    auto last_file_item = log_files_.rbegin();
    return file_writer.open(last_file_item->second.c_str(), max_file_size_per_file_);
}

RC LogFileManager::next_file(BinLogFileWriter &file_writer) {

    // 最小从 1 开始
    uint32_t fileno = log_files_.empty() ? 1 : log_files_.rbegin()->first + 1;

    last_file_no_ = fileno;

    std::ostringstream oss;
    oss << std::setw(6) << std::setfill('0') << fileno;
    file_suffix_ = oss.str();

    std::string nextFilename = file_prefix_ + std::string(file_dot_) + file_suffix_;
    std::filesystem::path next_file_path = directory_ / nextFilename;

    if (!log_files_.empty()) {
        auto nowFilename = log_files_.rbegin()->second.string();
        // 在上一个文件中，写入一个 rotate event 再关闭
        auto rotateEvent = std::make_unique<Rotate_event>(nowFilename, nowFilename.length(),
                                                          Rotate_event::DUP_NAME, 4);
        assert(rotateEvent != nullptr);
        file_writer.get_binlog()->write_event_to_binlog(rotateEvent.get());

        file_writer.close();
    }

    log_files_.emplace(fileno, next_file_path);
    // 写入索引文件
    write_filename2index(nextFilename);

    LOG_DEBUG("[==rotate file==]next file name = %s", next_file_path.c_str());

    return file_writer.open(next_file_path.c_str(), max_file_size_per_file_);
}

LogFileManager::~LogFileManager() {
  // 关闭 index_fd_
  if (index_fd_ >= 0) {
    ::close(index_fd_);
    index_fd_ = -1;
  }
}

RC LogFileManager::write_filename2index(std::string &filename) {

  filename += "\n";  // 添加换行符
  ssize_t write_len = write(index_fd_, filename.c_str(), filename.length());
  if (write_len != static_cast<ssize_t>(filename.length())) {
    LOG_ERROR("Failed to write to index file, expected %zu bytes, wrote %zd bytes, error: %s",
                 filename.length(), write_len, strerror(errno));
    return RC::IOERR_WRITE;
  }

  return RC::SUCCESS;
}

RC LogFileManager::get_last_status_from_filename(
    const std::string &filename, uint64 &scn, uint32 &seq, std::string &ckp)
{

  // 找到. 符号的位置
  std::string numberString;
  size_t dotPos = filename.find('.');
  if (dotPos != std::string::npos && filename.substr(0, dotPos) == DEFAULT_BINLOG_FILE_NAME_PREFIX ) {
    numberString = filename.substr(dotPos + 1);
  } else {
    LOG_ERROR("binlog file name format error");
    return RC::FILE_NOT_EXIST;
  }
  // 将数字字符串转换为整数
  uint32 file_no = static_cast<uint32_t>(std::strtoul(numberString.c_str(), nullptr, 10));

  // 处理 ckp，返回查询值
  ckp = file_ckp_[file_no];
  std::string input = ckp;

  std::string delimiter = "-";
  size_t pos = 0;
  std::string token;
  int count = 0;
  uint64 numbers[2]; // 用于存储提取的数字

  while ((pos = input.find(delimiter)) != std::string::npos) {
    token = input.substr(0, pos);
    numbers[count] = std::stoi(token); // 将字符串转换为整数
    input.erase(0, pos + delimiter.length());
    count++;
    if (count == 2) break; // 已经提取了 scn 和 seq
  }

  scn = numbers[0];
  seq = static_cast<uint32>(numbers[1]);
  return RC::SUCCESS;
}
