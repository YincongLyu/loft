//
// Created by Coonger on 2024/11/10.
//

#include <fcntl.h> // ::open

#include "common/lang/string_view.h"
#include "common/lang/charconv.h"
#include "log_file.h"
//#include "common/io/io.h"


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

auto RedoLogFileReader::readFromFile(const string &fileName)
    -> std::pair<std::unique_ptr<char[]>, size_t> {
    FILE *file = fopen(fileName.c_str(), "rb");
    if (file == nullptr) {
        std::cerr << "Failed to open file " << fileName << std::endl;
        return {nullptr, 0};  // 返回空指针和大小为0
    }

    const size_t bufferSize = 4096;  // 每次读取4KB数据
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
    LOFT_VERIFY(ret != RC::FILE_CREATE, "Failed to create binlog file.");
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


///////////////////////////////////////////////////

RC LogFileManager::init(const char *directory, const char *file_name_prefix, uint64_t max_file_size_per_file) {

    directory_ = filesystem::absolute(filesystem::path(directory));
    file_prefix_ = file_name_prefix;
    max_file_size_per_file_ = max_file_size_per_file;

    // 检查目录是否存在，不存在就创建出来
    if (!filesystem::is_directory(directory_)) {
        LOG_INFO("directory is not exist. directory=%s", directory_.c_str());

        error_code ec;
        bool ret = filesystem::create_directories(directory_, ec);
        if (!ret) {
            LOG_ERROR("create directory failed. directory=%s, error=%s", directory_.c_str(), ec.message().c_str());
            return RC::FILE_CREATE;
        }
    }

    // 列出所有的日志文件
    for (const filesystem::directory_entry &dir_entry : filesystem::directory_iterator(directory_)) {
        if (!dir_entry.is_regular_file()) {
            continue;
        }

        string filename = dir_entry.path().filename().string();
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
    return RC::SUCCESS;
}

RC LogFileManager::get_fileno_from_filename(
    const string &filename, uint32_t &fileno
) {

    if (!filename.starts_with(file_prefix_)) {
        return RC::INVALID_ARGUMENT;
    }

    string_view lsn_str(filename.data() + strlen(file_prefix_) + 1, filename.length() - strlen(file_prefix_) - 1);
    from_chars_result result = from_chars(lsn_str.data(), lsn_str.data() + lsn_str.size(), fileno);
    if (result.ec != errc()) {
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
            if (dml_count % 500 == 0)
                LOG_DEBUG("%lu", file_writer_->get_binlog()->get_bytes_written());
        }
        LOG_DEBUG("done, dml count=%d", dml_count);
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
    // TODO 当调用 next_file 时，要在 last_file 的末尾 写入一个 rotate event

    // 最小从 1 开始
    uint32_t fileno = log_files_.empty() ? 1 : log_files_.rbegin()->first + 1;

    std::ostringstream oss;
    oss << std::setw(6) << std::setfill('0') << fileno;
    file_suffix_ = oss.str();

    string nextFilename = file_prefix_ + std::string(file_dot_) + file_suffix_;
    filesystem::path next_file_path = directory_ / nextFilename;

    if (!log_files_.empty()) {
        auto nowFilename = log_files_.rbegin()->second.string();
        // 在上一个文件中，写入一个 rotate event 再关闭
        auto rotateEvent = std::make_unique<Rotate_event>(nowFilename, nowFilename.length(),
                                                          Rotate_event::DUP_NAME, 4);
        file_writer.get_binlog()->write_event_to_binlog(rotateEvent.get());

        file_writer.close();
    }

    log_files_.emplace(fileno, next_file_path);

    LOG_DEBUG("[==rotate file==]next file name = %s", next_file_path.c_str());

    return file_writer.open(next_file_path.c_str(), max_file_size_per_file_);
}
