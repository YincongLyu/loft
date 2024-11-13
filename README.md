# loft
convert the log format from cantian storage engine's redo log to MySQL's binlog via cpp

# TODO
- [x] 可解析 FlatBuffer 格式的 redo log
- [x] 改造 MySQL binlog 的构造函数
  - [x] control_event 3: fde, gtid, xid
  - [x] statement_event 1: query
  - [x] table_map event 1
  - [x] rows_event 3: write_rows, delete_rows, update_rows 
- [x] 本地集中验证 多条sql，使用mysqlbinlog工具回放无误
- [x] 支持程序运行时的 DEBUG 友好输出提示
- [x] 函数返回值的改造，使用 enum 类型确定返回信息：正确返回0，错误返回错误码，根据错误码确定返回信息
- [x] 支持单线程写入多个 binlog 二进制文件, 实现 binlog 文件的管理
- [ ] 实现 3 个必要接口：
  - [x] 设置 binlog 写入的文件路径接口 ——> 无返回值
  - [x] 转换 binlog 接口 ——> 返回值友好提醒
  - [ ] 获取 binlog 文件中最后一条 sql 的 scn、seq、checkPoint 的接口 ——> 返回值友好提醒
- [ ] 持续优化，支持多线程并行解析 cantian redo log 二进制文件
  - [ ] 读取模块改造：异步 scatter I/O，同时并行读多个 redo log 文件，按照 table_id shuffle 至不同 partition
  - [ ] 转换模块改造：对于每个 table_id 划分的 partition，同时处理。优先级定义如下：DDL > insert > update = delete
