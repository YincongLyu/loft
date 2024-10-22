# loft
convert the log format from cantian storage engine's redo log to MySQL's binlog via cpp

# TODO
- [x] 可解析 FlatBuffer 格式的 redo log
- [ ] 改造 MySQL binlog 的构造函数
  - [x] control_event 4: fde, prev gtid, gtid, xid
  - [x] statement_event 1: query
  - [ ] table_map 1
  - [ ] row_event 3: write rows, delete rows, update rows 
- [ ] 本地集中验证 多条sql，使用mysqlbinlog工具回放无误 <----------预计这周完成
- [ ] 支持单线程写入多个 binlog 二进制文件, 实现 binlog 文件的管理
- [ ] 支持程序运行时的 DEBUG 友好输出提示
- [ ] 函数返回值的改造，使用 enum 类型确定返回信息
- [ ] 持续优化，支持多线程并行解析 cantian redo log 二进制文件
