
#pragma once

// #include <cstring>
// #include <memory>
// #include <unordered_map>

#include "format/ddl_generated.h"
#include "format/dml_generated.h"

#include "events/write_event.h"
#include "binlog.h"
#include "utils/table_id.h"

using namespace loft;

class LogFormatTransformManager
{
public:
  LogFormatTransformManager()  = default;
  ~LogFormatTransformManager() = default;

  // 组装 3 个 event
  void transformDDL(const DDL *ddl, MYSQL_BIN_LOG *binLog);

  // 组装 5 个 event
  void transformDML(const DML *dml, MYSQL_BIN_LOG *binLog);

  std::vector<std::unique_ptr<AbstractEvent>> transformDDL(const DDL *ddl);
  std::vector<std::unique_ptr<AbstractEvent>> transformDML(const DML *dml);

private:
  inline uint64_t stringToTimestamp(const std::string& timeString);
  inline enum_field_types ConvertStringType(std::string_view type_str);
  void processRowData(const ::flatbuffers::Vector<::flatbuffers::Offset<loft::kvPair>> &fields, Rows_event *row,
      const std::unordered_map<std::string, int> &field_map, const std::vector<mysql::FieldRef> &field_vec,
      bool is_before);

private:
  // <table_name, table_id> 对应，在执行多条 DML 时，能确定正在 操作同一张表
  std::unordered_map<std::string, Table_id> tableName2TableId_;
};
