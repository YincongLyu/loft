//
// Created by Coonger on 2024/11/16.
//
#pragma once

#include "sql/mysql_fields.h"
#include "events/write_event.h"
#include "format/dml_generated.h"
#include <memory>
#include <map>


/**
 * @brief 通用的数据处理接口
 */
class FieldDataHandler {
public:
  virtual void processData(const kvPair* data, mysql::Field* field, Rows_event* row) = 0;
  virtual ~FieldDataHandler() = default;
};

/**
 * @brief long / double / string 实现具体的处理器
 */
class LongValueHandler : public FieldDataHandler {
public:
  void processData(const kvPair* data, mysql::Field* field, Rows_event* row) override {
    int64 value = data->value_as_LongVal()->value();

    if (field->type() == MYSQL_TYPE_YEAR) {
      value -= (value >= 2000 ? 2000 : 1900);
    }
    row->writeData(reinterpret_cast<uchar*>(&value), field->type(), field->pack_length());
  }
};

class DoubleValueHandler : public FieldDataHandler {
public:

  void processData(const kvPair* data, mysql::Field* field, Rows_event* row) override {
    double value = data->value_as_DoubleVal()->value();
    std::string num = formatDoubleToFixedWidth(value, field->get_width(), field->decimals());
    LOFT_ASSERT(num != "", "double number format must be valid");

    // 模板函数会发生静态 分支检查
    if (field->type() == MYSQL_TYPE_FLOAT) {
      float float_value = std::stof(num);
      row->writeData(reinterpret_cast<uchar*>(&float_value), field->type(), field->pack_length());
    } else {
      double double_value = std::stod(num);
      row->writeData(reinterpret_cast<uchar*>(&double_value), field->type(), field->pack_length());
    }
  }
private:
  inline std::string formatDoubleToFixedWidth(double number, int length, int frac) {
    // 转换为字符串，并设置固定的小数位数
    std::ostringstream oss;
    oss << std::fixed << std::setprecision(frac) << number;
    std::string str = oss.str();

    // 找到小数点的位置
    auto decimalPos = str.find('.');

    // 整数部分的宽度
    int intPartWidth =
        (decimalPos == std::string::npos) ? str.length() : decimalPos;

    // 判断整数部分是否超出要求的宽度
    if (intPartWidth > length - frac - 1) {
      return "";
    }

    // 按照总宽度截断
    if (str.length() > length) {
      str = str.substr(0, length);
    }

    return str;
  }
};

class StringValueHandler : public FieldDataHandler {
public:
  void processData(const kvPair* data, mysql::Field* field, Rows_event* row) override {
    const char *str = data->value_as_StringVal()->value()->c_str();

    if (field->type() == MYSQL_TYPE_NEWDECIMAL) {
      double double_value = std::stod(str);
      row->writeData(reinterpret_cast<uchar*>(&double_value), field->type(), 0, 0, field->pack_length(), field->decimals());
    } else if (field->type() == MYSQL_TYPE_JSON) {
      // base64 解码，后不用按照 3 组再合并解释出来，直接写入
      auto dst = base64_decode(str);
      row->writeData(dst.data(), field->type(), field->pack_length(), dst.size());

    } else {
      char *dst = (char *)malloc(base64_needed_decoded_length(strlen(str)));
      int64_t dst_len = base64_decode(str, strlen(str), (void *)dst, nullptr, 0);
      row->writeData(reinterpret_cast<uchar *>(dst), field->type(), field->pack_length(), dst_len);
      // 释放内存
      free(dst);
    }
    // TODO 时间类型 datatime timestamp

  }
};

/**
 * @brief 创建工厂类管理处理器
 */
class DataHandlerFactory {
public:
  static FieldDataHandler* getHandler(loft::DataMeta type) {
    static auto handlers = initHandlers();
    auto it = handlers.find(type);
    return it != handlers.end() ? it->second.get() : nullptr;
  }

private:
  static std::map<loft::DataMeta, std::unique_ptr<FieldDataHandler>> initHandlers() {
    std::map<loft::DataMeta, std::unique_ptr<FieldDataHandler>> m;
    m.insert({DataMeta_LongVal, std::make_unique<LongValueHandler>()});
    m.insert({DataMeta_DoubleVal, std::make_unique<DoubleValueHandler>()});
    m.insert({DataMeta_StringVal, std::make_unique<StringValueHandler>()});
    return m;
  }
};