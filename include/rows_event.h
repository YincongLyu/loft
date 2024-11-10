//
// Created by Coonger on 2024/10/21.
//

#ifndef LOFT_ROWS_EVENT_H
#define LOFT_ROWS_EVENT_H

#include "abstract_event.h"
#include "sql/mysql_fields.h"
#include "table_id.h"

#include <memory>
#include <vector>

class Table_map_event : public AbstractEvent {
  public:
    Table_map_event(
        const Table_id &tid,
        unsigned long colcnt,
        const char *dbnam,
        size_t dblen,
        const char *tblnam,
        size_t tbllen,
        const std::vector<mysql::FieldRef> &column_view
    );

    ~Table_map_event() override;
    DISALLOW_COPY(Table_map_event);

    // ********* impl virtual function *********************
    size_t get_data_size() override { return m_data_size_; }

    bool write_data_header(Basic_ostream *ostream) override;
    bool write_data_body(Basic_ostream *ostream) override;

    int save_field_metadata();

    /** Constants representing offsets */
    enum Table_map_event_offset {
        /** TM = "Table Map" */
        TM_MAPID_OFFSET = 0,
        TM_FLAGS_OFFSET = 6
    };

    /** Event post header contents */
    Table_id m_table_id_;
    typedef uint16_t flag_set;
    flag_set m_flags = 0; // 目前的 8.0 版本默认是 0

    size_t m_data_size_; /** event data size */

    /** Event body contents */
    std::string m_dbnam_;
    unsigned long long int m_dblen_;
    std::string m_tblnam_;
    unsigned long long int m_tbllen_;
    unsigned long m_colcnt_;
    //    unsigned char *m_coltype_;
    std::unique_ptr<unsigned char[]> m_coltype_;
    /**
      The size of field metadata buffer set by calling save_field_metadata()
    */
    unsigned long m_field_metadata_size_;
    std::unique_ptr<unsigned char[]> m_field_metadata_;
    std::unique_ptr<unsigned char[]> m_null_bits_;

    unsigned long long get_table_id() { return m_table_id_.get_id(); }

    std::string get_table_name() { return m_tblnam_; }

    std::string get_db_name() { return m_dbnam_; }

    // ********* log event field *********************

    std::vector<mysql::FieldRef> m_column_view_; // Table field set
};

#endif // LOFT_ROWS_EVENT_H
