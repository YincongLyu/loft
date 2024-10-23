//
// Created by Coonger on 2024/10/21.
//

#ifndef LOFT_ROWS_EVENT_H
#define LOFT_ROWS_EVENT_H

#include "abstract_event.h"
#include "sql/field.h"
#include "table_id.h"
#include <memory>
#include <rapidjson/stringbuffer.h>
#include <vector>

class Table_map_event : public AbstractEvent {
  public:
    /** Constants representing offsets */
    enum Table_map_event_offset {
        /** TM = "Table Map" */
        TM_MAPID_OFFSET = 0,
        TM_FLAGS_OFFSET = 6
    };

    typedef uint16_t flag_set;

    /**
      DEFAULT_CHARSET and COLUMN_CHARSET don't appear together, and
      ENUM_AND_SET_DEFAULT_CHARSET and ENUM_AND_SET_COLUMN_CHARSET don't
      appear together. They are just alternative ways to pack character
      set information. When binlogging, it logs character sets in the
      way that occupies least storage.

      SIMPLE_PRIMARY_KEY and PRIMARY_KEY_WITH_PREFIX don't appear together.
      SIMPLE_PRIMARY_KEY is for the primary keys which only use whole values of
      pk columns. PRIMARY_KEY_WITH_PREFIX is
      for the primary keys which just use part value of pk columns.
     */
    enum Optional_metadata_field_type {
        SIGNEDNESS = 1,  // UNSIGNED flag of numeric columns
        DEFAULT_CHARSET, /* Character set of string columns, optimized to
                            minimize space when many columns have the
                            same charset. */
        COLUMN_CHARSET,  /* Character set of string columns, optimized to
                            minimize space when columns have many
                            different charsets. */
        COLUMN_NAME,
        SET_STR_VALUE,                // String value of SET columns
        ENUM_STR_VALUE,               // String value of ENUM columns
        GEOMETRY_TYPE,                // Real type of geometry columns
        SIMPLE_PRIMARY_KEY,           // Primary key without prefix
        PRIMARY_KEY_WITH_PREFIX,      // Primary key with prefix
        ENUM_AND_SET_DEFAULT_CHARSET, /* Character set of enum and set
                                         columns, optimized to minimize
                                         space when many columns have the
                                         same charset. */
        ENUM_AND_SET_COLUMN_CHARSET,  /* Character set of enum and set
                                         columns, optimized to minimize
                                         space when many columns have the
                                         same charset. */
        COLUMN_VISIBILITY             /* Flag to indicate column visibility
                                         attribute. */
    };

    /**
      Metadata_fields organizes m_optional_metadata into a structured format which
      is easy to access. 暂时不使用
    */
    struct Optional_metadata_fields {
        typedef std::pair<unsigned int, unsigned int> uint_pair;
        typedef std::vector<std::string> str_vector;

        struct Default_charset {
            Default_charset() : default_charset(0) {}
            bool empty() const { return default_charset == 0; }

            // Default charset for the columns which are not in charset_pairs.
            unsigned int default_charset;

            /* The uint_pair means <column index, column charset number>. */
            std::vector<uint_pair> charset_pairs;
        };

        // Contents of DEFAULT_CHARSET field are converted into Default_charset.
        Default_charset m_default_charset;
        // Contents of ENUM_AND_SET_DEFAULT_CHARSET are converted into
        // Default_charset.
        Default_charset m_enum_and_set_default_charset;
        std::vector<bool> m_signedness;
        // Character set number of every string column
        std::vector<unsigned int> m_column_charset;
        // Character set number of every ENUM or SET column.
        std::vector<unsigned int> m_enum_and_set_column_charset;
        std::vector<std::string> m_column_name;
        // each str_vector stores values of one enum/set column
        std::vector<str_vector> m_enum_str_value;
        std::vector<str_vector> m_set_str_value;
        std::vector<unsigned int> m_geometry_type;
        /*
          The uint_pair means <column index, prefix length>.  Prefix length is 0 if
          whole column value is used.
        */
        std::vector<uint_pair> m_primary_key;
        std::vector<bool> m_column_visibility;

        /*
          It parses m_optional_metadata and populates into above variables.

          @param[in] optional_metadata points to the begin of optional metadata
                                       fields in table_map_event.
          @param[in] optional_metadata_len length of optional_metadata field.
         */
        Optional_metadata_fields(unsigned char *optional_metadata,
                                 unsigned int optional_metadata_len);
        // It is used to specify the validity of the deserialized structure
        bool is_valid_;
    };


    Table_map_event(const Table_id &tid, unsigned long colcnt, const char *dbnam,
                    size_t dblen, const char *tblnam, size_t tbllen);

    ~Table_map_event() override;

    /** Event post header contents */
    Table_id m_table_id_;
    flag_set m_flags;

    size_t m_data_size_; /** event data size */

    /** Event body contents */
    std::string m_dbnam_;
    unsigned long long int m_dblen_;
    std::string m_tblnam_;
    unsigned long long int m_tbllen_;
    unsigned long m_colcnt_;
    unsigned char *m_coltype_;

    /**
      The size of field metadata buffer set by calling save_field_metadata()
    */
    unsigned long m_field_metadata_size_;
    unsigned char *m_field_metadata_; /** field metadata */
    unsigned char *m_null_bits_;
    unsigned int m_optional_metadata_len;
    unsigned char *m_optional_metadata;

    Table_map_event()
        : AbstractEvent(TABLE_MAP_EVENT),
        m_coltype_(nullptr),
        m_field_metadata_size_(0),
        m_field_metadata_(nullptr),
        m_null_bits_(nullptr),
        m_optional_metadata_len(0),
        m_optional_metadata(nullptr) {}

    unsigned long long get_table_id() { return m_table_id_.get_id(); }
    std::string get_table_name() { return m_tblnam_; }
    std::string get_db_name() { return m_dbnam_; }


    // ********* impl virtual function *********************
    size_t get_data_size() override { return m_data_size_; }
    bool write_data_header(Basic_ostream *ostream) override;
    bool write_data_body(Basic_ostream *ostream) override;


    int save_field_metadata();

// ********* log event field *********************

    rapidjson::StringBuffer m_metadata_buf_; // Metadata fields buffer
    /**
      Wrapper around `TABLE *m_table` that abstracts the table field set iteration logic
     */
//    Table_columns_view<> m_column_view_{nullptr};

    std::vector<std::unique_ptr<Field>> m_column_view_; // Table field set
};




#endif // LOFT_ROWS_EVENT_H
