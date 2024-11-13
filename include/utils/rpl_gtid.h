//
// Created by Coonger on 2024/10/19.
//

#pragma once

#include "utils/uuid.h"

#include <cassert>
#include <cstdint>
#include <memory>
#include <unordered_map>
#include <vector>

enum enum_gtid_type {
    AUTOMATIC_GTID = 0,
    ASSIGNED_GTID,
    ANONYMOUS_GTID,
    UNDEFINED_GTID,
    NOT_YET_DETERMINED_GTID,
    PRE_GENERATE_GTID
};

enum enum_return_status {
    /// The function completed successfully.
    RETURN_STATUS_OK = 0,
    /// The function completed with error but did not report it.
    RETURN_STATUS_UNREPORTED_ERROR = 1,
    /// The function completed with error and has called my_error.
    RETURN_STATUS_REPORTED_ERROR = 2
};

// GTID: {SID, GNO} also known as {uuid, sequence number}
using rpl_sidno = int32_t;
using rpl_gno = int64_t;
using rpl_sid = binary_log::Uuid;

/// One-past-the-max value of GNO
const rpl_gno GNO_END = INT64_MAX;
/// The length of MAX_GNO when printed in decimal.
const int MAX_GNO_TEXT_LENGTH = 19;

/*
 * 准备两个 map，可互查
 */
class Sid_map {
  public:
    Sid_map() : sidno_to_sid_map_(), sid_to_sidno_map_() {}

    ~Sid_map() { clear(); }

    enum_return_status clear() {
        sid_to_sidno_map_.clear();
        sidno_to_sid_map_.clear();
        return RETURN_STATUS_OK;
    }

    // 有关 map 的操作
    rpl_sidno add_sid(const rpl_sid &sid);

    rpl_sidno get_max_sidno() const {
        return static_cast<rpl_sidno>(sidno_to_sid_map_.size());
    }

    enum_return_status add_node(rpl_sidno sidno, const rpl_sid &sid);

    /**
      SID -> SIDNO
      如果不在 sidmap 中，返回 0
    */
    rpl_sidno sid_to_sidno(const rpl_sid &sid) const {
        const auto it = sid_to_sidno_map_.find(sid);
        if (it == sid_to_sidno_map_.end()) {
            return 0;
        }
        return it->second->sidno_;
    }

    /**
      SIDNO -> SID, 在 array 里找
    */
    const rpl_sid &sidno_to_sid(rpl_sidno sidno) const {
        const rpl_sid &ret = (sidno_to_sid_map_[sidno - 1])->sid_;
        return ret;
    }

  private:
    /// Node pointed to by both the hash and the array.
    struct Node {
        rpl_sidno sidno_;
        rpl_sid sid_;
    };

    static const unsigned char *
    sid_map_get_key(const unsigned char *ptr, size_t *length) {
        const Node *node = pointer_cast<const Node *>(ptr);
        *length = binary_log::Uuid::BYTE_LENGTH;
        return node->sid_.bytes;
    }

    /**
        给定 <sidno, sid> 值，写入到 sidno_to_sid_map_， sid_to_sidno_map_ 中
    */
    //    enum_return_status add_node(rpl_sidno sidno, const rpl_sid &sid);

    /**
      SIDNO -> SID 的映射用 array 的下标直接索引
    */
    std::vector<std::unique_ptr<Node>> sidno_to_sid_map_;
    /**
      SID -> SIDNO 的映射用 hash 表实现
    */
    std::unordered_map<rpl_sid, std::unique_ptr<Node>, binary_log::Hash_Uuid>
        sid_to_sidno_map_;
};

struct Gtid {
    /// SIDNO of this Gtid.
    rpl_sidno sidno_;
    /// GNO of this Gtid.
    rpl_gno gno_;

    /// Set both components to 0.
    void clear() {
        sidno_ = 0;
        gno_ = 0;
    }

    /// Set both components to the given, positive values.
    void set(rpl_sidno sidno_arg, rpl_gno gno_arg) {
        assert(sidno_arg > 0);
        assert(gno_arg > 0);
        assert(gno_arg < GNO_END);
        sidno_ = sidno_arg;
        gno_ = gno_arg;
    }

    /**
      Return true if sidno is zero (and assert that gno is zero too in
      this case).
    */
    bool is_empty() const {
        // check that gno is not set inconsistently
        if (sidno_ <= 0) {
            assert(gno_ == 0);
        } else {
            assert(gno_ > 0);
        }
        return sidno_ == 0;
    }

    /**
      The maximal length of the textual representation of a SID, not
      including the terminating '\0'.
    */
    static const int MAX_TEXT_LENGTH =
        binary_log::Uuid::TEXT_LENGTH + 1 + MAX_GNO_TEXT_LENGTH;
    /**
       返回 parse() 的结果
    */
    static bool is_valid(const char *text);

    int to_string(const rpl_sid &sid, char *buf) const;

    int to_string(const Sid_map *sid_map, char *buf) const;

    /// Returns true if this Gtid has the same sid and gno as 'other'.
    bool equals(const Gtid &other) const {
        return sidno_ == other.sidno_ && gno_ == other.gno_;
    }

    enum_return_status parse(Sid_map *sid_map, const char *text);
};

/**

    一个具体 statement 的 GTID 表示，可能为 AUTOMATIC, ANONYMOUS, 或者 SID:GNO
*/
struct Gtid_specification {
    enum_gtid_type type_;
    /**
      The GTID:
      { SIDNO, GNO } if type == ASSIGNED_GTID;
      { 0, 0 } if type == AUTOMATIC or ANONYMOUS.
    */
    Gtid gtid_;

    /// Set the type to ASSIGNED_GTID and SID, GNO to the given values.
    void set(rpl_sidno sidno, rpl_gno gno) {
        gtid_.set(sidno, gno);
        type_ = ASSIGNED_GTID;
    }

    /// Set the type to ASSIGNED_GTID and SID, GNO to the given Gtid.
    void set(const Gtid &gtid_param) {
        set(gtid_param.sidno_, gtid_param.gno_);
    }

    /// Set the type to AUTOMATIC_GTID.
    void set_automatic() { type_ = AUTOMATIC_GTID; }

    /// Set the type to ANONYMOUS_GTID.
    void set_anonymous() { type_ = ANONYMOUS_GTID; }

    /// Set the type to NOT_YET_DETERMINED_GTID.
    void set_not_yet_determined() { type_ = NOT_YET_DETERMINED_GTID; }

    /// Set to undefined. Must only be called if the type is ASSIGNED_GTID.
    void set_undefined() {
        assert(type_ == ASSIGNED_GTID);
        type_ = UNDEFINED_GTID;
    }

    /// Return true if this Gtid_specification is equal to 'other'.
    bool equals(const Gtid_specification &other) const {
        return (
            type_ == other.type_
            && (type_ != ASSIGNED_GTID || gtid_.equals(other.gtid_))
        );
    }

    /**
      Return true if this Gtid_specification is a ASSIGNED_GTID with the
      same SID, GNO as 'other_gtid'.
    */
    bool equals(const Gtid &other_gtid) const {
        return type_ == ASSIGNED_GTID && gtid_.equals(other_gtid);
    }

    enum_return_status parse(Sid_map *sid_map, const char *text);
    /// Returns true if the given string is a valid Gtid_specification.
    static bool is_valid(const char *text);

    static const int MAX_TEXT_LENGTH = Gtid::MAX_TEXT_LENGTH;

    int to_string(const rpl_sid *sid, char *buf) const;

    int to_string(const Sid_map *sid_map, char *buf) const;
    /**
       如果 ANONYMOUS_GTID or AUTOMATIC_GTID 类型的 GTID，那么 sid = null
    */
};

class Gtid_set {
  public:
    Gtid_set(Sid_map *sid_map) : sid_map_(sid_map) {};
    ~Gtid_set();

    void clear() { sid_map_->clear(); }

    /**
    Encodes this Gtid_set as a binary string.
  */
    void encode(unsigned char *buf) const;

    /**
        Returns the length of this Gtid_set when encoded using the
        encode() function.
    */
    size_t get_encoded_length() const;

  public:
    Sid_map *sid_map_;
};

/*
    Gtid_set.  可能为 null 的情况
    如果为 null ，也需要考虑有 Gtid_set 对象，使用 memset(0), 这样可以复用
   malloc 内存的逻辑
*/

struct Gtid_set_or_null {
    /// Pointer to the Gtid_set.
    Gtid_set *gtid_set;
    /// True if this Gtid_set is NULL.
    bool is_non_null;

    /// Return NULL if this is NULL, otherwise return the Gtid_set.
    inline Gtid_set *get_gtid_set() const {
        assert(!(is_non_null && gtid_set == nullptr));
        return is_non_null ? gtid_set : nullptr;
    }

    Gtid_set *set_non_null(Sid_map *sm) {
        if (!is_non_null) {
            if (gtid_set == nullptr) {
                gtid_set = new Gtid_set(sm);
            } else {
                gtid_set->clear();
            }
        }
        is_non_null = (gtid_set != nullptr);
        return gtid_set;
    }

    /// Set this Gtid_set to NULL.
    inline void set_null() { is_non_null = false; }
};

