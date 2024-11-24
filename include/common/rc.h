//
// Created by Coonger on 2024/11/2.
//
#pragma once

#define DEFINE_RCS                       \
  DEFINE_RC(SUCCESS)                     \
  DEFINE_RC(INVALID_ARGUMENT)            \
  DEFINE_RC(UNREACHABLE)                 \
  DEFINE_RC(UNIMPLEMENTED)               \
  DEFINE_RC(INTERNAL)                    \
  DEFINE_RC(NOMEM)                       \
  DEFINE_RC(NOTFOUND)                    \
  DEFINE_RC(BUFFERPOOL_OPEN)             \
  DEFINE_RC(BUFFERPOOL_NOBUF)            \
  DEFINE_RC(BUFFERPOOL_INVALID_PAGE_NUM) \
  DEFINE_RC(SCHEMA_DB_EXIST)             \
  DEFINE_RC(SCHEMA_DB_NOT_EXIST)         \
  DEFINE_RC(SCHEMA_DB_NOT_OPENED)        \
  DEFINE_RC(SCHEMA_TABLE_NOT_EXIST)      \
  DEFINE_RC(SCHEMA_TABLE_EXIST)          \
  DEFINE_RC(SCHEMA_FIELD_NOT_EXIST)      \
  DEFINE_RC(SCHEMA_FIELD_MISSING)        \
  DEFINE_RC(SCHEMA_FIELD_TYPE_MISMATCH)  \
  DEFINE_RC(IOERR_EVENT_WRITE)           \
  DEFINE_RC(IOERR_READ)                  \
  DEFINE_RC(IOERR_WRITE)                 \
  DEFINE_RC(IOERR_ACCESS)                \
  DEFINE_RC(IOERR_OPEN)                  \
  DEFINE_RC(IOERR_CLOSE)                 \
  DEFINE_RC(IOERR_SEEK)                  \
  DEFINE_RC(IOERR_TOO_LONG)              \
  DEFINE_RC(IOERR_SYNC)                  \
  DEFINE_RC(LOCKED_UNLOCK)               \
  DEFINE_RC(LOCKED_NEED_WAIT)            \
  DEFINE_RC(LOCKED_CONCURRENCY_CONFLICT) \
  DEFINE_RC(FILE_EXIST)                  \
  DEFINE_RC(FILE_NOT_EXIST)              \
  DEFINE_RC(FILE_NAME)                   \
  DEFINE_RC(FILE_BOUND)                  \
  DEFINE_RC(FILE_CREATE)                 \
  DEFINE_RC(FILE_OPEN)                   \
  DEFINE_RC(FILE_NOT_OPENED)             \
  DEFINE_RC(FILE_CLOSE)                  \
  DEFINE_RC(FILE_REMOVE)                 \
  DEFINE_RC(LOGBUF_FULL)                 \
  DEFINE_RC(LOG_FILE_FULL)               \
  DEFINE_RC(LOG_ENTRY_INVALID)           \
  DEFINE_RC(SPEED_LIMIT)

enum class RC
{
#define DEFINE_RC(name) name,
  DEFINE_RCS
#undef DEFINE_RC
};

extern const char *strrc(RC rc);

extern bool LOFT_SUCC(RC rc);
extern bool LOFT_FAIL(RC rc);
