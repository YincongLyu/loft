#ifndef LOFT_MY_JSON_H
#define LOFT_MY_JSON_H

#include "little_endian.h"
#include "json_dom.h"
#include <string>
#include <iostream>
#include <algorithm>

inline void int2store(char *pT, uint16 A) {
  int2store(static_cast<uchar *>(static_cast<void *>(pT)), A);
}

inline void int4store(char *pT, uint32 A) {
  int4store(static_cast<uchar *>(static_cast<void *>(pT)), A);
}

inline void int8store(char *pT, ulonglong A) {
  int8store(static_cast<uchar *>(static_cast<void *>(pT)), A);
}

inline void float8store(char *V, double M) {
  float8store(static_cast<uchar *>(static_cast<void *>(V)), M);
}

/** Encode a 16-bit int at the end of the destination string. */
static bool append_int16(std::string *dest, int16 value) {
  int start_pos = dest->length();
  dest->resize(sizeof(value) + dest->size());
  char* ptr = &dest->at(0);
  int2store(ptr + start_pos, value);
  return false;
}

/** Encode a 32-bit int at the end of the destination string. */
static bool append_int32(std::string *dest, int32 value) {
  int start_pos = dest->length();
  dest->resize(sizeof(value) + dest->size());
  char* ptr = &dest->at(0);
  int4store(ptr + start_pos, value);
  return false;
}

/** Encode a 64-bit int at the end of the destination string. */
static bool append_int64(std::string *dest, int64 value) {
  int start_pos = dest->length();
  dest->resize(sizeof(value) + dest->size());
  char* ptr = &dest->at(0);
  int8store(ptr + start_pos, value);
  return false;
}

void json2bin(const Json_dom *dom, std::string *dest);

void store_json_binary(const char *data,size_t length, uchar *dest);
      
#endif