//
// Created by Coonger on 2024/11/2.
//

#include "common/rc.h"

const char *strrc(RC rc)
{
#define DEFINE_RC(name) \
  case RC::name: {      \
    return #name;       \
  } break;

  switch (rc) {
    DEFINE_RCS;
    default: {
      return "unknown";
    }
  }
#undef DEFINE_RC
}

bool LOFT_SUCC(RC rc) { return rc == RC::SUCCESS; }

bool LOFT_FAIL(RC rc) { return rc != RC::SUCCESS; }
