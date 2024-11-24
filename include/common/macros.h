//
// Created by Coonger on 2024/11/2.
//

#pragma once

#include <iostream>  // std::cerr

#define likely(x) __builtin_expect((x, 0)
#define unlikely(x) __builtin_expect(!!(x), 0)

// Macros to disable copying and moving
#define DISALLOW_COPY(cname)                       \
  cname(const cname &)                   = delete; \
  auto operator=(const cname &)->cname & = delete;

#define DISALLOW_MOVE(cname)                  \
  cname(cname &&)                   = delete; \
  auto operator=(cname &&)->cname & = delete;

#define DISALLOW_COPY_AND_MOVE(cname) \
  DISALLOW_COPY(cname);               \
  DISALLOW_MOVE(cname);

#define LOFT_ASSERT(expr, message) assert((expr) && (message))

#define LOFT_VERIFY(expr, message)                    \
  if (unlikely(expr)) {                               \
    std::cerr << "ERROR: " << (message) << std::endl; \
    std::terminate();                                 \
  }
