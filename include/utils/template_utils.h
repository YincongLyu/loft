//
// Created by Coonger on 2024/10/19.
//

#include <algorithm>
#include <assert.h>
#include <ctype.h>
#include <iterator>
#include <optional>
#include <stddef.h>
#include <type_traits>

/**
   refer from: mysql ./include/template_utils.h
*/

template <typename T>
inline T pointer_cast(void *p)
{
  return static_cast<T>(p);
}

template <typename T>
inline const T pointer_cast(const void *p)
{
  return static_cast<T>(p);
}
