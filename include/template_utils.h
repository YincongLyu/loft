//
// Created by Coonger on 2024/10/19.
//

#ifndef LOFT_TEMPLATE_UITLS_H
#define LOFT_TEMPLATE_UITLS_H

#include <algorithm>
#include <assert.h>
#include <ctype.h>
#include <iterator>
#include <optional>
#include <stddef.h>
#include <type_traits>

/**
   copy from: mysql ./include/template_utils.h
*/

/**
  Casts from one pointer type, to another, without using
  reinterpret_cast or C-style cast:
    foo *f; bar *b= pointer_cast<bar*>(f);
  This avoids having to do:
    foo *f; bar *b= static_cast<bar*>(static_cast<void*>(f));
 */
template<typename T>
inline T pointer_cast(void *p) {
    return static_cast<T>(p);
}

template<typename T>
inline const T pointer_cast(const void *p) {
    return static_cast<T>(p);
}

/**
   Utility to allow returning values from functions which can fail
   (until we have std::optional).
 */
template<class VALUE_TYPE>
struct ReturnValueOrError {
    /** Value returned from function in the normal case. */
    VALUE_TYPE value;

    /** True if an error occurred. */
    bool error;
};

#endif // LOFT_TEMPLATE_UITLS_H
