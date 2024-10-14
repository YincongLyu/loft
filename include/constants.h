// constants.h
#ifndef CONSTANTS_H
#define CONSTANTS_H

namespace loft {

using int32_t = signed int;
using uint32_t = unsigned int;
using int64_t = signed long int;
using uint64_t = unsigned long int;

constexpr int INT_OFFSET = 4;
constexpr int SQL_SIZE_ARRAY[] = {248,  744,  3304, 3208, 3208,
                                  2040, 2040, 1968, 264,  224};

} // namespace loft

#endif // CONSTANTS_H
