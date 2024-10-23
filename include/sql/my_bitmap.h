//
// Created by Coonger on 2024/10/21.
// copy from mysql: my_bitmap.h

#ifndef LOFT_MY_BITMAP_H
#define LOFT_MY_BITMAP_H

#define MY_BIT_NONE (~(uint)0)

#include <assert.h>
#include <limits.h>
#include <string.h>
#include <constants.h>
#include <sys/types.h>
#include <cstdint>

typedef uint32_t my_bitmap_map;

struct MY_BITMAP {
    my_bitmap_map *bitmap{nullptr};
    uint n_bits{0}; /* number of bits occupied by the above */
    my_bitmap_map last_word_mask{0};
    my_bitmap_map *last_word_ptr{nullptr};
};

extern void create_last_word_mask(MY_BITMAP *map);
extern bool bitmap_init(MY_BITMAP *map, my_bitmap_map *buf, uint n_bits);
extern bool bitmap_is_clear_all(const MY_BITMAP *map);
extern bool bitmap_is_prefix(const MY_BITMAP *map, uint prefix_size);
extern bool bitmap_is_set_all(const MY_BITMAP *map);
extern bool bitmap_is_subset(const MY_BITMAP *map1, const MY_BITMAP *map2);
extern bool bitmap_is_overlapping(const MY_BITMAP *map1, const MY_BITMAP *map2);
extern bool bitmap_is_valid(const MY_BITMAP *map);
extern bool bitmap_test_and_set(MY_BITMAP *map, uint bitmap_bit);
extern uint bitmap_set_next(MY_BITMAP *map);
extern uint bitmap_get_first(const MY_BITMAP *map);
extern uint bitmap_get_first_set(const MY_BITMAP *map);
extern uint bitmap_get_next_set(const MY_BITMAP *map, uint bitmap_bit);
extern uint bitmap_bits_set(const MY_BITMAP *map);
extern void bitmap_free(MY_BITMAP *map);
extern void bitmap_set_above(MY_BITMAP *map, uint from_byte, bool use_bit);
extern void bitmap_set_prefix(MY_BITMAP *map, uint prefix_size);
extern void bitmap_intersect(MY_BITMAP *to, const MY_BITMAP *from);
extern void bitmap_subtract(MY_BITMAP *map, const MY_BITMAP *map2);
extern void bitmap_union(MY_BITMAP *map, const MY_BITMAP *map2);
extern void bitmap_xor(MY_BITMAP *map, const MY_BITMAP *map2);
extern void bitmap_invert(MY_BITMAP *map);
extern void bitmap_copy(MY_BITMAP *map, const MY_BITMAP *map2);
extern uint bitmap_n_copy(MY_BITMAP *dst, const MY_BITMAP *src,
                          uint max_bits_to_copy = UINT_MAX);

#define bitmap_buffer_size(bits) (((bits) + 31) / 32) * 4
#define no_bytes_in_map(map) (((map)->n_bits + 7) / 8)
#define no_words_in_map(map) (((map)->n_bits + 31) / 32)

static inline void bitmap_set_bit(MY_BITMAP *map, uint bit) {
    assert(bit < map->n_bits);
    ((uchar*)map->bitmap)[bit / 8] |= (1 << (bit & 7));
}

static inline void bitmap_flip_bit(MY_BITMAP *map, uint bit) {
    assert(bit < map->n_bits);
    ((uchar *)map->bitmap)[bit / 8] ^= (1 << (bit & 7));
}

static inline void bitmap_clear_bit(MY_BITMAP *map, uint bit) {
    assert(bit < map->n_bits);
    ((uchar *)map->bitmap)[bit / 8] &= ~(1 << (bit & 7));
}

static inline bool bitmap_is_set(const MY_BITMAP *map, uint bit) {
    assert(bit < map->n_bits);
    return ((uchar *)map->bitmap)[bit / 8] & (1 << (bit & 7));
}

/**
   Quite unlike other C comparison functions ending with 'cmp', e.g. memcmp(),
   strcmp(), this function returns true if the bitmaps are equal, and false
   otherwise.

   @retval true The bitmaps are equal.
   @retval false The bitmaps differ.
 */
static inline bool bitmap_cmp(const MY_BITMAP *map1, const MY_BITMAP *map2) {
    assert(map1->n_bits > 0);
    assert(map2->n_bits > 0);

    if (memcmp(map1->bitmap, map2->bitmap, 4 * (no_words_in_map(map1) - 1)) != 0)
    return false;
  return ((*map1->last_word_ptr | map1->last_word_mask) ==
            (*map2->last_word_ptr | map2->last_word_mask));
}

/*
  Clears all bits. This is allowed even for a zero-size bitmap.
 */
static inline void bitmap_clear_all(MY_BITMAP *map) {
    memset(map->bitmap, 0, 4 * no_words_in_map(map));
}

/*
  Sets all bits. This is allowed even for a zero-size bitmap.
 */
static inline void bitmap_set_all(MY_BITMAP *map) {
    memset(map->bitmap, 0xFF, 4 * no_words_in_map(map));
}



#endif // LOFT_MY_BITMAP_H
