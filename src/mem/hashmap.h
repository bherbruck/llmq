// mem/hashmap.h - Generic open-addressing hash table
// Uses linear probing, O(1) average lookup/insert/delete
// Zero-cost generic: u32 value fits any pool index, slot index, etc.

#ifndef MEM_HASHMAP_H
#define MEM_HASHMAP_H

#include "sys/types.h"
#include "sys/syscall.h"

// =============================================================================
// Hash Table Entry (8 bytes - cache friendly)
// =============================================================================

// Special values for hash table value field
#define HASHMAP_EMPTY   ((u32)-1) // Slot has never been used
#define HASHMAP_DELETED ((u32)-2) // Slot was used but entry deleted (tombstone)

// Hash table entry: stores (hash, value) pair
struct hashmap_entry {
    u32 hash;  // Key hash (caller computes, e.g., FNV-1a)
    u32 value; // Generic u32 value, or HASHMAP_EMPTY/DELETED
};

// =============================================================================
// Hash Table Structure
// =============================================================================

struct hashmap {
    struct hashmap_entry *entries; // mmap'd array of entries
    u32 capacity;                  // Power of 2 for fast modulo
    u32 count;                     // Number of active entries
    u32 mask;                      // capacity - 1 for fast modulo
};

// =============================================================================
// Hash Table Operations
// =============================================================================

#define HASHMAP_MIN_CAPACITY 16 // Minimum capacity (power of 2)

// Initialize hash table with given capacity (will be rounded up to power of 2)
INLINE i32 hashmap_init(struct hashmap *h, u32 min_capacity) {
    // Round up to power of 2
    u32 capacity = HASHMAP_MIN_CAPACITY;
    while (capacity < min_capacity) {
        capacity *= 2;
    }

    h->capacity = capacity;
    h->count    = 0;
    h->mask     = capacity - 1;

    // mmap the entry array
    usize entries_size = (usize)capacity * sizeof(struct hashmap_entry);
    h->entries         = (struct hashmap_entry *)sys_mmap(NULL, entries_size, PROT_READ | PROT_WRITE,
                                                          MAP_PRIVATE | MAP_ANON, -1, 0);
    if (IS_ERR(h->entries)) {
        return -1;
    }

    // Initialize all slots as empty
    for (u32 i = 0; i < capacity; i++) {
        h->entries[i].value = HASHMAP_EMPTY;
        h->entries[i].hash  = 0;
    }

    return 0;
}

// Cleanup hash table
INLINE void hashmap_cleanup(struct hashmap *h) {
    if (h->entries) {
        sys_munmap(h->entries, (usize)h->capacity * sizeof(struct hashmap_entry));
        h->entries = NULL;
    }
}

// Insert (hash, value) into hash table
// Returns true if inserted, false if table is full (shouldn't happen if sized correctly)
INLINE bool hashmap_insert(struct hashmap *h, u32 hash, u32 value) {
    // Table is too full (>75% load factor) - this indicates sizing issue
    if (unlikely(h->count >= (h->capacity * 3) / 4)) {
        return false;
    }

    u32 idx = hash & h->mask;

    // Linear probing to find empty slot or tombstone
    while (h->entries[idx].value != HASHMAP_EMPTY && h->entries[idx].value != HASHMAP_DELETED) {
        idx = (idx + 1) & h->mask;
    }

    h->entries[idx].hash  = hash;
    h->entries[idx].value = value;
    h->count++;

    return true;
}

// Find value by hash, returns value or HASHMAP_EMPTY if not found
// Caller must verify full key match (hash collision possible)
INLINE u32 hashmap_find(struct hashmap *h, u32 hash) {
    u32 idx   = hash & h->mask;
    u32 start = idx;

    // Linear probing until we find matching hash, empty slot, or wrap around
    do {
        if (h->entries[idx].value == HASHMAP_EMPTY) {
            return HASHMAP_EMPTY; // Not found
        }
        if (h->entries[idx].value != HASHMAP_DELETED && h->entries[idx].hash == hash) {
            return h->entries[idx].value; // Found candidate (caller verifies full match)
        }
        idx = (idx + 1) & h->mask;
    } while (likely(idx != start));

    return HASHMAP_EMPTY; // Wrapped around, not found
}

// Remove entry by hash and value
// Returns true if found and removed
INLINE bool hashmap_remove(struct hashmap *h, u32 hash, u32 value) {
    u32 idx   = hash & h->mask;
    u32 start = idx;

    do {
        if (h->entries[idx].value == HASHMAP_EMPTY) {
            return false; // Not found
        }
        if (h->entries[idx].hash == hash && h->entries[idx].value == value) {
            // Mark as tombstone (allows probing to continue past this slot)
            h->entries[idx].value = HASHMAP_DELETED;
            h->count--;
            return true;
        }
        idx = (idx + 1) & h->mask;
    } while (likely(idx != start));

    return false;
}

// Update entry: change value for an existing (hash, old_value) pair
// Returns true if found and updated
INLINE bool hashmap_update(struct hashmap *h, u32 hash, u32 old_value, u32 new_value) {
    u32 idx   = hash & h->mask;
    u32 start = idx;

    do {
        if (h->entries[idx].value == HASHMAP_EMPTY) {
            return false;
        }
        if (h->entries[idx].hash == hash && h->entries[idx].value == old_value) {
            h->entries[idx].value = new_value;
            return true;
        }
        idx = (idx + 1) & h->mask;
    } while (likely(idx != start));

    return false;
}

#endif // MEM_HASHMAP_H
