// subs.test.c - Tests for broker/subs.h (topic matching)
// NOLINTBEGIN(readability-magic-numbers,cppcoreguidelines-avoid-magic-numbers)

#define TESTING
#include "test/test.h"
#include "broker/subs.h"

TEST(topic_exact_match) {
    ASSERT(topic_matches("a/b/c", 5, (const u8 *)"a/b/c", 5) == true);
    ASSERT(topic_matches("a/b/c", 5, (const u8 *)"a/b/d", 5) == false);
    ASSERT(topic_matches("a/b/c", 5, (const u8 *)"a/b", 3) == false);
}

TEST(topic_plus_wildcard) {
    // + matches single level
    ASSERT(topic_matches("+/b/c", 5, (const u8 *)"a/b/c", 5) == true);
    ASSERT(topic_matches("a/+/c", 5, (const u8 *)"a/b/c", 5) == true);
    ASSERT(topic_matches("a/b/+", 5, (const u8 *)"a/b/c", 5) == true);
    ASSERT(topic_matches("+/+/+", 5, (const u8 *)"a/b/c", 5) == true);
    // + doesn't match multiple levels
    ASSERT(topic_matches("+/c", 3, (const u8 *)"a/b/c", 5) == false);
}

TEST(topic_hash_wildcard) {
    // # matches remaining levels
    ASSERT(topic_matches("a/#", 3, (const u8 *)"a/b/c", 5) == true);
    ASSERT(topic_matches("a/#", 3, (const u8 *)"a/b", 3) == true);
    ASSERT(topic_matches("#", 1, (const u8 *)"a/b/c", 5) == true);
    ASSERT(topic_matches("a/b/#", 5, (const u8 *)"a/b/c/d/e", 9) == true);
    // # must match at least one level from its position
    ASSERT(topic_matches("a/b/#", 5, (const u8 *)"a/x", 3) == false);
}

TEST(topic_mixed_wildcards) {
    ASSERT(topic_matches("+/b/#", 5, (const u8 *)"a/b/c/d", 7) == true);
    ASSERT(topic_matches("a/+/#", 5, (const u8 *)"a/b/c", 5) == true);
}

TEST(topic_sensors_example) {
    // Real-world sensor topics
    ASSERT(topic_matches("sensors/+/temp", 14, (const u8 *)"sensors/room1/temp", 18) == true);
    ASSERT(topic_matches("sensors/+/temp", 14, (const u8 *)"sensors/room2/temp", 18) == true);
    ASSERT(topic_matches("sensors/+/temp", 14, (const u8 *)"sensors/room1/humidity", 22) == false);
    ASSERT(topic_matches("home/#", 6, (const u8 *)"home/kitchen/light", 18) == true);
}

// NOLINTEND(readability-magic-numbers,cppcoreguidelines-avoid-magic-numbers)
