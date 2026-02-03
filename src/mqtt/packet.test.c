// packet.test.c - Tests for mqtt/packet.h
// NOLINTBEGIN(readability-magic-numbers,cppcoreguidelines-avoid-magic-numbers)

#define TESTING
#include "test/test.h"
#include "mqtt/packet.h"

TEST(fixed_header_connect) {
    // CONNECT with remaining length 12
    u8 buf[] = {0x10, 0x0c};
    struct mqtt_fixed_header hdr;
    i32 rc = mqtt_parse_fixed_header(buf, sizeof(buf), &hdr);
    ASSERT_EQ(rc, 2);
    ASSERT_EQ(hdr.type, MQTT_CONNECT);
    ASSERT_EQ(hdr.flags, 0);
    ASSERT_EQ(hdr.remaining_len, 12);
}

TEST(fixed_header_publish_qos1) {
    // PUBLISH QoS 1, remaining length 10
    u8 buf[] = {0x32, 0x0a}; // 0x32 = PUBLISH(3) << 4 | QoS1(2)
    struct mqtt_fixed_header hdr;
    i32 rc = mqtt_parse_fixed_header(buf, sizeof(buf), &hdr);
    ASSERT_EQ(rc, 2);
    ASSERT_EQ(hdr.type, MQTT_PUBLISH);
    ASSERT_EQ(hdr.flags, 0x02); // QoS 1
    ASSERT_EQ(hdr.remaining_len, 10);
}

TEST(fixed_header_vbi_two_bytes) {
    // Remaining length 128 requires 2 bytes: 0x80 0x01
    u8 buf[] = {0x10, 0x80, 0x01};
    struct mqtt_fixed_header hdr;
    i32 rc = mqtt_parse_fixed_header(buf, sizeof(buf), &hdr);
    ASSERT_EQ(rc, 3);
    ASSERT_EQ(hdr.remaining_len, 128);
}

TEST(fixed_header_incomplete) {
    u8 buf[] = {0x10};
    struct mqtt_fixed_header hdr;
    i32 rc = mqtt_parse_fixed_header(buf, sizeof(buf), &hdr);
    ASSERT_EQ(rc, MQTT_INCOMPLETE);
}

TEST(packet_complete_yes) {
    // Complete PINGREQ: type + remaining_len(0)
    u8 buf[] = {0xc0, 0x00};
    i32 rc   = mqtt_packet_complete(buf, sizeof(buf));
    ASSERT_EQ(rc, 2);
}

TEST(packet_complete_no) {
    // CONNECT header says 12 bytes follow, but we only have 4
    u8 buf[] = {0x10, 0x0c, 0x00, 0x00};
    i32 rc   = mqtt_packet_complete(buf, sizeof(buf));
    ASSERT_EQ(rc, MQTT_INCOMPLETE);
}

TEST(parse_string) {
    // Length-prefixed string "hello"
    u8 buf[] = {0x00, 0x05, 'h', 'e', 'l', 'l', 'o'};
    struct mqtt_str str;
    i32 rc = mqtt_parse_string(buf, sizeof(buf), &str);
    ASSERT_EQ(rc, 7);
    ASSERT_EQ(str.len, 5);
    ASSERT(str.ptr[0] == 'h');
    ASSERT(str.ptr[4] == 'o');
}

TEST(read_u16) {
    u8 buf[] = {0x01, 0x00}; // 256 in big-endian
    ASSERT_EQ(mqtt_read_u16(buf), 256);
}

TEST(encode_remaining_len_one_byte) {
    u8 buf[4];
    u32 n = mqtt_encode_remaining_len(buf, 127);
    ASSERT_EQ(n, 1);
    ASSERT_EQ(buf[0], 127);
}

TEST(encode_remaining_len_two_bytes) {
    u8 buf[4];
    u32 n = mqtt_encode_remaining_len(buf, 128);
    ASSERT_EQ(n, 2);
    ASSERT_EQ(buf[0], 0x80);
    ASSERT_EQ(buf[1], 0x01);
}

// NOLINTEND(readability-magic-numbers,cppcoreguidelines-avoid-magic-numbers)
