// Copyright 2026 Yusheng Peng
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <cstring>

#include "gtest/gtest.h"
#include "mdds/frame.hpp"

namespace
{

mdds::Guid make_test_guid(uint8_t seed)
{
  mdds::Guid g;
  for (size_t i = 0; i < g.bytes.size(); ++i) {
    g.bytes[i] = static_cast<uint8_t>(seed + i);
  }
  return g;
}

TEST(FrameHeader, EncodeDecodeRoundtrip)
{
  uint8_t buf[mdds::kHeaderSize];
  mdds::encode_header(buf, mdds::FrameType::DATA, 0x1234, 100);

  // header + 100 body bytes must be present to decode
  std::vector<uint8_t> frame(mdds::kHeaderSize + 100, 0);
  std::memcpy(frame.data(), buf, mdds::kHeaderSize);

  mdds::FrameHeader hdr{};
  ASSERT_TRUE(mdds::decode_header(frame.data(), frame.size(), hdr));
  EXPECT_EQ(hdr.type, mdds::FrameType::DATA);
  EXPECT_EQ(hdr.flags, 0x1234);
  EXPECT_EQ(hdr.body_len, 100u);
}

TEST(FrameHeader, RejectsBadMagic)
{
  std::vector<uint8_t> buf(mdds::kHeaderSize, 0);
  buf[0] = 'X';
  mdds::FrameHeader hdr{};
  EXPECT_FALSE(mdds::decode_header(buf.data(), buf.size(), hdr));
}

TEST(FrameHeader, RejectsTruncatedBody)
{
  uint8_t buf[mdds::kHeaderSize];
  mdds::encode_header(buf, mdds::FrameType::DATA, 0, 100);
  mdds::FrameHeader hdr{};
  // only 50 body bytes available but header promises 100
  std::vector<uint8_t> frame(mdds::kHeaderSize + 50, 0);
  std::memcpy(frame.data(), buf, mdds::kHeaderSize);
  EXPECT_FALSE(mdds::decode_header(frame.data(), frame.size(), hdr));
}

TEST(DataFrame, Roundtrip)
{
  mdds::Guid writer = make_test_guid(0x10);
  const char payload[] = "hello mdds";
  auto frame = mdds::encode_data(
    writer, 42, 1234567890, reinterpret_cast<const uint8_t *>(payload), sizeof(payload));

  mdds::DataBody body{};
  ASSERT_TRUE(mdds::decode_data(frame.data(), frame.size(), body));
  EXPECT_EQ(body.writer, writer);
  EXPECT_EQ(body.seq, 42u);
  EXPECT_EQ(body.pub_time_ms, 1234567890u);
  ASSERT_EQ(body.payload_len, sizeof(payload));
  EXPECT_EQ(0, std::memcmp(body.payload, payload, sizeof(payload)));
}

TEST(DataFrame, EmptyPayload)
{
  mdds::Guid writer = make_test_guid(0x20);
  auto frame = mdds::encode_data(writer, 1, 0, nullptr, 0);
  mdds::DataBody body{};
  ASSERT_TRUE(mdds::decode_data(frame.data(), frame.size(), body));
  EXPECT_EQ(body.payload_len, 0u);
}

TEST(DataFragFrame, Roundtrip)
{
  mdds::Guid writer = make_test_guid(0x30);
  std::vector<uint8_t> chunk(1000);
  for (size_t i = 0; i < chunk.size(); ++i) {
    chunk[i] = static_cast<uint8_t>(i);
  }
  auto frame = mdds::encode_data_frag(writer, 7, 999, 10000, 4000, chunk.data(), chunk.size());

  mdds::DataFragBody body{};
  ASSERT_TRUE(mdds::decode_data_frag(frame.data(), frame.size(), body));
  EXPECT_EQ(body.writer, writer);
  EXPECT_EQ(body.seq, 7u);
  EXPECT_EQ(body.pub_time_ms, 999u);
  EXPECT_EQ(body.total_size, 10000u);
  EXPECT_EQ(body.frag_offset, 4000u);
  ASSERT_EQ(body.payload_len, chunk.size());
  EXPECT_EQ(0, std::memcmp(body.payload, chunk.data(), chunk.size()));
}

TEST(DataFragFrame, RejectsRangeOverflow)
{
  mdds::Guid writer = make_test_guid(0x40);
  uint8_t chunk[10] = {};
  // offset 100 + len 10 > total 105
  auto frame = mdds::encode_data_frag(writer, 1, 0, 105, 100, chunk, sizeof(chunk));
  mdds::DataFragBody body{};
  EXPECT_FALSE(mdds::decode_data_frag(frame.data(), frame.size(), body));
}

TEST(AckNackFrame, Roundtrip)
{
  mdds::Guid reader = make_test_guid(0x50);
  mdds::Guid writer = make_test_guid(0x60);
  auto frame = mdds::encode_acknack(reader, writer, 1000, 0xdeadbeefcafef00dULL);

  mdds::AckNackBody body{};
  ASSERT_TRUE(mdds::decode_acknack(frame.data(), frame.size(), body));
  EXPECT_EQ(body.reader, reader);
  EXPECT_EQ(body.writer, writer);
  EXPECT_EQ(body.base_seq, 1000u);
  EXPECT_EQ(body.bitmap, 0xdeadbeefcafef00dULL);
}

TEST(HeartbeatFrame, Roundtrip)
{
  mdds::Guid writer = make_test_guid(0x70);
  auto frame = mdds::encode_heartbeat(writer, 1, 500);

  mdds::HeartbeatBody body{};
  ASSERT_TRUE(mdds::decode_heartbeat(frame.data(), frame.size(), body));
  EXPECT_EQ(body.writer, writer);
  EXPECT_EQ(body.first_seq, 1u);
  EXPECT_EQ(body.last_seq, 500u);
}

TEST(Guid, MakeGuidLayout)
{
  std::array<uint8_t, 12> prefix{};
  for (size_t i = 0; i < prefix.size(); ++i) {
    prefix[i] = static_cast<uint8_t>(i);
  }
  auto g = mdds::make_guid(prefix, mdds::EntityKind::WRITER, 0x010203);
  EXPECT_EQ(g.bytes[12], 1u);
  EXPECT_EQ(g.bytes[13], 0x01u);
  EXPECT_EQ(g.bytes[14], 0x02u);
  EXPECT_EQ(g.bytes[15], 0x03u);
  for (size_t i = 0; i < 12; ++i) {
    EXPECT_EQ(g.bytes[i], i);
  }
}

TEST(Guid, PrefixRandomDistinct)
{
  auto a = mdds::generate_participant_prefix();
  auto b = mdds::generate_participant_prefix();
  EXPECT_NE(a, b);
}

TEST(FrameHeader, RejectsOlderProtocolVersion)
{
  mdds::Guid writer = make_test_guid(0x10);
  auto frame = mdds::encode_data(writer, 1, 0, nullptr, 0);
  frame[4] = 2;  // v2 frame on a v3 decoder
  mdds::FrameHeader hdr{};
  EXPECT_FALSE(mdds::decode_header(frame.data(), frame.size(), hdr));
}

TEST(DataFrame, ReliableFlagRoundtrip)
{
  mdds::Guid writer = make_test_guid(0x11);
  const char payload[] = "x";
  auto frame = mdds::encode_data(
    writer, 1, 0, reinterpret_cast<const uint8_t *>(payload), 1, mdds::kFlagReliable);
  mdds::FrameHeader hdr{};
  ASSERT_TRUE(mdds::decode_header(frame.data(), frame.size(), hdr));
  EXPECT_EQ(hdr.flags & mdds::kFlagReliable, mdds::kFlagReliable);
}

TEST(DataFrame, OriginTrailerRoundtrip)
{
  mdds::Guid writer = make_test_guid(0x12);
  mdds::DataTrailer trailer;
  trailer.has_origin = true;
  trailer.origin_guid = make_test_guid(0x77);
  trailer.origin_seq = 4242;
  const char payload[] = "hello trailer";
  auto frame = mdds::encode_data(
    writer, 5, 100, reinterpret_cast<const uint8_t *>(payload), sizeof(payload),
    mdds::kFlagReliable, &trailer);

  mdds::DataBody body{};
  ASSERT_TRUE(mdds::decode_data(frame.data(), frame.size(), body));
  ASSERT_EQ(body.payload_len, sizeof(payload));
  EXPECT_EQ(0, std::memcmp(body.payload, payload, sizeof(payload)));
  EXPECT_TRUE(body.trailer.has_origin);
  EXPECT_EQ(body.trailer.origin_guid, trailer.origin_guid);
  EXPECT_EQ(body.trailer.origin_seq, 4242u);
}

TEST(DataFrame, NoTrailerDecodesEmpty)
{
  mdds::Guid writer = make_test_guid(0x13);
  auto frame = mdds::encode_data(writer, 1, 0, nullptr, 0);
  mdds::DataBody body{};
  ASSERT_TRUE(mdds::decode_data(frame.data(), frame.size(), body));
  EXPECT_FALSE(body.trailer.has_origin);
}

TEST(DataFrame, UnknownTlvEntrySkipped)
{
  mdds::Guid writer = make_test_guid(0x14);
  auto frame = mdds::encode_data(writer, 1, 0, nullptr, 0);
  // Append an unknown TLV entry (type 0xEE, len 3) to the body.
  const uint32_t body_len = mdds::kDataBodyFixedSize + 5;
  frame.resize(mdds::kHeaderSize + body_len);
  frame[mdds::kHeaderSize + mdds::kDataBodyFixedSize + 0] = 0xEE;
  frame[mdds::kHeaderSize + mdds::kDataBodyFixedSize + 1] = 3;
  // fix up body_len in the header
  frame[8] = static_cast<uint8_t>(body_len >> 24);
  frame[9] = static_cast<uint8_t>(body_len >> 16);
  frame[10] = static_cast<uint8_t>(body_len >> 8);
  frame[11] = static_cast<uint8_t>(body_len & 0xff);

  mdds::DataBody body{};
  ASSERT_TRUE(mdds::decode_data(frame.data(), frame.size(), body));
  EXPECT_EQ(body.payload_len, 0u);
  EXPECT_FALSE(body.trailer.has_origin);
}

TEST(DataFrame, MalformedTrailerKeepsPayload)
{
  mdds::Guid writer = make_test_guid(0x15);
  const char payload[] = "abc";
  auto frame = mdds::encode_data(
    writer, 1, 0, reinterpret_cast<const uint8_t *>(payload), sizeof(payload));
  // Append a truncated TLV entry (declares len 10, only 1 byte present).
  const uint32_t body_len = mdds::kDataBodyFixedSize + sizeof(payload) + 3;
  frame.resize(mdds::kHeaderSize + body_len);
  const size_t tail = mdds::kHeaderSize + mdds::kDataBodyFixedSize + sizeof(payload);
  frame[tail + 0] = mdds::kTlvOriginGuid;
  frame[tail + 1] = 10;
  frame[tail + 2] = 0xAA;
  frame[8] = static_cast<uint8_t>(body_len >> 24);
  frame[9] = static_cast<uint8_t>(body_len >> 16);
  frame[10] = static_cast<uint8_t>(body_len >> 8);
  frame[11] = static_cast<uint8_t>(body_len & 0xff);

  mdds::DataBody body{};
  ASSERT_TRUE(mdds::decode_data(frame.data(), frame.size(), body));
  ASSERT_EQ(body.payload_len, sizeof(payload));
  EXPECT_EQ(0, std::memcmp(body.payload, payload, sizeof(payload)));
  EXPECT_FALSE(body.trailer.has_origin);
}

TEST(DataFragFrame, OriginTrailerRoundtrip)
{
  mdds::Guid writer = make_test_guid(0x31);
  mdds::DataTrailer trailer;
  trailer.has_origin = true;
  trailer.origin_guid = make_test_guid(0x88);
  trailer.origin_seq = 7;
  uint8_t chunk[64];
  for (size_t i = 0; i < sizeof(chunk); ++i) {
    chunk[i] = static_cast<uint8_t>(i);
  }
  auto frame = mdds::encode_data_frag(
    writer, 9, 555, 6400, 1024, chunk, sizeof(chunk), mdds::kFlagReliable, &trailer);

  mdds::DataFragBody body{};
  ASSERT_TRUE(mdds::decode_data_frag(frame.data(), frame.size(), body));
  EXPECT_EQ(body.writer, writer);
  EXPECT_EQ(body.seq, 9u);
  EXPECT_EQ(body.total_size, 6400u);
  EXPECT_EQ(body.frag_offset, 1024u);
  ASSERT_EQ(body.payload_len, sizeof(chunk));
  EXPECT_TRUE(body.trailer.has_origin);
  EXPECT_EQ(body.trailer.origin_guid, trailer.origin_guid);
  EXPECT_EQ(body.trailer.origin_seq, 7u);
}

TEST(GapFrame, Roundtrip)
{
  mdds::Guid writer = make_test_guid(0x90);
  mdds::Guid reader = make_test_guid(0x91);
  auto frame = mdds::encode_gap(writer, reader, 10, 42);

  mdds::GapBody body{};
  ASSERT_TRUE(mdds::decode_gap(frame.data(), frame.size(), body));
  EXPECT_EQ(body.writer, writer);
  EXPECT_EQ(body.reader, reader);
  EXPECT_EQ(body.gap_start, 10u);
  EXPECT_EQ(body.gap_end, 42u);
}

TEST(GapFrame, RejectsWrongType)
{
  mdds::Guid writer = make_test_guid(0x91);
  auto frame = mdds::encode_heartbeat(writer, 1, 2);
  mdds::GapBody body{};
  EXPECT_FALSE(mdds::decode_gap(frame.data(), frame.size(), body));
}

}  // namespace
