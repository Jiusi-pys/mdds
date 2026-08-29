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

#include <algorithm>
#include <cstring>
#include <random>

#include "gtest/gtest.h"
#include "mdds/fragment.hpp"
#include "mdds/qos.hpp"
#include "mdds/transport.hpp"

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

std::vector<uint8_t> make_payload(size_t len)
{
  std::vector<uint8_t> p(len);
  for (size_t i = 0; i < len; ++i) {
    p[i] = static_cast<uint8_t>((i * 31 + 7) & 0xff);
  }
  return p;
}

TEST(Fragmenter, SmallPayloadStaysSingleDataFrame)
{
  auto writer = make_test_guid(0x01);
  auto payload = make_payload(100);
  auto frames = mdds::Fragmenter::fragment(
    writer, 1, 1000, payload.data(), payload.size(), mdds::kDefaultMaxPayload);
  ASSERT_EQ(frames.size(), 1u);

  mdds::FrameHeader hdr{};
  ASSERT_TRUE(mdds::decode_header(frames[0].data(), frames[0].size(), hdr));
  EXPECT_EQ(hdr.type, mdds::FrameType::DATA);
}

TEST(Fragmenter, LargePayloadSplitsIntoFragsWithinLimit)
{
  auto writer = make_test_guid(0x02);
  auto payload = make_payload(10000);
  const size_t max_payload = 1024;  // tight channel
  auto frames = mdds::Fragmenter::fragment(
    writer, 9, 1000, payload.data(), payload.size(), max_payload);
  ASSERT_GT(frames.size(), 1u);
  for (const auto & f : frames) {
    EXPECT_LE(f.size(), max_payload);
    mdds::FrameHeader hdr{};
    ASSERT_TRUE(mdds::decode_header(f.data(), f.size(), hdr));
    EXPECT_EQ(hdr.type, mdds::FrameType::DATA_FRAG);
  }
}

TEST(Fragmenter, TinyMaxPayloadCannotProgress)
{
  auto writer = make_test_guid(0x03);
  auto payload = make_payload(10000);
  auto frames = mdds::Fragmenter::fragment(writer, 1, 1000, payload.data(), payload.size(), 16);
  EXPECT_TRUE(frames.empty());
}

TEST(Reassembler, RoundtripInOrder)
{
  auto writer = make_test_guid(0x04);
  auto payload = make_payload(9000);
  auto frames = mdds::Fragmenter::fragment(writer, 5, 1000, payload.data(), payload.size(), 1024);
  ASSERT_GT(frames.size(), 1u);

  mdds::Reassembler reasm;
  std::vector<uint8_t> out;
  size_t done_at = 0;
  for (size_t i = 0; i < frames.size(); ++i) {
    mdds::DataFragBody body{};
    ASSERT_TRUE(mdds::decode_data_frag(frames[i].data(), frames[i].size(), body));
    if (reasm.add_fragment(body, out)) {
      done_at = i + 1;
    }
  }
  EXPECT_EQ(done_at, frames.size());
  EXPECT_EQ(out, payload);
  EXPECT_EQ(reasm.pending_count(), 0u);
}

TEST(Reassembler, RoundtripOutOfOrder)
{
  auto writer = make_test_guid(0x05);
  auto payload = make_payload(9000);
  auto frames = mdds::Fragmenter::fragment(writer, 6, 1000, payload.data(), payload.size(), 1024);
  ASSERT_GT(frames.size(), 2u);

  std::shuffle(frames.begin(), frames.end(), std::mt19937(42));

  mdds::Reassembler reasm;
  std::vector<uint8_t> out;
  bool complete = false;
  for (const auto & f : frames) {
    mdds::DataFragBody body{};
    ASSERT_TRUE(mdds::decode_data_frag(f.data(), f.size(), body));
    if (reasm.add_fragment(body, out)) {
      complete = true;
    }
  }
  EXPECT_TRUE(complete);
  EXPECT_EQ(out, payload);
}

TEST(Reassembler, DuplicateFragmentsAreIdempotent)
{
  auto writer = make_test_guid(0x06);
  auto payload = make_payload(5000);
  auto frames = mdds::Fragmenter::fragment(writer, 7, 1000, payload.data(), payload.size(), 1024);
  ASSERT_GT(frames.size(), 1u);

  mdds::Reassembler reasm;
  std::vector<uint8_t> out;
  bool complete = false;
  for (const auto & f : frames) {
    mdds::DataFragBody body{};
    ASSERT_TRUE(mdds::decode_data_frag(f.data(), f.size(), body));
    // feed each fragment twice (retransmit scenario)
    bool first = reasm.add_fragment(body, out);
    bool second = first ? false : reasm.add_fragment(body, out);
    complete = complete || first || second;
  }
  EXPECT_TRUE(complete);
  EXPECT_EQ(out, payload);
}

TEST(Reassembler, RejectsInconsistentTotalSize)
{
  auto writer = make_test_guid(0x07);
  auto payload = make_payload(5000);
  auto frames = mdds::Fragmenter::fragment(writer, 8, 1000, payload.data(), payload.size(), 1024);

  mdds::Reassembler reasm;
  std::vector<uint8_t> out;
  mdds::DataFragBody body{};
  ASSERT_TRUE(mdds::decode_data_frag(frames[0].data(), frames[0].size(), body));
  EXPECT_FALSE(reasm.add_fragment(body, out));
  // same writer+seq, different total_size -> rejected, original state kept
  EXPECT_FALSE(
    reasm.add_fragment(writer, 8, 4000, 0, body.payload, body.payload_len, out));
  EXPECT_EQ(reasm.pending_count(), 1u);
}

TEST(Reassembler, SweepExpiredDropsStale)
{
  auto writer = make_test_guid(0x08);
  auto payload = make_payload(5000);
  auto frames = mdds::Fragmenter::fragment(writer, 9, 1000, payload.data(), payload.size(), 1024);

  mdds::Reassembler reasm(std::chrono::milliseconds(100));
  std::vector<uint8_t> out;
  mdds::DataFragBody body{};
  ASSERT_TRUE(mdds::decode_data_frag(frames[0].data(), frames[0].size(), body));
  EXPECT_FALSE(reasm.add_fragment(body, out));
  EXPECT_EQ(reasm.pending_count(), 1u);

  auto future = mdds::Reassembler::Clock::now() + std::chrono::seconds(10);
  EXPECT_EQ(reasm.sweep_expired(future), 1u);
  EXPECT_EQ(reasm.pending_count(), 0u);
}

TEST(Reassembler, DropWriterClearsOnlyThatWriter)
{
  auto wa = make_test_guid(0x09);
  auto wb = make_test_guid(0x0a);
  mdds::Reassembler reasm;
  std::vector<uint8_t> out;
  uint8_t b[4] = {1, 2, 3, 4};
  EXPECT_FALSE(reasm.add_fragment(wa, 1, 100, 0, b, 4, out));
  EXPECT_FALSE(reasm.add_fragment(wb, 1, 100, 0, b, 4, out));
  EXPECT_EQ(reasm.pending_count(), 2u);
  reasm.drop_writer(wa);
  EXPECT_EQ(reasm.pending_count(), 1u);
}

TEST(Qos, EncodeDecodeRoundtrip)
{
  mdds::QosProfile q;
  q.reliability = mdds::Reliability::RELIABLE;
  q.durability = mdds::Durability::TRANSIENT_LOCAL;
  q.history = mdds::History::KEEP_ALL;
  q.liveliness = mdds::Liveliness::MANUAL_BY_TOPIC;
  q.depth = 42;
  q.deadline_ms = 1000;
  q.lifespan_ms = 2000;
  q.liveliness_lease_ms = 3000;

  uint8_t buf[mdds::kQosWireSize];
  mdds::encode_qos(buf, q);

  mdds::QosProfile out;
  ASSERT_TRUE(mdds::decode_qos(buf, sizeof(buf), out));
  EXPECT_EQ(out.reliability, q.reliability);
  EXPECT_EQ(out.durability, q.durability);
  EXPECT_EQ(out.history, q.history);
  EXPECT_EQ(out.liveliness, q.liveliness);
  EXPECT_EQ(out.depth, q.depth);
  EXPECT_EQ(out.deadline_ms, q.deadline_ms);
  EXPECT_EQ(out.lifespan_ms, q.lifespan_ms);
  EXPECT_EQ(out.liveliness_lease_ms, q.liveliness_lease_ms);
}

TEST(Qos, CompatibilityMatrix)
{
  mdds::QosProfile be;
  be.reliability = mdds::Reliability::BEST_EFFORT;
  mdds::QosProfile rel;
  rel.reliability = mdds::Reliability::RELIABLE;

  EXPECT_TRUE(mdds::qos_compatible(be, be));
  EXPECT_TRUE(mdds::qos_compatible(be, rel));
  EXPECT_TRUE(mdds::qos_compatible(rel, rel));
  EXPECT_FALSE(mdds::qos_compatible(rel, be));

  mdds::QosProfile tl;
  tl.durability = mdds::Durability::TRANSIENT_LOCAL;
  mdds::QosProfile vol;
  vol.durability = mdds::Durability::VOLATILE;
  EXPECT_FALSE(mdds::qos_compatible(tl, vol));
  EXPECT_TRUE(mdds::qos_compatible(vol, tl));
}

}  // namespace
