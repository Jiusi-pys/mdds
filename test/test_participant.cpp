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
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "gtest/gtest.h"
#include "mdds/frame.hpp"
#include "mdds/participant.hpp"

namespace
{

using namespace std::chrono_literals;

constexpr uint16_t kTestBasePort = 48017;

mdds::ParticipantConfig test_config()
{
  mdds::ParticipantConfig cfg;
  cfg.domain_id = 88;
  cfg.use_dsoftbus = false;
  cfg.announce_period_ms = 200;
  cfg.udp_base_port = kTestBasePort;
  cfg.udp_port_count = 16;
  cfg.udp_announce_ms = 50;
  return cfg;
}

mdds::QosProfile default_qos()
{
  mdds::QosProfile q;
  q.reliability = mdds::Reliability::RELIABLE;
  q.history = mdds::History::KEEP_LAST;
  q.depth = 10;
  return q;
}

class ParticipantFixture : public ::testing::Test
{
protected:
  void SetUp() override
  {
    pa_ = mdds::Participant::create(test_config());
    pb_ = mdds::Participant::create(test_config());
    ASSERT_NE(pa_, nullptr);
    ASSERT_NE(pb_, nullptr);
    pa_->set_graph_callback([this] {graph_notify(pa_graph_);});
    pb_->set_graph_callback([this] {graph_notify(pb_graph_);});
  }

  void TearDown() override
  {
    pa_.reset();
    pb_.reset();
  }

  void graph_notify(std::atomic<size_t> & counter)
  {
    ++counter;
    graph_cv_.notify_all();
  }

  bool wait_graph(std::atomic<size_t> & counter, size_t n, std::chrono::milliseconds t = 10s)
  {
    auto deadline = std::chrono::steady_clock::now() + t;
    while (std::chrono::steady_clock::now() < deadline) {
      if (counter.load() >= n) {
        return true;
      }
      std::this_thread::sleep_for(10ms);
    }
    return counter.load() >= n;
  }

  bool wait_take(
    mdds::Reader * r, std::vector<uint8_t> & out, mdds::MessageInfo & info,
    std::chrono::milliseconds t = 10s)
  {
    auto deadline = std::chrono::steady_clock::now() + t;
    while (std::chrono::steady_clock::now() < deadline) {
      if (r->take(out, info)) {
        return true;
      }
      std::this_thread::sleep_for(5ms);
    }
    return false;
  }

  std::unique_ptr<mdds::Participant> pa_, pb_;
  std::atomic<size_t> pa_graph_{0}, pb_graph_{0};
  std::condition_variable graph_cv_;
};

TEST_F(ParticipantFixture, DiscoveryAndMatch)
{
  auto * w = pa_->create_writer("/chatter", "std_msgs::msg::String", default_qos());
  auto * r = pb_->create_reader("/chatter", "std_msgs::msg::String", default_qos());

  ASSERT_TRUE(wait_graph(pb_graph_, 1));
  ASSERT_TRUE(wait_graph(pa_graph_, 1));  // discovery is symmetric; wait both ways
  EXPECT_EQ(pa_->remote_participants().size(), 1u);
  EXPECT_EQ(pb_->remote_participants().size(), 1u);

  // both sides should converge on matched counts
  auto deadline = std::chrono::steady_clock::now() + 10s;
  while (std::chrono::steady_clock::now() < deadline) {
    if (w->matched_count() == 1 && r->matched_count() == 1) {
      break;
    }
    std::this_thread::sleep_for(10ms);
  }
  EXPECT_EQ(w->matched_count(), 1u);
  EXPECT_EQ(r->matched_count(), 1u);
}

TEST_F(ParticipantFixture, PublishSubscribeRoundtrip)
{
  auto * w = pa_->create_writer("/chatter", "std_msgs::msg::String", default_qos());
  auto * r = pb_->create_reader("/chatter", "std_msgs::msg::String", default_qos());
  ASSERT_TRUE(wait_graph(pb_graph_, 1));

  const char msg[] = "hello mdds";
  ASSERT_TRUE(w->write(reinterpret_cast<const uint8_t *>(msg), sizeof(msg)));

  std::vector<uint8_t> out;
  mdds::MessageInfo info;
  ASSERT_TRUE(wait_take(r, out, info));
  EXPECT_EQ(out.size(), sizeof(msg));
  EXPECT_EQ(0, std::memcmp(out.data(), msg, sizeof(msg)));
  EXPECT_EQ(info.writer_guid, w->guid());
  EXPECT_EQ(info.seq, 1u);
}

TEST_F(ParticipantFixture, LargeMessageFragmentationEndToEnd)
{
  auto * w = pa_->create_writer("/big", "test/Big", default_qos());
  auto * r = pb_->create_reader("/big", "test/Big", default_qos());
  ASSERT_TRUE(wait_graph(pb_graph_, 1));

  std::vector<uint8_t> big(300 * 1024);  // 300 KB > UDP loopback max_payload
  for (size_t i = 0; i < big.size(); ++i) {
    big[i] = static_cast<uint8_t>(i * 7 + 3);
  }
  ASSERT_TRUE(w->write(big.data(), big.size()));

  std::vector<uint8_t> out;
  mdds::MessageInfo info;
  ASSERT_TRUE(wait_take(r, out, info, 30s));
  EXPECT_EQ(out, big);
}

TEST_F(ParticipantFixture, TopicMismatchNoDelivery)
{
  auto * w = pa_->create_writer("/chatter", "std_msgs::msg::String", default_qos());
  auto * r = pb_->create_reader("/other", "std_msgs::msg::String", default_qos());
  ASSERT_TRUE(wait_graph(pb_graph_, 1));

  const char msg[] = "x";
  ASSERT_TRUE(w->write(reinterpret_cast<const uint8_t *>(msg), sizeof(msg)));

  std::vector<uint8_t> out;
  mdds::MessageInfo info;
  EXPECT_FALSE(wait_take(r, out, info, 500ms));
}

TEST_F(ParticipantFixture, DuplicateDeliverySuppressed)
{
  auto * w = pa_->create_writer("/chatter", "std_msgs::msg::String", default_qos());
  auto * r = pb_->create_reader("/chatter", "std_msgs::msg::String", default_qos());
  ASSERT_TRUE(wait_graph(pb_graph_, 1));

  const char msg[] = "dedup";
  ASSERT_TRUE(w->write(reinterpret_cast<const uint8_t *>(msg), sizeof(msg)));

  std::vector<uint8_t> out;
  mdds::MessageInfo info;
  ASSERT_TRUE(wait_take(r, out, info));
  // a retransmitted duplicate of the same seq must be dropped
  ASSERT_FALSE(wait_take(r, out, info, 300ms));
}

TEST_F(ParticipantFixture, KeepLastDepthOnReaderQueue)
{
  auto qos = default_qos();
  qos.depth = 2;
  auto * w = pa_->create_writer("/chatter", "std_msgs::msg::String", default_qos());
  auto * r = pb_->create_reader("/chatter", "std_msgs::msg::String", qos);
  ASSERT_TRUE(wait_graph(pb_graph_, 1));

  for (int i = 0; i < 5; ++i) {
    std::string m = "m" + std::to_string(i);
    ASSERT_TRUE(w->write(reinterpret_cast<const uint8_t *>(m.data()), m.size()));
  }

  auto deadline = std::chrono::steady_clock::now() + 10s;
  while (std::chrono::steady_clock::now() < deadline && r->available() < 2) {
    std::this_thread::sleep_for(10ms);
  }
  // reader queue is capped at depth=2, oldest dropped
  EXPECT_LE(r->available(), 2u);
}

TEST_F(ParticipantFixture, LocalDeliveryAndIgnoreLocal)
{
  auto * w = pa_->create_writer("/local", "test/Local", default_qos());
  auto * r_normal = pa_->create_reader("/local", "test/Local", default_qos());
  auto * r_ign =
    pa_->create_reader("/local", "test/Local", default_qos(), /*ignore_local=*/true);

  // local endpoints match immediately, no discovery round trip needed
  EXPECT_EQ(w->matched_count(), 2u);
  EXPECT_EQ(r_normal->matched_count(), 1u);

  const char msg[] = "local";
  ASSERT_TRUE(w->write(reinterpret_cast<const uint8_t *>(msg), sizeof(msg)));

  std::vector<uint8_t> out;
  mdds::MessageInfo info;
  ASSERT_TRUE(wait_take(r_normal, out, info, 2s));
  EXPECT_EQ(out.size(), sizeof(msg));
  EXPECT_EQ(info.writer_guid, w->guid());
  // the ignore_local reader must not see samples from this participant
  EXPECT_FALSE(wait_take(r_ign, out, info, 300ms));
}

TEST_F(ParticipantFixture, PeerDownClearsGraph)
{
  auto pc = mdds::Participant::create(test_config());
  ASSERT_NE(pc, nullptr);
  pc->create_writer("/chatter", "std_msgs::msg::String", default_qos());
  const mdds::Guid pc_guid = pc->guid();
  ASSERT_TRUE(wait_graph(pb_graph_, 1, 10s));

  // pa must actually see pc before we can test its departure
  auto seen_deadline = std::chrono::steady_clock::now() + 10s;
  bool seen = false;
  while (std::chrono::steady_clock::now() < seen_deadline && !seen) {
    for (const auto & s : pa_->remote_participants()) {
      seen = seen || s.participant_guid == pc_guid;
    }
    std::this_thread::sleep_for(50ms);
  }
  ASSERT_TRUE(seen);

  pc.reset();  // participant destroyed -> transport stops -> peers time out
  // udp announce 50ms, timeout 4x = 200ms + sweep granularity
  auto deadline = std::chrono::steady_clock::now() + 15s;
  bool gone = false;
  while (std::chrono::steady_clock::now() < deadline && !gone) {
    auto remotes = pa_->remote_participants();
    gone = std::none_of(
      remotes.begin(), remotes.end(),
      [&](const mdds::ParticipantSnapshot & s) {return s.participant_guid == pc_guid;});
    std::this_thread::sleep_for(50ms);
  }
  // pc is gone; pb (still alive) remains visible
  EXPECT_TRUE(gone);
  EXPECT_FALSE(pa_->remote_participants().empty());
}

// ---- phase 2 QoS: transient_local / lifespan / liveliness ----

constexpr uint16_t kTestBasePort2 = 48137;

mdds::ParticipantConfig test_config_2()
{
  mdds::ParticipantConfig cfg = test_config();
  cfg.udp_base_port = kTestBasePort2;
  return cfg;
}

mdds::QosProfile transient_local_qos()
{
  mdds::QosProfile q = default_qos();
  q.durability = mdds::Durability::TRANSIENT_LOCAL;
  return q;
}

class ParticipantQosFixture : public ParticipantFixture
{
protected:
  void SetUp() override
  {
    // Rebuild both participants on the phase-2 port range.
    pa_.reset();
    pb_.reset();
    pa_ = mdds::Participant::create(test_config_2());
    pb_ = mdds::Participant::create(test_config_2());
    ASSERT_NE(pa_, nullptr);
    ASSERT_NE(pb_, nullptr);
    pa_->set_graph_callback([this] {graph_notify(pa_graph_);});
    pb_->set_graph_callback([this] {graph_notify(pb_graph_);});
  }
};

TEST_F(ParticipantQosFixture, TransientLocalReplayToLateRemoteReader)
{
  auto * w = pa_->create_writer("/tl", "test/Tl", transient_local_qos());
  for (int i = 0; i < 3; ++i) {
    std::string m = "hist" + std::to_string(i);
    ASSERT_TRUE(w->write(reinterpret_cast<const uint8_t *>(m.data()), m.size()));
  }

  // Reader joins after the samples were written: history must be replayed.
  auto * r = pb_->create_reader("/tl", "test/Tl", transient_local_qos());
  for (int i = 0; i < 3; ++i) {
    std::vector<uint8_t> out;
    mdds::MessageInfo info;
    ASSERT_TRUE(wait_take(r, out, info)) << "missing replayed sample " << i;
    std::string expect = "hist" + std::to_string(i);
    EXPECT_EQ(out.size(), expect.size());
    EXPECT_EQ(0, std::memcmp(out.data(), expect.data(), expect.size()));
    EXPECT_EQ(info.seq, static_cast<uint64_t>(i + 1));
  }
}

TEST_F(ParticipantQosFixture, TransientLocalReplayToLateLocalReader)
{
  auto * w = pa_->create_writer("/tl_local", "test/Tl", transient_local_qos());
  for (int i = 0; i < 2; ++i) {
    std::string m = "lh" + std::to_string(i);
    ASSERT_TRUE(w->write(reinterpret_cast<const uint8_t *>(m.data()), m.size()));
  }

  auto * r = pa_->create_reader("/tl_local", "test/Tl", transient_local_qos());
  for (int i = 0; i < 2; ++i) {
    std::vector<uint8_t> out;
    mdds::MessageInfo info;
    ASSERT_TRUE(wait_take(r, out, info, 2s)) << "missing local replayed sample " << i;
    EXPECT_EQ(info.writer_guid, w->guid());
    EXPECT_EQ(info.seq, static_cast<uint64_t>(i + 1));
  }
  // no duplicates once the live stream continues
  const char msg[] = "live";
  ASSERT_TRUE(w->write(reinterpret_cast<const uint8_t *>(msg), sizeof(msg)));
  std::vector<uint8_t> out;
  mdds::MessageInfo info;
  ASSERT_TRUE(wait_take(r, out, info, 2s));
  EXPECT_EQ(info.seq, 3u);
  EXPECT_FALSE(wait_take(r, out, info, 300ms));
}

TEST_F(ParticipantQosFixture, VolatileReaderGetsNoHistory)
{
  auto * w = pa_->create_writer("/tl_vol", "test/Tl", transient_local_qos());
  const char msg[] = "old";
  ASSERT_TRUE(w->write(reinterpret_cast<const uint8_t *>(msg), sizeof(msg)));

  // Volatile reader matches a transient_local writer (durability RxO) but
  // must not receive samples written before it existed.
  auto * r = pb_->create_reader("/tl_vol", "test/Tl", default_qos());
  std::vector<uint8_t> out;
  mdds::MessageInfo info;
  EXPECT_FALSE(wait_take(r, out, info, 1s));

  const char msg2[] = "new";
  ASSERT_TRUE(w->write(reinterpret_cast<const uint8_t *>(msg2), sizeof(msg2)));
  ASSERT_TRUE(wait_take(r, out, info));
  EXPECT_EQ(out.size(), sizeof(msg2));
  EXPECT_EQ(info.seq, 2u);
}

TEST_F(ParticipantQosFixture, LifespanDropsExpiredReplay)
{
  auto qos = transient_local_qos();
  qos.lifespan_ms = 400;
  auto * w = pa_->create_writer("/ls", "test/Ls", qos);
  const char msg[] = "shortlived";
  ASSERT_TRUE(w->write(reinterpret_cast<const uint8_t *>(msg), sizeof(msg)));

  std::this_thread::sleep_for(700ms);  // sample expires in the writer history

  auto * r = pb_->create_reader("/ls", "test/Ls", transient_local_qos());
  std::vector<uint8_t> out;
  mdds::MessageInfo info;
  EXPECT_FALSE(wait_take(r, out, info, 1s));  // expired history is not replayed
}

TEST_F(ParticipantQosFixture, LifespanSweepsReaderQueue)
{
  auto qos = default_qos();
  qos.lifespan_ms = 300;
  auto * w = pa_->create_writer("/lsq", "test/Ls", qos);
  auto * r = pb_->create_reader("/lsq", "test/Ls", default_qos());
  ASSERT_TRUE(wait_graph(pb_graph_, 1));
  // wait until pa has discovered the reader, else the write never goes on wire
  auto match_deadline = std::chrono::steady_clock::now() + 10s;
  while (std::chrono::steady_clock::now() < match_deadline && w->matched_count() == 0) {
    std::this_thread::sleep_for(10ms);
  }
  ASSERT_EQ(w->matched_count(), 1u);

  const char msg[] = "a";
  ASSERT_TRUE(w->write(reinterpret_cast<const uint8_t *>(msg), sizeof(msg)));
  std::this_thread::sleep_for(600ms);  // let it expire while sitting in the queue

  std::vector<uint8_t> out;
  mdds::MessageInfo info;
  EXPECT_FALSE(wait_take(r, out, info, 500ms));  // lazily swept at take time
  EXPECT_EQ(r->available(), 0u);
}

TEST_F(ParticipantQosFixture, LivelinessObservationApis)
{
  auto * w = pa_->create_writer("/live", "test/Live", default_qos());
  auto * r = pb_->create_reader("/live", "test/Live", default_qos());
  ASSERT_TRUE(wait_graph(pb_graph_, 1));

  // AUTOMATIC: participant-level last-seen comes from ANNOUNCE frames.
  uint64_t last_ms = 0;
  ASSERT_TRUE(wait_graph(pa_graph_, 1));
  EXPECT_TRUE(pb_->remote_participant_last_seen_ms(pa_->guid(), last_ms));
  EXPECT_GT(last_ms, 0u);

  // Writer-level last-seen only appears once DATA/HEARTBEAT arrives.
  uint64_t w_last = 0;
  pb_->remote_writer_last_seen_ms(w->guid(), w_last);  // may or may not be set yet
  const char msg[] = "ping";
  ASSERT_TRUE(w->write(reinterpret_cast<const uint8_t *>(msg), sizeof(msg)));
  std::vector<uint8_t> out;
  mdds::MessageInfo info;
  ASSERT_TRUE(wait_take(r, out, info));
  ASSERT_TRUE(pb_->remote_writer_last_seen_ms(w->guid(), w_last));
  EXPECT_GT(w_last, 0u);
}

TEST_F(ParticipantQosFixture, AssertLivelinessSendsHeartbeat)
{
  // Best-effort writer: no periodic heartbeats, so the only way the remote
  // side learns about it between writes is an explicit assert.
  mdds::QosProfile be = default_qos();
  be.reliability = mdds::Reliability::BEST_EFFORT;
  auto * w = pa_->create_writer("/assert", "test/Live", be);
  auto * r = pb_->create_reader("/assert", "test/Live", be);
  ASSERT_TRUE(wait_graph(pb_graph_, 1));
  ASSERT_TRUE(wait_graph(pa_graph_, 1));
  static_cast<void>(r);

  uint64_t before = 0;
  pb_->remote_writer_last_seen_ms(w->guid(), before);

  w->assert_liveliness();
  auto deadline = std::chrono::steady_clock::now() + 5s;
  uint64_t after = 0;
  while (std::chrono::steady_clock::now() < deadline) {
    if (pb_->remote_writer_last_seen_ms(w->guid(), after) && after > before) {
      break;
    }
    std::this_thread::sleep_for(20ms);
  }
  ASSERT_TRUE(pb_->remote_writer_last_seen_ms(w->guid(), after));
  EXPECT_GT(after, before);
}

// ---- reliability: precise NACK / GAP / KEEP_ALL backpressure (wire v3) ----

constexpr uint16_t kTestBasePort3 = 48257;

mdds::ParticipantConfig test_config_3()
{
  mdds::ParticipantConfig cfg = test_config();
  cfg.udp_base_port = kTestBasePort3;
  cfg.announce_period_ms = 100;
  return cfg;
}

bool wait_matched(mdds::Writer * w, size_t n, std::chrono::milliseconds t = 10s)
{
  auto deadline = std::chrono::steady_clock::now() + t;
  while (std::chrono::steady_clock::now() < deadline) {
    if (w->matched_count() >= n) {
      return true;
    }
    std::this_thread::sleep_for(10ms);
  }
  return false;
}

/// Reader-side counterpart: the reader has discovered the remote writer.
/// DATA from a not-yet-discovered writer is dropped (not counted lost), so
/// tests must wait for this before writing, or early samples vanish.
bool wait_reader_matched(mdds::Reader * r, size_t n, std::chrono::milliseconds t = 10s)
{
  auto deadline = std::chrono::steady_clock::now() + t;
  while (std::chrono::steady_clock::now() < deadline) {
    if (r->matched_count() >= n) {
      return true;
    }
    std::this_thread::sleep_for(10ms);
  }
  return false;
}

/// True when the frame is a DATA frame carrying `seq`.
bool is_data_seq(const uint8_t * frame, size_t len, uint64_t seq)
{
  mdds::FrameHeader hdr{};
  if (!mdds::decode_header(frame, len, hdr) || hdr.type != mdds::FrameType::DATA) {
    return false;
  }
  mdds::DataBody body{};
  return mdds::decode_data(frame, len, body) && body.seq == seq;
}

// A dropped DATA frame is healed by precise NACK + retransmission, and the
// reader never counts the healed hole as lost.
TEST(ReliabilityTest, RetransmitHealsDroppedFrame)
{
  std::atomic<size_t> seq2_sends{0};
  std::atomic<bool> dropped_once{false};
  std::atomic<uint64_t> nack_bitmap_at_base2{0};
  auto cfg_a = test_config_3();
  cfg_a.test_send_hook = [&](const uint8_t * f, size_t n) {
    if (is_data_seq(f, n, 2)) {
      ++seq2_sends;
      if (!dropped_once.exchange(true)) {
        return true;  // drop the first copy of seq 2
      }
    }
    return false;
  };
  auto cfg_b = test_config_3();
  cfg_b.test_send_hook = [&](const uint8_t * f, size_t n) {
    mdds::FrameHeader hdr{};
    if (mdds::decode_header(f, n, hdr) && hdr.type == mdds::FrameType::ACKNACK) {
      mdds::AckNackBody body{};
      if (mdds::decode_acknack(f, n, body) && body.base_seq == 2 && body.bitmap != 0) {
        nack_bitmap_at_base2 = body.bitmap;
      }
    }
    return false;
  };
  auto pa = mdds::Participant::create(cfg_a);
  auto pb = mdds::Participant::create(cfg_b);
  ASSERT_NE(pa, nullptr);
  ASSERT_NE(pb, nullptr);

  auto * w = pa->create_writer("/rel", "test/Rel", default_qos());
  auto * r = pb->create_reader("/rel", "test/Rel", default_qos());
  ASSERT_TRUE(wait_matched(w, 1));
  ASSERT_TRUE(wait_reader_matched(r, 1));

  for (int i = 0; i < 3; ++i) {
    std::string m = "s" + std::to_string(i + 1);
    ASSERT_TRUE(w->write(reinterpret_cast<const uint8_t *>(m.data()), m.size()));
  }

  std::set<uint64_t> got;
  auto deadline = std::chrono::steady_clock::now() + 10s;
  while (std::chrono::steady_clock::now() < deadline && got.size() < 3) {
    std::vector<uint8_t> out;
    mdds::MessageInfo info;
    if (r->take(out, info)) {
      got.insert(info.seq);
    } else {
      std::this_thread::sleep_for(5ms);
    }
  }
  EXPECT_EQ(got, (std::set<uint64_t>{1, 2, 3}));  // seq 2 healed out of order
  EXPECT_EQ(r->messages_lost(), 0u);  // a healed hole is never counted lost
  EXPECT_GE(seq2_sends.load(), 2u);   // original + at least one retransmit
  // The NACK for base 2 must mark seq 3 (base+1) as already received.
  if (nack_bitmap_at_base2.load() != 0) {
    EXPECT_EQ(nack_bitmap_at_base2.load() & 0b10ULL, 0b10ULL);
  }
}

// A permanently lost seq that the writer has already evicted is closed by a
// GAP frame: counted lost exactly once, NACK loop terminates.
TEST(ReliabilityTest, GapDeclaresEvictedSeqsLost)
{
  std::atomic<size_t> gap_frames{0};
  auto cfg_a = test_config_3();
  cfg_a.test_send_hook = [&](const uint8_t * f, size_t n) {
    mdds::FrameHeader hdr{};
    if (mdds::decode_header(f, n, hdr)) {
      if (hdr.type == mdds::FrameType::GAP) {
        mdds::GapBody g{};
        if (mdds::decode_gap(f, n, g) && g.gap_start == 2 && g.gap_end == 2) {
          ++gap_frames;
        }
      }
      return is_data_seq(f, n, 2);  // seq 2 never reaches the reader
    }
    return false;
  };
  auto pa = mdds::Participant::create(cfg_a);
  auto pb = mdds::Participant::create(test_config_3());
  ASSERT_NE(pa, nullptr);
  ASSERT_NE(pb, nullptr);

  auto writer_qos = default_qos();
  writer_qos.depth = 1;  // writer history holds only the latest sample
  auto * w = pa->create_writer("/gap", "test/Gap", writer_qos);
  auto * r = pb->create_reader("/gap", "test/Gap", default_qos());
  ASSERT_TRUE(wait_matched(w, 1));
  ASSERT_TRUE(wait_reader_matched(r, 1));

  for (int i = 0; i < 4; ++i) {
    std::string m = "s" + std::to_string(i + 1);
    ASSERT_TRUE(w->write(reinterpret_cast<const uint8_t *>(m.data()), m.size()));
  }

  auto deadline = std::chrono::steady_clock::now() + 10s;
  while (std::chrono::steady_clock::now() < deadline && r->messages_lost() != 1) {
    std::this_thread::sleep_for(10ms);
  }
  EXPECT_EQ(r->messages_lost(), 1u);  // exactly seq 2
  EXPECT_GE(gap_frames.load(), 1u);

  std::set<uint64_t> got;
  deadline = std::chrono::steady_clock::now() + 10s;
  while (std::chrono::steady_clock::now() < deadline && got.size() < 3) {
    std::vector<uint8_t> out;
    mdds::MessageInfo info;
    if (r->take(out, info)) {
      got.insert(info.seq);
    } else {
      std::this_thread::sleep_for(5ms);
    }
  }
  EXPECT_EQ(got, (std::set<uint64_t>{1, 3, 4}));

  // Further heartbeats/NACKs must not double-count the healed gap.
  std::this_thread::sleep_for(500ms);
  EXPECT_EQ(r->messages_lost(), 1u);
}

// KEEP_ALL writer history full -> write() rejects instead of silently
// evicting the oldest sample (KEEP_ALL semantics are not negotiable).
TEST(ReliabilityTest, KeepAllWriterBackpressure)
{
  auto p = mdds::Participant::create(test_config_3());
  ASSERT_NE(p, nullptr);
  auto qos = default_qos();
  qos.history = mdds::History::KEEP_ALL;
  auto * w = p->create_writer("/ka", "test/Ka", qos);
  const char msg[] = "x";
  // kKeepAllCap = 256 (participant.cpp): the first 256 writes are cached.
  for (int i = 0; i < 256; ++i) {
    ASSERT_TRUE(w->write(reinterpret_cast<const uint8_t *>(msg), sizeof(msg))) << i;
  }
  EXPECT_FALSE(w->write(reinterpret_cast<const uint8_t *>(msg), sizeof(msg)));
  EXPECT_FALSE(w->write(reinterpret_cast<const uint8_t *>(msg), sizeof(msg)));
}

}  // namespace
