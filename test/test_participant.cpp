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
#include <string>
#include <thread>
#include <vector>

#include "gtest/gtest.h"
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

}  // namespace
