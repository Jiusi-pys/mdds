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

#include <chrono>
#include <condition_variable>
#include <cstring>
#include <mutex>
#include <vector>

#include "gtest/gtest.h"
#include "mdds/frame.hpp"
#include "mdds/transport.hpp"

namespace
{

using namespace std::chrono_literals;

constexpr uint16_t kTestBasePort = 47937;

class RecordingListener : public mdds::TransportListener
{
public:
  void on_peer_up(mdds::PeerId peer) override
  {
    std::lock_guard<std::mutex> lock(mutex_);
    ups_.push_back(peer);
    cv_.notify_all();
  }

  void on_peer_down(mdds::PeerId peer) override
  {
    std::lock_guard<std::mutex> lock(mutex_);
    downs_.push_back(peer);
    cv_.notify_all();
  }

  void on_bytes(mdds::PeerId peer, const uint8_t * data, size_t len) override
  {
    std::lock_guard<std::mutex> lock(mutex_);
    received_.emplace_back(peer, std::vector<uint8_t>(data, data + len));
    cv_.notify_all();
  }

  bool wait_for_peer_up(size_t count, std::chrono::milliseconds timeout = 5s)
  {
    std::unique_lock<std::mutex> lock(mutex_);
    return cv_.wait_for(lock, timeout, [&] {return ups_.size() >= count;});
  }

  bool wait_for_peer_down(size_t count, std::chrono::milliseconds timeout = 10s)
  {
    std::unique_lock<std::mutex> lock(mutex_);
    return cv_.wait_for(lock, timeout, [&] {return downs_.size() >= count;});
  }

  bool wait_for_bytes(size_t count, std::chrono::milliseconds timeout = 5s)
  {
    std::unique_lock<std::mutex> lock(mutex_);
    return cv_.wait_for(lock, timeout, [&] {return received_.size() >= count;});
  }

  std::vector<mdds::PeerId> ups()
  {
    std::lock_guard<std::mutex> lock(mutex_);
    return ups_;
  }

  std::vector<std::pair<mdds::PeerId, std::vector<uint8_t>>> received()
  {
    std::lock_guard<std::mutex> lock(mutex_);
    return received_;
  }

private:
  std::mutex mutex_;
  std::condition_variable cv_;
  std::vector<mdds::PeerId> ups_;
  std::vector<mdds::PeerId> downs_;
  std::vector<std::pair<mdds::PeerId, std::vector<uint8_t>>> received_;
};

mdds::TransportConfig test_config()
{
  mdds::TransportConfig cfg;
  cfg.domain_id = 77;
  cfg.udp_base_port = kTestBasePort;
  cfg.udp_port_count = 16;
  cfg.udp_announce_ms = 50;
  return cfg;
}

TEST(UdpLoopback, DiscoverSendReceive)
{
  RecordingListener la, lb;
  auto ta = mdds::make_udp_loopback_transport();
  auto tb = mdds::make_udp_loopback_transport();
  ASSERT_TRUE(ta->start(test_config(), &la));
  ASSERT_TRUE(tb->start(test_config(), &lb));

  ASSERT_TRUE(la.wait_for_peer_up(1));
  ASSERT_TRUE(lb.wait_for_peer_up(1));
  const mdds::PeerId peer_of_a = lb.ups().front();
  const mdds::PeerId peer_of_b = la.ups().front();
  EXPECT_NE(peer_of_a, peer_of_b);

  // A -> B
  const char msg1[] = "hello from A";
  ASSERT_TRUE(
    ta->send(peer_of_b, reinterpret_cast<const uint8_t *>(msg1), sizeof(msg1)));
  ASSERT_TRUE(lb.wait_for_bytes(1));
  {
    auto rec = lb.received();
    EXPECT_EQ(rec.front().first, peer_of_a);
    EXPECT_EQ(rec.front().second.size(), sizeof(msg1));
    EXPECT_EQ(0, std::memcmp(rec.front().second.data(), msg1, sizeof(msg1)));
  }

  // B -> A (bidirectional)
  const char msg2[] = "hello from B";
  ASSERT_TRUE(
    tb->send(peer_of_a, reinterpret_cast<const uint8_t *>(msg2), sizeof(msg2)));
  ASSERT_TRUE(la.wait_for_bytes(1));

  tb->stop();
  ta->stop();
}

TEST(UdpLoopback, SendToUnknownPeerFails)
{
  RecordingListener la;
  auto ta = mdds::make_udp_loopback_transport();
  ASSERT_TRUE(ta->start(test_config(), &la));
  const char msg[] = "x";
  EXPECT_FALSE(ta->send(1, reinterpret_cast<const uint8_t *>(msg), sizeof(msg)));
  ta->stop();
}

TEST(UdpLoopback, PeerDownAfterStop)
{
  RecordingListener la, lb;
  auto ta = mdds::make_udp_loopback_transport();
  auto tb = mdds::make_udp_loopback_transport();
  ASSERT_TRUE(ta->start(test_config(), &la));
  ASSERT_TRUE(tb->start(test_config(), &lb));
  ASSERT_TRUE(la.wait_for_peer_up(1));

  tb->stop();
  // timeout = 4 * 50ms = 200ms; sweep happens per announce period
  EXPECT_TRUE(la.wait_for_peer_down(1, 10s));
  ta->stop();
}

TEST(UdpLoopback, DomainIsolation)
{
  RecordingListener la, lb;
  auto ta = mdds::make_udp_loopback_transport();
  auto tb = mdds::make_udp_loopback_transport();
  auto cfg = test_config();
  ASSERT_TRUE(ta->start(cfg, &la));
  cfg.domain_id = 78;  // different domain, same port range
  ASSERT_TRUE(tb->start(cfg, &lb));

  // presence in another domain must not register a peer
  EXPECT_FALSE(la.wait_for_peer_up(1, 500ms));
  EXPECT_FALSE(lb.wait_for_peer_up(1, 500ms));
  tb->stop();
  ta->stop();
}

TEST(UdpLoopback, LargeFrameRoundtrip)
{
  RecordingListener la, lb;
  auto cfg = test_config();
  cfg.udp_max_payload = 60000;
  auto ta = mdds::make_udp_loopback_transport();
  auto tb = mdds::make_udp_loopback_transport();
  ASSERT_TRUE(ta->start(cfg, &la));
  ASSERT_TRUE(tb->start(cfg, &lb));
  ASSERT_TRUE(la.wait_for_peer_up(1));
  const mdds::PeerId peer_of_b = la.ups().front();

  std::vector<uint8_t> big(50000);
  for (size_t i = 0; i < big.size(); ++i) {
    big[i] = static_cast<uint8_t>(i * 13);
  }
  ASSERT_TRUE(ta->send(peer_of_b, big.data(), big.size()));
  ASSERT_TRUE(lb.wait_for_bytes(1));
  EXPECT_EQ(lb.received().front().second, big);

  // beyond max_payload must fail
  std::vector<uint8_t> too_big(60001, 0);
  EXPECT_FALSE(ta->send(peer_of_b, too_big.data(), too_big.size()));

  tb->stop();
  ta->stop();
}

}  // namespace
