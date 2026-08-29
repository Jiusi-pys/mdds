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

// mdds cross-board smoke test over the DSoftBus transport backend.
//
//   board A: ./mdds_e2e listener [expected_count]   (default 100)
//   board B: ./mdds_e2e talker  [count] [period_ms] (default 100, 100)
//
// Both sides: participant on domain 7, topic /mdds_e2e, reliable keep_last(10).
// Exit code 0 = all expected messages received (listener) or sent (talker).

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <thread>
#include <vector>

#include "mdds/participant.hpp"

namespace
{

constexpr const char * kTopic = "/mdds_e2e";
constexpr const char * kType = "mdds::E2E";
constexpr uint32_t kDomain = 7;

mdds::ParticipantConfig e2e_config()
{
  mdds::ParticipantConfig cfg;
  cfg.domain_id = kDomain;
  cfg.use_udp_loopback = false;  // dsoftbus only: this is the cross-device test
  cfg.use_dsoftbus = true;
  cfg.announce_period_ms = 500;
  return cfg;
}

mdds::QosProfile e2e_qos()
{
  mdds::QosProfile q;
  q.reliability = mdds::Reliability::RELIABLE;
  q.history = mdds::History::KEEP_LAST;
  q.depth = 10;
  return q;
}

int run_talker(int count, int period_ms, int payload_bytes)
{
  auto p = mdds::Participant::create(e2e_config());
  if (!p) {
    std::fprintf(stderr, "talker: participant create failed\n");
    return 1;
  }
  auto * w = p->create_writer(kTopic, kType, e2e_qos());

  // wait for a matched reader (discovery + dsoftbus channel bring-up)
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
  while (w->matched_count() < 1 && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
  }
  if (w->matched_count() < 1) {
    std::fprintf(stderr, "talker: no matched reader within 30s\n");
    return 2;
  }
  std::printf("talker: matched %zu reader(s), sending %d messages\n",
    w->matched_count(), count);

  int sent = 0;
  for (int i = 1; i <= count; ++i) {
    std::vector<uint8_t> buf(static_cast<size_t>(payload_bytes), 'x');
    std::snprintf(
      reinterpret_cast<char *>(buf.data()), buf.size(), "mdds-e2e-%d", i);
    if (!w->write(buf.data(), buf.size())) {
      std::fprintf(stderr, "talker: write %d failed\n", i);
    } else {
      ++sent;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(period_ms));
  }
  std::printf("talker: sent %d/%d\n", sent, count);
  // keep the participant alive briefly so trailing retransmits can flow
  std::this_thread::sleep_for(std::chrono::seconds(2));
  return sent == count ? 0 : 3;
}

int run_listener(int expected)
{
  auto p = mdds::Participant::create(e2e_config());
  if (!p) {
    std::fprintf(stderr, "listener: participant create failed\n");
    return 1;
  }
  auto * r = p->create_reader(kTopic, kType, e2e_qos());
  std::printf("listener: waiting for %d messages on %s\n", expected, kTopic);

  int received = 0;
  std::vector<bool> seen(static_cast<size_t>(expected) + 1, false);
  int duplicates = 0;
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(90);
  while (received < expected && std::chrono::steady_clock::now() < deadline) {
    std::vector<uint8_t> out;
    mdds::MessageInfo info;
    if (!r->take(out, info)) {
      std::this_thread::sleep_for(std::chrono::milliseconds(20));
      continue;
    }
    const std::string s(out.begin(), out.end());
    int idx = 0;
    if (std::sscanf(s.c_str(), "mdds-e2e-%d", &idx) == 1 && idx >= 1 && idx <= expected) {
      if (seen[static_cast<size_t>(idx)]) {
        ++duplicates;
      } else {
        seen[static_cast<size_t>(idx)] = true;
        ++received;
      }
    }
    if (received % 10 == 0) {
      std::printf("listener: %d/%d received (last seq %llu)\n", received, expected,
        static_cast<unsigned long long>(info.seq));
    }
  }
  std::printf("listener: done received=%d/%d duplicates=%d\n", received, expected,
    duplicates);
  return (received == expected && duplicates == 0) ? 0 : 4;
}

}  // namespace

int main(int argc, char ** argv)
{
  if (argc < 2) {
    std::fprintf(stderr,
      "usage: %s talker [count] [period_ms] [payload_bytes] | listener [expected]\n",
      argv[0]);
    return 64;
  }
  if (std::strcmp(argv[1], "talker") == 0) {
    const int count = argc > 2 ? std::atoi(argv[2]) : 100;
    const int period = argc > 3 ? std::atoi(argv[3]) : 100;
    const int payload = argc > 4 ? std::atoi(argv[4]) : 64;
    return run_talker(count, period, payload);
  }
  if (std::strcmp(argv[1], "listener") == 0) {
    const int expected = argc > 2 ? std::atoi(argv[2]) : 100;
    return run_listener(expected);
  }
  std::fprintf(stderr, "unknown mode %s\n", argv[1]);
  return 64;
}
