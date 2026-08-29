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
#include <cstdint>
#include <mutex>
#include <thread>
#include <vector>

#include "gtest/gtest.h"

#include "../src/send_lane.hpp"

namespace
{

using namespace std::chrono_literals;

/// Sink that records every frame and can be gated to control worker progress.
/// close() parks the worker inside deliver(); open() lets frames through;
/// cancel() releases a parked call with a failure result (channel died).
class MockSink
{
public:
  bool deliver(const uint8_t * data, size_t len)
  {
    std::unique_lock<std::mutex> lock(mutex_);
    ++entered_;
    cv_.notify_all();
    cv_.wait(lock, [this] {return open_ || canceled_;});
    if (canceled_) {
      return false;
    }
    frames_.emplace_back(data, data + len);
    cv_.notify_all();
    return result_;
  }

  void close()
  {
    std::lock_guard<std::mutex> lock(mutex_);
    open_ = false;
  }

  void open()
  {
    std::lock_guard<std::mutex> lock(mutex_);
    open_ = true;
    cv_.notify_all();
  }

  void cancel()
  {
    std::lock_guard<std::mutex> lock(mutex_);
    canceled_ = true;
    cv_.notify_all();
  }

  void set_result(bool ok)
  {
    std::lock_guard<std::mutex> lock(mutex_);
    result_ = ok;
  }

  bool wait_frames(size_t count, std::chrono::milliseconds timeout = 5s)
  {
    std::unique_lock<std::mutex> lock(mutex_);
    return cv_.wait_for(lock, timeout, [&] {return frames_.size() >= count;});
  }

  bool wait_entered(size_t count, std::chrono::milliseconds timeout = 5s)
  {
    std::unique_lock<std::mutex> lock(mutex_);
    return cv_.wait_for(lock, timeout, [&] {return entered_ >= count;});
  }

  std::vector<std::vector<uint8_t>> frames()
  {
    std::lock_guard<std::mutex> lock(mutex_);
    return frames_;
  }

private:
  std::mutex mutex_;
  std::condition_variable cv_;
  std::vector<std::vector<uint8_t>> frames_;
  size_t entered_ = 0;
  bool open_ = true;
  bool canceled_ = false;
  bool result_ = true;
};

std::vector<uint8_t> payload(uint8_t seed, size_t len)
{
  std::vector<uint8_t> out(len);
  for (size_t i = 0; i < len; ++i) {
    out[i] = static_cast<uint8_t>(seed + i);
  }
  return out;
}

TEST(SendLane, PreservesOrder)
{
  mdds::SendBudget budget(1 << 20);
  MockSink sink;
  {
    mdds::SendLane lane(
      [&sink](const uint8_t * d, size_t l) {return sink.deliver(d, l);}, 1 << 16, budget);
    for (uint8_t i = 0; i < 32; ++i) {
      ASSERT_TRUE(lane.push(payload(i, 64).data(), 64));
    }
    ASSERT_TRUE(sink.wait_frames(32));
    lane.stop(true);
  }
  const auto frames = sink.frames();
  ASSERT_EQ(frames.size(), 32u);
  for (uint8_t i = 0; i < 32; ++i) {
    EXPECT_EQ(frames[i], payload(i, 64)) << "frame " << int(i) << " out of order";
  }
  EXPECT_EQ(budget.used.load(), 0u);
}

TEST(SendLane, FullLaneDropsNewAndCounts)
{
  mdds::SendBudget budget(1 << 20);
  MockSink sink;
  sink.close();  // park the worker on the first frame
  mdds::SendLane lane(
    [&sink](const uint8_t * d, size_t l) {return sink.deliver(d, l);}, 256, budget);

  ASSERT_TRUE(lane.push(payload(0, 128).data(), 128));
  ASSERT_TRUE(sink.wait_entered(1));  // worker now holds frame 0 in flight
  // In-flight frames do not count against the budgets; only queued ones do.
  ASSERT_TRUE(lane.push(payload(1, 128).data(), 128));  // queued: 128/256
  ASSERT_TRUE(lane.push(payload(2, 128).data(), 128));  // queued: 256/256
  EXPECT_FALSE(lane.push(payload(3, 64).data(), 64));   // over budget: drop new
  EXPECT_EQ(lane.dropped(), 1u);

  sink.open();
  ASSERT_TRUE(sink.wait_frames(3));
  lane.stop(true);
  EXPECT_EQ(lane.dropped(), 1u);  // no further drops while draining
  EXPECT_EQ(budget.used.load(), 0u);
}

TEST(SendLane, GlobalBudgetSharedAcrossLanes)
{
  mdds::SendBudget budget(256);  // both lanes together may queue 256 bytes
  MockSink sink_a;
  MockSink sink_b;
  sink_a.close();
  mdds::SendLane lane_a(
    [&sink_a](const uint8_t * d, size_t l) {return sink_a.deliver(d, l);}, 1 << 16, budget);
  mdds::SendLane lane_b(
    [&sink_b](const uint8_t * d, size_t l) {return sink_b.deliver(d, l);}, 1 << 16, budget);

  ASSERT_TRUE(lane_a.push(payload(0, 128).data(), 128));
  ASSERT_TRUE(sink_a.wait_entered(1));  // frame 0 in flight, not counted
  ASSERT_TRUE(lane_a.push(payload(1, 128).data(), 128));  // global used: 128
  EXPECT_FALSE(lane_b.push(payload(2, 129).data(), 129));  // 128+129 > 256: drop
  EXPECT_TRUE(lane_b.push(payload(2, 128).data(), 128));   // exactly fills: ok
  EXPECT_EQ(lane_b.dropped(), 1u);

  sink_a.open();
  ASSERT_TRUE(sink_a.wait_frames(2));
  ASSERT_TRUE(sink_b.wait_frames(1));
  lane_a.stop(true);
  lane_b.stop(true);
  EXPECT_EQ(budget.used.load(), 0u);
}

TEST(SendLane, StopDrainDeliversQueue)
{
  mdds::SendBudget budget(1 << 20);
  MockSink sink;
  sink.close();
  mdds::SendLane lane(
    [&sink](const uint8_t * d, size_t l) {return sink.deliver(d, l);}, 1 << 16, budget);
  for (uint8_t i = 0; i < 8; ++i) {
    ASSERT_TRUE(lane.push(payload(i, 32).data(), 32));
  }
  ASSERT_TRUE(sink.wait_entered(1));  // frame 0 in flight, 1..7 queued
  sink.open();
  lane.stop(true);  // drains the remaining 7, then exits
  EXPECT_EQ(sink.frames().size(), 8u);
  EXPECT_FALSE(lane.push(payload(9, 32).data(), 32));  // stopped: rejects
  EXPECT_EQ(lane.dropped(), 1u);
  EXPECT_EQ(budget.used.load(), 0u);
}

TEST(SendLane, StopDiscardReleasesBudgets)
{
  mdds::SendBudget budget(1 << 20);
  MockSink sink;
  sink.close();
  mdds::SendLane lane(
    [&sink](const uint8_t * d, size_t l) {return sink.deliver(d, l);}, 1 << 16, budget);
  ASSERT_TRUE(lane.push(payload(0, 64).data(), 64));
  ASSERT_TRUE(sink.wait_entered(1));  // worker parked in the sink on frame 0
  ASSERT_TRUE(lane.push(payload(1, 64).data(), 64));
  ASSERT_TRUE(lane.push(payload(2, 64).data(), 64));
  EXPECT_EQ(lane.queued_bytes(), 128u);

  // stop(false) joins the worker, which is stuck in the sink — so stop on a
  // helper thread, then cancel the sink (what a real channel death does).
  std::thread stopper([&lane] {lane.stop(false);});
  for (size_t i = 0; i < 5000 && !lane.stopping(); ++i) {
    std::this_thread::sleep_for(1ms);
  }
  ASSERT_TRUE(lane.stopping());
  sink.cancel();
  stopper.join();

  EXPECT_TRUE(sink.frames().empty());       // frame 0 died with the channel
  EXPECT_EQ(lane.failed(), 1u);
  EXPECT_EQ(lane.dropped(), 0u);            // discards are not push rejections
  EXPECT_EQ(lane.queued_bytes(), 0u);
  EXPECT_EQ(budget.used.load(), 0u);        // f1+f2 returned to the budgets
}

TEST(SendLane, SinkFailureCounts)
{
  mdds::SendBudget budget(1 << 20);
  MockSink sink;
  sink.set_result(false);
  mdds::SendLane lane(
    [&sink](const uint8_t * d, size_t l) {return sink.deliver(d, l);}, 1 << 16, budget);
  ASSERT_TRUE(lane.push(payload(0, 32).data(), 32));
  ASSERT_TRUE(sink.wait_frames(1));
  lane.stop(true);
  EXPECT_EQ(lane.failed(), 1u);
  EXPECT_EQ(lane.dropped(), 0u);
  EXPECT_EQ(budget.used.load(), 0u);
}

}  // namespace
