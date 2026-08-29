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

// Per-peer send lane: a bounded queue drained by one worker thread.
//
// Rationale (mirrors the Kaihong MDDS send-lane design): callers of
// Transport::send() must never block on network IO, and must never run IO on
// a DSoftBus callback thread. push() only copies the frame into the queue;
// the worker performs the actual sink call (SendBytes) later, off the caller
// thread. Backpressure is carried by two byte budgets — one per lane, one
// shared across all lanes of a transport — so a slow or dead peer cannot grow
// memory without bound. When a budget is exceeded the NEW frame is dropped
// and counted; reliable writers heal the hole via ACKNACK/GAP exactly like a
// network drop, best-effort writers simply lose it.
//
// Header-only under src/ so unit tests exercise it without exporting it.

#ifndef MDDS__SEND_LANE_HPP_
#define MDDS__SEND_LANE_HPP_

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <mutex>
#include <thread>
#include <vector>

namespace mdds
{

/// Byte budget shared by every lane of one transport instance.
struct SendBudget
{
  explicit SendBudget(size_t limit)
  : limit(limit) {}

  const size_t limit;
  std::atomic<size_t> used{0};
};

class SendLane
{
public:
  /// Delivers one queued frame; returns false when the frame could not be
  /// sent (dead channel, IO error). Called only from the worker thread.
  using Sink = std::function<bool(const uint8_t * data, size_t len)>;

  SendLane(Sink sink, size_t byte_limit, SendBudget & global)
  : sink_(std::move(sink)), byte_limit_(byte_limit), global_(global),
    worker_([this] {run();})
  {
  }

  ~SendLane() {stop(false);}

  SendLane(const SendLane &) = delete;
  SendLane & operator=(const SendLane &) = delete;

  /// Enqueue a copy of [data, data+len). Returns false — frame dropped — when
  /// the lane is stopping or a byte budget is exceeded.
  bool push(const uint8_t * data, size_t len)
  {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (stopping_ ||
        bytes_ + len > byte_limit_ ||
        global_.used.load() + len > global_.limit)
      {
        ++dropped_;
        return false;
      }
      queue_.emplace_back(data, data + len);
      bytes_ += len;
      global_.used.fetch_add(len);
    }
    cv_.notify_one();
    return true;
  }

  /// Stop the worker. drain=true delivers every queued frame before exiting;
  /// drain=false discards the queue (peer is gone). Idempotent.
  void stop(bool drain)
  {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      stopping_ = true;
      drain_ = drain;
    }
    cv_.notify_all();
    if (worker_.joinable()) {
      worker_.join();
    }
  }

  /// Frames rejected by push() (budget or stopping).
  size_t dropped() const {return dropped_.load();}
  /// Frames the sink reported as failed.
  size_t failed() const {return failed_.load();}
  size_t queued_bytes() const
  {
    std::lock_guard<std::mutex> lock(mutex_);
    return bytes_;
  }
  /// Test seam: true once stop() has latched (worker may still be in-flight).
  bool stopping() const
  {
    std::lock_guard<std::mutex> lock(mutex_);
    return stopping_;
  }

private:
  void release(size_t len)
  {
    bytes_ -= len;  // mutex_ held by caller
    global_.used.fetch_sub(len);
  }

  void run()
  {
    for (;; ) {
      std::vector<uint8_t> frame;
      {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait(lock, [this] {return stopping_ || !queue_.empty();});
        if (queue_.empty()) {
          return;  // stopping and nothing left (or asked not to drain)
        }
        if (stopping_ && !drain_) {
          while (!queue_.empty()) {
            release(queue_.front().size());
            queue_.pop_front();
          }
          return;
        }
        frame = std::move(queue_.front());
        queue_.pop_front();
        release(frame.size());
      }
      if (!sink_(frame.data(), frame.size())) {
        ++failed_;
      }
    }
  }

  Sink sink_;
  const size_t byte_limit_;
  SendBudget & global_;

  mutable std::mutex mutex_;
  std::condition_variable cv_;
  std::deque<std::vector<uint8_t>> queue_;
  size_t bytes_ = 0;
  bool stopping_ = false;
  bool drain_ = false;

  std::atomic<size_t> dropped_{0};
  std::atomic<size_t> failed_{0};
  std::thread worker_;
};

}  // namespace mdds

#endif  // MDDS__SEND_LANE_HPP_
