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

#ifndef MDDS__FRAGMENT_HPP_
#define MDDS__FRAGMENT_HPP_

#include <chrono>
#include <cstdint>
#include <map>
#include <utility>
#include <vector>

#include "mdds/frame.hpp"
#include "mdds/guid.hpp"

namespace mdds
{

/// Splits a serialized sample into frames that each fit within max_payload
/// bytes on the wire. Small samples produce a single DATA frame; larger ones
/// produce DATA_FRAG frames.
class Fragmenter
{
public:
  /// Returns the wire frames for one sample. Empty if the input is invalid
  /// (payload null while payload_len > 0, or max_payload too small to make
  /// progress). `pub_time_ms` (system_clock ms at write time) is stamped on
  /// every produced frame for lifespan bookkeeping; `flags` (e.g.
  /// kFlagReliable) is stamped on every produced frame header.
  static std::vector<std::vector<uint8_t>> fragment(
    const Guid & writer, uint64_t seq, uint64_t pub_time_ms,
    const uint8_t * payload, uint32_t payload_len,
    size_t max_payload, uint16_t flags = 0);
};

/// Reassembles DATA_FRAG sequences back into complete samples. Coverage is
/// tracked with exact merged byte intervals, so arbitrarily overlapping or
/// misaligned fragments are handled correctly.
/// Thread-safety: not internally synchronized; callers must serialize access.
class Reassembler
{
public:
  using Clock = std::chrono::steady_clock;

  explicit Reassembler(
    std::chrono::milliseconds timeout = std::chrono::milliseconds(5000))
  : timeout_(timeout) {}

  /// Feed one decoded fragment. When the message completes, returns true and
  /// fills `out` with the full payload; the partial state is dropped.
  /// Returns false while the message is still incomplete or the fragment is
  /// invalid (inconsistent total_size, out of range).
  bool add_fragment(const DataFragBody & frag, std::vector<uint8_t> & out);

  bool add_fragment(
    const Guid & writer, uint64_t seq, uint32_t total_size,
    uint32_t frag_offset, const uint8_t * data, uint32_t data_len,
    std::vector<uint8_t> & out);

  /// Drop partial messages older than the timeout. Returns the number dropped.
  size_t sweep_expired(Clock::time_point now = Clock::now());

  /// Drop all partial state for a writer (peer disconnect).
  void drop_writer(const Guid & writer);

  size_t pending_count() const {return partials_.size();}

private:
  struct Partial
  {
    uint32_t total_size = 0;
    std::vector<uint8_t> buffer;
    // Merged, sorted, non-overlapping [begin, end) byte intervals received.
    std::vector<std::pair<uint32_t, uint32_t>> intervals;
    Clock::time_point last_update;
  };

  using Key = std::pair<Guid, uint64_t>;
  std::map<Key, Partial> partials_;
  std::chrono::milliseconds timeout_;
};

}  // namespace mdds

#endif  // MDDS__FRAGMENT_HPP_
