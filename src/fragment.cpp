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

#include "mdds/fragment.hpp"

#include <algorithm>
#include <cstring>

namespace mdds
{

std::vector<std::vector<uint8_t>> Fragmenter::fragment(
  const Guid & writer, uint64_t seq, uint64_t pub_time_ms,
  const uint8_t * payload, uint32_t payload_len,
  size_t max_payload)
{
  std::vector<std::vector<uint8_t>> frames;
  if (payload_len > 0 && payload == nullptr) {
    return frames;
  }

  // Fits in a single DATA frame?
  if (kHeaderSize + kDataBodyFixedSize + payload_len <= max_payload) {
    frames.push_back(encode_data(writer, seq, pub_time_ms, payload, payload_len));
    return frames;
  }

  if (max_payload <= kHeaderSize + kDataFragBodyFixedSize) {
    return frames;  // cannot make progress
  }
  const uint32_t frag_capacity =
    static_cast<uint32_t>(max_payload - kHeaderSize - kDataFragBodyFixedSize);

  uint32_t offset = 0;
  while (offset < payload_len) {
    const uint32_t chunk = std::min(frag_capacity, payload_len - offset);
    frames.push_back(
      encode_data_frag(writer, seq, pub_time_ms, payload_len, offset, payload + offset, chunk));
    offset += chunk;
  }
  return frames;
}

bool Reassembler::add_fragment(const DataFragBody & frag, std::vector<uint8_t> & out)
{
  return add_fragment(
    frag.writer, frag.seq, frag.total_size, frag.frag_offset,
    frag.payload, frag.payload_len, out);
}

bool Reassembler::add_fragment(
  const Guid & writer, uint64_t seq, uint32_t total_size,
  uint32_t frag_offset, const uint8_t * data, uint32_t data_len,
  std::vector<uint8_t> & out)
{
  if (total_size == 0 || frag_offset + data_len > total_size) {
    return false;
  }
  if (data_len > 0 && data == nullptr) {
    return false;
  }

  const Key key{writer, seq};
  auto it = partials_.find(key);
  if (it == partials_.end()) {
    Partial p;
    p.total_size = total_size;
    p.buffer.resize(total_size);
    p.last_update = Clock::now();
    it = partials_.emplace(key, std::move(p)).first;
  }

  Partial & p = it->second;
  if (p.total_size != total_size) {
    return false;  // inconsistent; keep existing state
  }

  if (data_len > 0) {
    std::memcpy(p.buffer.data() + frag_offset, data, data_len);
  }

  // Merge [frag_offset, frag_offset + data_len) into the interval list.
  const uint32_t begin = frag_offset;
  const uint32_t end = frag_offset + data_len;
  std::vector<std::pair<uint32_t, uint32_t>> merged;
  merged.reserve(p.intervals.size() + 1);
  uint32_t cur_begin = begin;
  uint32_t cur_end = end;
  for (const auto & iv : p.intervals) {
    if (iv.second < cur_begin) {
      merged.push_back(iv);
    } else if (iv.first > cur_end) {
      merged.emplace_back(cur_begin, cur_end);
      cur_begin = iv.first;
      cur_end = iv.second;
    } else {
      cur_begin = std::min(cur_begin, iv.first);
      cur_end = std::max(cur_end, iv.second);
    }
  }
  merged.emplace_back(cur_begin, cur_end);
  p.intervals = std::move(merged);
  p.last_update = Clock::now();

  if (p.intervals.size() == 1 &&
    p.intervals[0].first == 0 &&
    p.intervals[0].second == total_size)
  {
    out = std::move(p.buffer);
    partials_.erase(it);
    return true;
  }
  return false;
}

size_t Reassembler::sweep_expired(Clock::time_point now)
{
  size_t dropped = 0;
  for (auto it = partials_.begin(); it != partials_.end(); ) {
    if (now - it->second.last_update > timeout_) {
      it = partials_.erase(it);
      ++dropped;
    } else {
      ++it;
    }
  }
  return dropped;
}

void Reassembler::drop_writer(const Guid & writer)
{
  for (auto it = partials_.begin(); it != partials_.end(); ) {
    if (it->first.first == writer) {
      it = partials_.erase(it);
    } else {
      ++it;
    }
  }
}

}  // namespace mdds
