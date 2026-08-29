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

#ifndef MDDS__FRAME_HPP_
#define MDDS__FRAME_HPP_

#include <cstddef>
#include <cstdint>
#include <vector>

#include "mdds/guid.hpp"

namespace mdds
{

/// Frame format v3 (see docs/design.md section 4). All multi-byte fields are
/// big-endian. One mdds frame maps to exactly one Transport::send() call.
/// v3 changes vs v2 (v3 does not interoperate with v2, same stance as v1->v2):
///  - DATA / DATA_FRAG bodies carry an explicit payload_len (u32), so the body
///    may be followed by an optional TLV trailer; decoders skip unknown TLV
///    entry types, which lets the protocol evolve without a version bump.
///  - New frame type GAP: a reliable writer tells readers to give up on a
///    sequence range it has already evicted from its history cache.
///  - flags bit0 (kFlagReliable) marks DATA / DATA_FRAG from RELIABLE writers
///    (advisory; receiver-side QoS still comes from discovery).
///
/// Common header (12 bytes):
///   magic[4] "MDDS" | version:u8 | frame_type:u8 | flags:u16 | body_len:u32

enum class FrameType : uint8_t
{
  DATA = 1,
  DATA_FRAG = 2,
  ACKNACK = 3,
  HEARTBEAT = 4,
  ANNOUNCE = 5,
  GAP = 6,
};

constexpr uint8_t kProtocolVersion = 3;
constexpr uint16_t kFlagReliable = 0x0001;

constexpr size_t kHeaderSize = 12;
constexpr size_t kDataBodyFixedSize = 36;      // writer_guid(16) + seq(8) + pub_time_ms(8) + payload_len(4)
constexpr size_t kDataFragBodyFixedSize = 44;  // + total_size(4) + frag_offset(4)
constexpr size_t kAckNackBodySize = 48;        // reader(16) + writer(16) + base(8) + bitmap(8)
constexpr size_t kHeartbeatBodySize = 32;      // writer(16) + first_seq(8) + last_seq(8)
constexpr size_t kGapBodySize = 32;            // writer(16) + gap_start(8) + gap_end(8)

/// TLV trailer entry types (DATA / DATA_FRAG body tail).
/// Entry encoding: type:u8 | len:u8 | value[len].
constexpr uint8_t kTlvOriginGuid = 1;  // 16 bytes: originating writer's GUID
constexpr uint8_t kTlvOriginSeq = 2;   // 8 bytes: sequence at the originating writer

/// Parsed form of the optional TLV trailer. All fields absent means
/// has_origin == false. Reserved for bridge/relay identity forwarding.
struct DataTrailer
{
  bool has_origin = false;
  Guid origin_guid{};
  uint64_t origin_seq = 0;
};

struct FrameHeader
{
  FrameType type;
  uint16_t flags;
  uint32_t body_len;
};

struct DataBody
{
  Guid writer;
  uint64_t seq;
  uint64_t pub_time_ms;  // system_clock ms at write time (lifespan bookkeeping)
  const uint8_t * payload;  // points into the decoded buffer
  uint32_t payload_len;
  DataTrailer trailer;
};

struct DataFragBody
{
  Guid writer;
  uint64_t seq;
  uint64_t pub_time_ms;  // identical on every fragment of one sample
  uint32_t total_size;
  uint32_t frag_offset;
  const uint8_t * payload;  // points into the decoded buffer
  uint32_t payload_len;
  DataTrailer trailer;
};

struct AckNackBody
{
  Guid reader;
  Guid writer;
  uint64_t base_seq;  // bitmap bit i covers seq = base_seq + i
  uint64_t bitmap;    // bit set = that seq was received (precise NACK)
};

struct HeartbeatBody
{
  Guid writer;
  uint64_t first_seq;
  uint64_t last_seq;
};

struct GapBody
{
  Guid writer;
  uint64_t gap_start;  // inclusive
  uint64_t gap_end;    // inclusive
};

/// Encode a 12-byte frame header. `out` must hold at least kHeaderSize bytes.
void encode_header(uint8_t * out, FrameType type, uint16_t flags, uint32_t body_len);

/// Decode a frame header. Returns false if the buffer is shorter than
/// kHeaderSize, the magic/version do not match, or body_len exceeds the
/// remaining bytes in the buffer.
bool decode_header(const uint8_t * buf, size_t len, FrameHeader & out);

/// Encode the TLV trailer for `trailer`. Empty vector when nothing is set.
std::vector<uint8_t> encode_trailer(const DataTrailer & trailer);

/// Encode a complete DATA frame (header + body + optional TLV trailer).
std::vector<uint8_t> encode_data(
  const Guid & writer, uint64_t seq, uint64_t pub_time_ms,
  const uint8_t * payload, uint32_t payload_len,
  uint16_t flags = 0, const DataTrailer * trailer = nullptr);

/// Decode a DATA frame body (buf must start at the frame header). A malformed
/// TLV trailer does not fail the decode; the payload is still delivered.
bool decode_data(const uint8_t * buf, size_t len, DataBody & out);

/// Encode a complete DATA_FRAG frame carrying payload[0..payload_len) as the
/// fragment at `frag_offset` of a reassembled message of `total_size` bytes.
std::vector<uint8_t> encode_data_frag(
  const Guid & writer, uint64_t seq, uint64_t pub_time_ms,
  uint32_t total_size, uint32_t frag_offset,
  const uint8_t * payload, uint32_t payload_len,
  uint16_t flags = 0, const DataTrailer * trailer = nullptr);

/// Decode a DATA_FRAG frame body (buf must start at the frame header).
bool decode_data_frag(const uint8_t * buf, size_t len, DataFragBody & out);

std::vector<uint8_t> encode_acknack(
  const Guid & reader, const Guid & writer, uint64_t base_seq, uint64_t bitmap);
bool decode_acknack(const uint8_t * buf, size_t len, AckNackBody & out);

std::vector<uint8_t> encode_heartbeat(
  const Guid & writer, uint64_t first_seq, uint64_t last_seq);
bool decode_heartbeat(const uint8_t * buf, size_t len, HeartbeatBody & out);

/// GAP: the writer has evicted [gap_start, gap_end] (inclusive) and will
/// never retransmit them; readers advance their baseline past the range.
std::vector<uint8_t> encode_gap(
  const Guid & writer, uint64_t gap_start, uint64_t gap_end);
bool decode_gap(const uint8_t * buf, size_t len, GapBody & out);

}  // namespace mdds

#endif  // MDDS__FRAME_HPP_
