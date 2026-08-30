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

#include "mdds/frame.hpp"

#include <cstring>
#include <random>

namespace mdds
{

std::array<uint8_t, 12> generate_participant_prefix()
{
  std::random_device rd;
  std::mt19937_64 gen(
    (static_cast<uint64_t>(rd()) << 32) ^ static_cast<uint64_t>(rd()));
  std::array<uint8_t, 12> prefix{};
  for (size_t i = 0; i < prefix.size(); i += sizeof(uint64_t)) {
    uint64_t v = gen();
    std::memcpy(prefix.data() + i, &v, sizeof(v));
  }
  return prefix;
}

namespace
{

void put_u16(uint8_t * out, uint16_t v)
{
  out[0] = static_cast<uint8_t>(v >> 8);
  out[1] = static_cast<uint8_t>(v & 0xff);
}

void put_u32(uint8_t * out, uint32_t v)
{
  out[0] = static_cast<uint8_t>(v >> 24);
  out[1] = static_cast<uint8_t>((v >> 16) & 0xff);
  out[2] = static_cast<uint8_t>((v >> 8) & 0xff);
  out[3] = static_cast<uint8_t>(v & 0xff);
}

void put_u64(uint8_t * out, uint64_t v)
{
  put_u32(out, static_cast<uint32_t>(v >> 32));
  put_u32(out + 4, static_cast<uint32_t>(v & 0xffffffffu));
}

uint16_t get_u16(const uint8_t * in)
{
  return static_cast<uint16_t>((static_cast<uint16_t>(in[0]) << 8) | in[1]);
}

uint32_t get_u32(const uint8_t * in)
{
  return (static_cast<uint32_t>(in[0]) << 24) |
         (static_cast<uint32_t>(in[1]) << 16) |
         (static_cast<uint32_t>(in[2]) << 8) |
         static_cast<uint32_t>(in[3]);
}

uint64_t get_u64(const uint8_t * in)
{
  return (static_cast<uint64_t>(get_u32(in)) << 32) | get_u32(in + 4);
}

void put_guid(uint8_t * out, const Guid & g)
{
  std::memcpy(out, g.bytes.data(), g.bytes.size());
}

void get_guid(const uint8_t * in, Guid & g)
{
  std::memcpy(g.bytes.data(), in, g.bytes.size());
}

/// Tolerant TLV trailer parser: fills `out` with the recognized entries,
/// silently skipping unknown types and stopping (without failing) at the
/// first malformed entry. Returns true when all bytes parsed cleanly.
bool parse_trailer(const uint8_t * buf, size_t len, DataTrailer & out)
{
  size_t pos = 0;
  while (pos + 2 <= len) {
    const uint8_t type = buf[pos];
    const uint8_t tlen = buf[pos + 1];
    if (pos + 2 + tlen > len) {
      return false;  // truncated entry; keep what was parsed so far
    }
    const uint8_t * value = buf + pos + 2;
    switch (type) {
      case kTlvOriginGuid:
        if (tlen == 16) {
          get_guid(value, out.origin_guid);
          out.has_origin = true;
        }
        break;
      case kTlvOriginSeq:
        if (tlen == 8) {
          out.origin_seq = get_u64(value);
        }
        break;
      default:
        break;  // unknown entry: skip, stay forward compatible
    }
    pos += 2 + tlen;
  }
  return pos == len;
}

}  // namespace

void encode_header(uint8_t * out, FrameType type, uint16_t flags, uint32_t body_len)
{
  out[0] = 'M';
  out[1] = 'D';
  out[2] = 'D';
  out[3] = 'S';
  out[4] = kProtocolVersion;
  out[5] = static_cast<uint8_t>(type);
  put_u16(out + 6, flags);
  put_u32(out + 8, body_len);
}

bool decode_header(const uint8_t * buf, size_t len, FrameHeader & out)
{
  if (buf == nullptr || len < kHeaderSize) {
    return false;
  }
  if (buf[0] != 'M' || buf[1] != 'D' || buf[2] != 'D' || buf[3] != 'S') {
    return false;
  }
  if (buf[4] != kProtocolVersion) {
    return false;
  }
  out.type = static_cast<FrameType>(buf[5]);
  out.flags = get_u16(buf + 6);
  out.body_len = get_u32(buf + 8);
  if (len - kHeaderSize < out.body_len) {
    return false;
  }
  return true;
}

std::vector<uint8_t> encode_trailer(const DataTrailer & trailer)
{
  std::vector<uint8_t> out;
  if (!trailer.has_origin) {
    return out;
  }
  out.resize(2 + 16 + 2 + 8);
  out[0] = kTlvOriginGuid;
  out[1] = 16;
  put_guid(out.data() + 2, trailer.origin_guid);
  out[18] = kTlvOriginSeq;
  out[19] = 8;
  put_u64(out.data() + 20, trailer.origin_seq);
  return out;
}

std::vector<uint8_t> encode_data(
  const Guid & writer, uint64_t seq, uint64_t pub_time_ms,
  const uint8_t * payload, uint32_t payload_len,
  uint16_t flags, const DataTrailer * trailer)
{
  const std::vector<uint8_t> tail = trailer != nullptr ? encode_trailer(*trailer) :
    std::vector<uint8_t>();
  const uint32_t body_len = kDataBodyFixedSize + payload_len + tail.size();
  std::vector<uint8_t> out(kHeaderSize + body_len);
  encode_header(out.data(), FrameType::DATA, flags, body_len);
  uint8_t * body = out.data() + kHeaderSize;
  put_guid(body, writer);
  put_u64(body + 16, seq);
  put_u64(body + 24, pub_time_ms);
  put_u32(body + 32, payload_len);
  if (payload_len > 0 && payload != nullptr) {
    std::memcpy(body + kDataBodyFixedSize, payload, payload_len);
  }
  if (!tail.empty()) {
    std::memcpy(body + kDataBodyFixedSize + payload_len, tail.data(), tail.size());
  }
  return out;
}

bool decode_data(const uint8_t * buf, size_t len, DataBody & out)
{
  FrameHeader hdr;
  if (!decode_header(buf, len, hdr) || hdr.type != FrameType::DATA) {
    return false;
  }
  if (hdr.body_len < kDataBodyFixedSize) {
    return false;
  }
  const uint8_t * body = buf + kHeaderSize;
  get_guid(body, out.writer);
  out.seq = get_u64(body + 16);
  out.pub_time_ms = get_u64(body + 24);
  out.payload_len = get_u32(body + 32);
  if (hdr.body_len - kDataBodyFixedSize < out.payload_len) {
    return false;
  }
  out.payload = body + kDataBodyFixedSize;
  out.trailer = DataTrailer{};
  const size_t tail_off = kDataBodyFixedSize + out.payload_len;
  if (hdr.body_len > tail_off) {
    parse_trailer(body + tail_off, hdr.body_len - tail_off, out.trailer);
  }
  return true;
}

std::vector<uint8_t> encode_data_frag(
  const Guid & writer, uint64_t seq, uint64_t pub_time_ms,
  uint32_t total_size, uint32_t frag_offset,
  const uint8_t * payload, uint32_t payload_len,
  uint16_t flags, const DataTrailer * trailer)
{
  const std::vector<uint8_t> tail = trailer != nullptr ? encode_trailer(*trailer) :
    std::vector<uint8_t>();
  const uint32_t body_len = kDataFragBodyFixedSize + payload_len + tail.size();
  std::vector<uint8_t> out(kHeaderSize + body_len);
  encode_header(out.data(), FrameType::DATA_FRAG, flags, body_len);
  uint8_t * body = out.data() + kHeaderSize;
  put_guid(body, writer);
  put_u64(body + 16, seq);
  put_u64(body + 24, pub_time_ms);
  put_u32(body + 32, total_size);
  put_u32(body + 36, frag_offset);
  put_u32(body + 40, payload_len);
  if (payload_len > 0 && payload != nullptr) {
    std::memcpy(body + kDataFragBodyFixedSize, payload, payload_len);
  }
  if (!tail.empty()) {
    std::memcpy(body + kDataFragBodyFixedSize + payload_len, tail.data(), tail.size());
  }
  return out;
}

bool decode_data_frag(const uint8_t * buf, size_t len, DataFragBody & out)
{
  FrameHeader hdr;
  if (!decode_header(buf, len, hdr) || hdr.type != FrameType::DATA_FRAG) {
    return false;
  }
  if (hdr.body_len < kDataFragBodyFixedSize) {
    return false;
  }
  const uint8_t * body = buf + kHeaderSize;
  get_guid(body, out.writer);
  out.seq = get_u64(body + 16);
  out.pub_time_ms = get_u64(body + 24);
  out.total_size = get_u32(body + 32);
  out.frag_offset = get_u32(body + 36);
  out.payload_len = get_u32(body + 40);
  if (hdr.body_len - kDataFragBodyFixedSize < out.payload_len) {
    return false;
  }
  out.payload = body + kDataFragBodyFixedSize;
  out.trailer = DataTrailer{};
  const size_t tail_off = kDataFragBodyFixedSize + out.payload_len;
  if (hdr.body_len > tail_off) {
    parse_trailer(body + tail_off, hdr.body_len - tail_off, out.trailer);
  }
  if (static_cast<uint64_t>(out.frag_offset) + out.payload_len > out.total_size) {
    return false;
  }
  return true;
}

std::vector<uint8_t> encode_acknack(
  const Guid & reader, const Guid & writer, uint64_t base_seq, uint64_t bitmap)
{
  std::vector<uint8_t> out(kHeaderSize + kAckNackBodySize);
  encode_header(out.data(), FrameType::ACKNACK, 0, kAckNackBodySize);
  uint8_t * body = out.data() + kHeaderSize;
  put_guid(body, reader);
  put_guid(body + 16, writer);
  put_u64(body + 32, base_seq);
  put_u64(body + 40, bitmap);
  return out;
}

bool decode_acknack(const uint8_t * buf, size_t len, AckNackBody & out)
{
  FrameHeader hdr;
  if (!decode_header(buf, len, hdr) || hdr.type != FrameType::ACKNACK) {
    return false;
  }
  if (hdr.body_len < kAckNackBodySize) {
    return false;
  }
  const uint8_t * body = buf + kHeaderSize;
  get_guid(body, out.reader);
  get_guid(body + 16, out.writer);
  out.base_seq = get_u64(body + 32);
  out.bitmap = get_u64(body + 40);
  return true;
}

std::vector<uint8_t> encode_heartbeat(
  const Guid & writer, uint64_t first_seq, uint64_t last_seq)
{
  std::vector<uint8_t> out(kHeaderSize + kHeartbeatBodySize);
  encode_header(out.data(), FrameType::HEARTBEAT, 0, kHeartbeatBodySize);
  uint8_t * body = out.data() + kHeaderSize;
  put_guid(body, writer);
  put_u64(body + 16, first_seq);
  put_u64(body + 24, last_seq);
  return out;
}

bool decode_heartbeat(const uint8_t * buf, size_t len, HeartbeatBody & out)
{
  FrameHeader hdr;
  if (!decode_header(buf, len, hdr) || hdr.type != FrameType::HEARTBEAT) {
    return false;
  }
  if (hdr.body_len < kHeartbeatBodySize) {
    return false;
  }
  const uint8_t * body = buf + kHeaderSize;
  get_guid(body, out.writer);
  out.first_seq = get_u64(body + 16);
  out.last_seq = get_u64(body + 24);
  return true;
}

std::vector<uint8_t> encode_gap(
  const Guid & writer, const Guid & reader, uint64_t gap_start, uint64_t gap_end)
{
  std::vector<uint8_t> out(kHeaderSize + kGapBodySize);
  encode_header(out.data(), FrameType::GAP, 0, kGapBodySize);
  uint8_t * body = out.data() + kHeaderSize;
  put_guid(body, writer);
  put_guid(body + 16, reader);
  put_u64(body + 32, gap_start);
  put_u64(body + 40, gap_end);
  return out;
}

bool decode_gap(const uint8_t * buf, size_t len, GapBody & out)
{
  FrameHeader hdr;
  if (!decode_header(buf, len, hdr) || hdr.type != FrameType::GAP) {
    return false;
  }
  if (hdr.body_len < kGapBodySize) {
    return false;
  }
  const uint8_t * body = buf + kHeaderSize;
  get_guid(body, out.writer);
  get_guid(body + 16, out.reader);
  out.gap_start = get_u64(body + 32);
  out.gap_end = get_u64(body + 40);
  return true;
}

}  // namespace mdds
