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

#include "mdds/discovery.hpp"

#include <cstring>

#include "mdds/frame.hpp"

namespace mdds
{

namespace
{

void put_u16(std::vector<uint8_t> & out, uint16_t v)
{
  out.push_back(static_cast<uint8_t>(v >> 8));
  out.push_back(static_cast<uint8_t>(v & 0xff));
}

void put_u64(std::vector<uint8_t> & out, uint64_t v)
{
  for (int i = 7; i >= 0; --i) {
    out.push_back(static_cast<uint8_t>((v >> (i * 8)) & 0xff));
  }
}

void put_string(std::vector<uint8_t> & out, const std::string & s)
{
  put_u16(out, static_cast<uint16_t>(s.size()));
  out.insert(out.end(), s.begin(), s.end());
}

class Reader2
{
public:
  Reader2(const uint8_t * buf, size_t len) : buf_(buf), len_(len) {}

  bool u16(uint16_t & v)
  {
    if (pos_ + 2 > len_) {return false;}
    v = static_cast<uint16_t>((buf_[pos_] << 8) | buf_[pos_ + 1]);
    pos_ += 2;
    return true;
  }

  bool u64(uint64_t & v)
  {
    if (pos_ + 8 > len_) {return false;}
    v = 0;
    for (int i = 0; i < 8; ++i) {
      v = (v << 8) | buf_[pos_++];
    }
    return true;
  }

  bool bytes(void * out, size_t n)
  {
    if (pos_ + n > len_) {return false;}
    std::memcpy(out, buf_ + pos_, n);
    pos_ += n;
    return true;
  }

  bool str(std::string & s)
  {
    uint16_t n;
    if (!u16(n)) {return false;}
    if (pos_ + n > len_) {return false;}
    s.assign(reinterpret_cast<const char *>(buf_ + pos_), n);
    pos_ += n;
    return true;
  }

private:
  const uint8_t * buf_;
  size_t len_;
  size_t pos_ = 0;
};

}  // namespace

std::vector<uint8_t> encode_announce(const ParticipantSnapshot & snap, uint64_t announce_seq)
{
  std::vector<uint8_t> body;
  body.insert(body.end(), snap.participant_guid.bytes.begin(), snap.participant_guid.bytes.end());
  put_u64(body, announce_seq);

  put_u16(body, static_cast<uint16_t>(snap.nodes.size()));
  for (const auto & n : snap.nodes) {
    put_string(body, n.ns);
    put_string(body, n.name);
  }

  put_u16(body, static_cast<uint16_t>(snap.endpoints.size()));
  for (const auto & e : snap.endpoints) {
    body.insert(body.end(), e.guid.bytes.begin(), e.guid.bytes.end());
    body.push_back(static_cast<uint8_t>(e.kind));
    put_string(body, e.topic);
    put_string(body, e.type_name);
    uint8_t qos_buf[kQosWireSize];
    encode_qos(qos_buf, e.qos);
    body.insert(body.end(), qos_buf, qos_buf + kQosWireSize);
    put_u64(body, e.birth_us);
  }

  std::vector<uint8_t> frame(kHeaderSize + body.size());
  encode_header(frame.data(), FrameType::ANNOUNCE, 0, static_cast<uint32_t>(body.size()));
  std::memcpy(frame.data() + kHeaderSize, body.data(), body.size());
  return frame;
}

bool decode_announce(
  const uint8_t * buf, size_t len, ParticipantSnapshot & snap, uint64_t & announce_seq)
{
  FrameHeader hdr;
  if (!decode_header(buf, len, hdr) || hdr.type != FrameType::ANNOUNCE) {
    return false;
  }
  Reader2 r(buf + kHeaderSize, hdr.body_len);

  if (!r.bytes(snap.participant_guid.bytes.data(), snap.participant_guid.bytes.size())) {
    return false;
  }
  if (!r.u64(announce_seq)) {
    return false;
  }

  uint16_t node_count;
  if (!r.u16(node_count)) {
    return false;
  }
  snap.nodes.clear();
  snap.nodes.reserve(node_count);
  for (uint16_t i = 0; i < node_count; ++i) {
    NodeInfo n;
    if (!r.str(n.ns) || !r.str(n.name)) {
      return false;
    }
    snap.nodes.push_back(std::move(n));
  }

  uint16_t ep_count;
  if (!r.u16(ep_count)) {
    return false;
  }
  snap.endpoints.clear();
  snap.endpoints.reserve(ep_count);
  for (uint16_t i = 0; i < ep_count; ++i) {
    EndpointRecord e;
    if (!r.bytes(e.guid.bytes.data(), e.guid.bytes.size())) {
      return false;
    }
    uint8_t kind;
    if (!r.bytes(&kind, 1)) {
      return false;
    }
    e.kind = static_cast<EntityKind>(kind);
    if (!r.str(e.topic) || !r.str(e.type_name)) {
      return false;
    }
    uint8_t qos_buf[kQosWireSize];
    if (!r.bytes(qos_buf, sizeof(qos_buf))) {
      return false;
    }
    if (!decode_qos(qos_buf, sizeof(qos_buf), e.qos)) {
      return false;
    }
    if (!r.u64(e.birth_us)) {
      return false;
    }
    snap.endpoints.push_back(std::move(e));
  }
  return true;
}

}  // namespace mdds
