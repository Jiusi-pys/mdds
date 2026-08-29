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

#ifndef MDDS__DISCOVERY_HPP_
#define MDDS__DISCOVERY_HPP_

#include <cstdint>
#include <string>
#include <vector>

#include "mdds/guid.hpp"
#include "mdds/qos.hpp"

namespace mdds
{

struct NodeInfo
{
  std::string ns;
  std::string name;

  bool operator==(const NodeInfo & o) const {return ns == o.ns && name == o.name;}
};

struct EndpointRecord
{
  Guid guid;
  EntityKind kind;  // WRITER or READER
  std::string topic;
  std::string type_name;
  QosProfile qos;

  bool operator==(const EndpointRecord & o) const
  {
    return guid == o.guid && kind == o.kind && topic == o.topic &&
           type_name == o.type_name;
  }
};

/// Full local state, broadcast verbatim in every ANNOUNCE frame. v1 always
/// sends the complete snapshot; receivers keep the snapshot with the highest
/// announce_seq per participant (idempotent, dedup-friendly for gateway
/// topologies).
struct ParticipantSnapshot
{
  Guid participant_guid;
  std::vector<NodeInfo> nodes;
  std::vector<EndpointRecord> endpoints;
};

/// Encode an ANNOUNCE frame (header + body).
std::vector<uint8_t> encode_announce(const ParticipantSnapshot & snap, uint64_t announce_seq);

/// Decode an ANNOUNCE frame (buf starts at the frame header).
bool decode_announce(
  const uint8_t * buf, size_t len, ParticipantSnapshot & snap, uint64_t & announce_seq);

}  // namespace mdds

#endif  // MDDS__DISCOVERY_HPP_
