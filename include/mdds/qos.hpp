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

#ifndef MDDS__QOS_HPP_
#define MDDS__QOS_HPP_

#include <cstddef>
#include <cstdint>

namespace mdds
{

enum class Reliability : uint8_t
{
  BEST_EFFORT = 0,
  RELIABLE = 1,
};

enum class Durability : uint8_t
{
  VOLATILE = 0,
  TRANSIENT_LOCAL = 1,
};

enum class History : uint8_t
{
  KEEP_LAST = 0,
  KEEP_ALL = 1,
};

enum class Liveliness : uint8_t
{
  AUTOMATIC = 0,
  MANUAL_BY_TOPIC = 1,
};

/// QoS carried in ANNOUNCE endpoint entries (32-byte fixed wire encoding,
/// see docs/design.md section 4). Phase 1 consumes reliability/history/depth;
/// the remaining fields are reserved for phase 2.
struct QosProfile
{
  Reliability reliability = Reliability::RELIABLE;
  Durability durability = Durability::VOLATILE;
  History history = History::KEEP_LAST;
  Liveliness liveliness = Liveliness::AUTOMATIC;
  uint32_t depth = 10;
  uint64_t deadline_ms = 0;   // 0 = infinite
  uint64_t lifespan_ms = 0;   // 0 = infinite
  uint64_t liveliness_lease_ms = 0;  // 0 = infinite
};

constexpr size_t kQosWireSize = 32;

/// Encode into out[0..kQosWireSize). All fields big-endian.
void encode_qos(uint8_t * out, const QosProfile & qos);
bool decode_qos(const uint8_t * in, size_t len, QosProfile & qos);

/// DDS requested/offered compatibility: returns true if a reader with
/// `requested` can match a writer with `offered`.
bool qos_compatible(const QosProfile & requested, const QosProfile & offered);

/// Reader-side acceptance: like qos_compatible but lenient on reliability,
/// so a reliable reader still counts and receives from a best-effort writer
/// (at best-effort quality). Durability RxO stays strict.
bool qos_accepted_by_reader(const QosProfile & reader_qos, const QosProfile & writer_qos);

}  // namespace mdds

#endif  // MDDS__QOS_HPP_
