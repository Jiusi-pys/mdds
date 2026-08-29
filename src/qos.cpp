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

#include "mdds/qos.hpp"

namespace mdds
{

namespace
{

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

}  // namespace

QosProfile QosProfile::preset_default()
{
  return QosProfile{};
}

QosProfile QosProfile::preset_best_effort()
{
  QosProfile q;
  q.reliability = Reliability::BEST_EFFORT;
  q.depth = 1;
  return q;
}

QosProfile QosProfile::preset_sensor_data()
{
  QosProfile q;
  q.reliability = Reliability::BEST_EFFORT;
  q.depth = 5;
  q.deadline_ms = 100;
  return q;
}

QosProfile QosProfile::preset_transient_local()
{
  QosProfile q;
  q.durability = Durability::TRANSIENT_LOCAL;
  q.reliability = Reliability::RELIABLE;
  return q;
}

QosProfile QosProfile::preset_bulk_data()
{
  QosProfile q;
  q.history = History::KEEP_ALL;
  q.reliability = Reliability::RELIABLE;
  return q;
}

bool qos_valid(const QosProfile & qos)
{
  if (static_cast<uint8_t>(qos.reliability) > static_cast<uint8_t>(Reliability::RELIABLE) ||
    static_cast<uint8_t>(qos.durability) > static_cast<uint8_t>(Durability::TRANSIENT_LOCAL) ||
    static_cast<uint8_t>(qos.history) > static_cast<uint8_t>(History::KEEP_ALL) ||
    static_cast<uint8_t>(qos.liveliness) > static_cast<uint8_t>(Liveliness::MANUAL_BY_TOPIC))
  {
    return false;
  }
  // KEEP_LAST with depth 0 keeps nothing.
  if (qos.history == History::KEEP_LAST && qos.depth == 0) {
    return false;
  }
  return true;
}

void encode_qos(uint8_t * out, const QosProfile & qos)
{
  out[0] = static_cast<uint8_t>(qos.reliability);
  out[1] = static_cast<uint8_t>(qos.durability);
  out[2] = static_cast<uint8_t>(qos.history);
  out[3] = static_cast<uint8_t>(qos.liveliness);
  put_u32(out + 4, qos.depth);
  put_u64(out + 8, qos.deadline_ms);
  put_u64(out + 16, qos.lifespan_ms);
  put_u64(out + 24, qos.liveliness_lease_ms);
}

bool decode_qos(const uint8_t * in, size_t len, QosProfile & qos)
{
  if (in == nullptr || len < kQosWireSize) {
    return false;
  }
  qos.reliability = static_cast<Reliability>(in[0]);
  qos.durability = static_cast<Durability>(in[1]);
  qos.history = static_cast<History>(in[2]);
  qos.liveliness = static_cast<Liveliness>(in[3]);
  qos.depth = get_u32(in + 4);
  qos.deadline_ms = get_u64(in + 8);
  qos.lifespan_ms = get_u64(in + 16);
  qos.liveliness_lease_ms = get_u64(in + 24);
  return true;
}

bool qos_compatible(const QosProfile & requested, const QosProfile & offered)
{
  // reliability: reliable reader requires reliable writer
  if (requested.reliability == Reliability::RELIABLE &&
    offered.reliability == Reliability::BEST_EFFORT)
  {
    return false;
  }
  // durability: transient_local reader requires transient_local writer
  if (requested.durability == Durability::TRANSIENT_LOCAL &&
    offered.durability == Durability::VOLATILE)
  {
    return false;
  }
  return true;
}

bool qos_accepted_by_reader(const QosProfile & reader_qos, const QosProfile & writer_qos)
{
  // Reader-side view is lenient on reliability: a reliable reader still
  // receives from (and counts) a best-effort writer, the data simply arrives
  // at best-effort quality. This mirrors the rmw test contract, which counts
  // a best-effort publisher as matched from a reliable subscription while the
  // publisher does not count the reliable subscription (strict RxO on the
  // writer side, see qos_compatible). Durability RxO stays strict.
  if (reader_qos.durability == Durability::TRANSIENT_LOCAL &&
    writer_qos.durability == Durability::VOLATILE)
  {
    return false;
  }
  return true;
}

}  // namespace mdds
