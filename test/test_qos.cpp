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

#include <cstdint>

#include "gtest/gtest.h"
#include "mdds/qos.hpp"

namespace
{

TEST(Qos, PresetsMatchRosConventions)
{
  const auto def = mdds::QosProfile::preset_default();
  EXPECT_EQ(def.reliability, mdds::Reliability::RELIABLE);
  EXPECT_EQ(def.durability, mdds::Durability::VOLATILE);
  EXPECT_EQ(def.history, mdds::History::KEEP_LAST);
  EXPECT_EQ(def.depth, 10u);

  const auto be = mdds::QosProfile::preset_best_effort();
  EXPECT_EQ(be.reliability, mdds::Reliability::BEST_EFFORT);
  EXPECT_EQ(be.depth, 1u);

  const auto sensor = mdds::QosProfile::preset_sensor_data();
  EXPECT_EQ(sensor.reliability, mdds::Reliability::BEST_EFFORT);
  EXPECT_EQ(sensor.depth, 5u);
  EXPECT_EQ(sensor.deadline_ms, 100u);

  const auto tl = mdds::QosProfile::preset_transient_local();
  EXPECT_EQ(tl.durability, mdds::Durability::TRANSIENT_LOCAL);
  EXPECT_EQ(tl.reliability, mdds::Reliability::RELIABLE);

  const auto bulk = mdds::QosProfile::preset_bulk_data();
  EXPECT_EQ(bulk.history, mdds::History::KEEP_ALL);
  EXPECT_EQ(bulk.reliability, mdds::Reliability::RELIABLE);
}

TEST(Qos, PresetsAreValid)
{
  EXPECT_TRUE(mdds::qos_valid(mdds::QosProfile::preset_default()));
  EXPECT_TRUE(mdds::qos_valid(mdds::QosProfile::preset_best_effort()));
  EXPECT_TRUE(mdds::qos_valid(mdds::QosProfile::preset_sensor_data()));
  EXPECT_TRUE(mdds::qos_valid(mdds::QosProfile::preset_transient_local()));
  EXPECT_TRUE(mdds::qos_valid(mdds::QosProfile::preset_bulk_data()));
}

TEST(Qos, ValidityChecks)
{
  mdds::QosProfile q;
  EXPECT_TRUE(mdds::qos_valid(q));

  q.reliability = static_cast<mdds::Reliability>(2);
  EXPECT_FALSE(mdds::qos_valid(q));
  q.reliability = mdds::Reliability::RELIABLE;

  q.durability = static_cast<mdds::Durability>(2);
  EXPECT_FALSE(mdds::qos_valid(q));
  q.durability = mdds::Durability::VOLATILE;

  q.history = static_cast<mdds::History>(2);
  EXPECT_FALSE(mdds::qos_valid(q));
  q.history = mdds::History::KEEP_LAST;

  q.liveliness = static_cast<mdds::Liveliness>(2);
  EXPECT_FALSE(mdds::qos_valid(q));
  q.liveliness = mdds::Liveliness::AUTOMATIC;

  q.depth = 0;  // KEEP_LAST with depth 0 keeps nothing: invalid
  EXPECT_FALSE(mdds::qos_valid(q));
  q.history = mdds::History::KEEP_ALL;  // depth ignored under KEEP_ALL
  EXPECT_TRUE(mdds::qos_valid(q));
}

TEST(Qos, WireRoundTrip)
{
  const auto qos = mdds::QosProfile::preset_sensor_data();
  uint8_t buf[mdds::kQosWireSize];
  mdds::encode_qos(buf, qos);

  mdds::QosProfile back;
  ASSERT_TRUE(mdds::decode_qos(buf, sizeof(buf), back));
  EXPECT_EQ(back.reliability, qos.reliability);
  EXPECT_EQ(back.durability, qos.durability);
  EXPECT_EQ(back.history, qos.history);
  EXPECT_EQ(back.liveliness, qos.liveliness);
  EXPECT_EQ(back.depth, qos.depth);
  EXPECT_EQ(back.deadline_ms, qos.deadline_ms);
  EXPECT_EQ(back.lifespan_ms, qos.lifespan_ms);
  EXPECT_EQ(back.liveliness_lease_ms, qos.liveliness_lease_ms);

  EXPECT_FALSE(mdds::decode_qos(buf, mdds::kQosWireSize - 1, back));
  EXPECT_FALSE(mdds::decode_qos(nullptr, sizeof(buf), back));
}

}  // namespace
