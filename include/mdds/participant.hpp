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

#ifndef MDDS__PARTICIPANT_HPP_
#define MDDS__PARTICIPANT_HPP_

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "mdds/discovery.hpp"
#include "mdds/guid.hpp"
#include "mdds/qos.hpp"

namespace mdds
{

struct MessageInfo
{
  Guid writer_guid;
  uint64_t seq;
  /// system_clock milliseconds at write time (0 when unknown). Used for
  /// lifespan bookkeeping.
  uint64_t pub_time_ms = 0;
};

class Writer
{
public:
  virtual ~Writer() = default;
  virtual Guid guid() const = 0;
  virtual const std::string & topic() const = 0;
  virtual const std::string & type_name() const = 0;
  virtual const QosProfile & qos() const = 0;
  /// Publish one serialized sample. Thread-safe.
  virtual bool write(const uint8_t * data, size_t len) = 0;
  /// Number of matched remote readers.
  virtual size_t matched_count() const = 0;
  /// Assert liveliness: immediately broadcasts a HEARTBEAT to every peer so
  /// remote participants refresh this writer's last-seen timestamp. Used for
  /// MANUAL_BY_TOPIC liveliness; harmless for other kinds. Thread-safe.
  virtual void assert_liveliness() = 0;
};

class Reader
{
public:
  virtual ~Reader() = default;
  virtual Guid guid() const = 0;
  virtual const std::string & topic() const = 0;
  virtual const std::string & type_name() const = 0;
  virtual const QosProfile & qos() const = 0;
  /// Pop the oldest unread sample. Returns false when empty. Samples past
  /// their writer's lifespan are dropped lazily here.
  virtual bool take(std::vector<uint8_t> & out, MessageInfo & info) = 0;
  virtual size_t available() const = 0;
  /// Number of matched remote writers.
  virtual size_t matched_count() const = 0;
  /// Cumulative count of samples lost (sequence gaps observed from matched
  /// writers). Feeds the RMW_EVENT_MESSAGE_LOST status.
  virtual uint64_t messages_lost() const = 0;
  /// Called (from transport/timer threads) whenever new data is available.
  virtual void set_data_callback(std::function<void()> cb) = 0;
};

struct ParticipantConfig
{
  uint32_t domain_id = 0;
  bool use_udp_loopback = true;   // same-host inter-process + tests
  bool use_dsoftbus = true;       // cross-device (no-op when not built in)
  uint32_t announce_period_ms = 2000;  // also the reliable-writer heartbeat period
  // UDP loopback backend knobs (forwarded to TransportConfig).
  uint16_t udp_base_port = 47811;
  uint16_t udp_port_count = 32;
  uint32_t udp_announce_ms = 500;
};

/// One mdds domain participant. Owns transports, discovery, endpoint tables
/// and the reliability machinery. Thread-safe.
class Participant
{
public:
  virtual ~Participant() = default;

  static std::unique_ptr<Participant> create(const ParticipantConfig & config);

  virtual Guid guid() const = 0;

  virtual Writer * create_writer(
    const std::string & topic, const std::string & type_name, const QosProfile & qos) = 0;
  /// `ignore_local` readers skip samples from writers owned by this same
  /// participant (rmw ignore_local_publications). Local delivery itself is
  /// always on for matching local writer/reader pairs.
  virtual Reader * create_reader(
    const std::string & topic, const std::string & type_name, const QosProfile & qos,
    bool ignore_local = false) = 0;
  virtual void destroy_writer(Writer * writer) = 0;
  virtual void destroy_reader(Reader * reader) = 0;

  /// Node bookkeeping for the ROS graph (ANNOUNCE NODE entries).
  virtual void add_node(const std::string & ns, const std::string & name) = 0;
  virtual void remove_node(const std::string & ns, const std::string & name) = 0;

  /// Snapshot of every remote participant currently visible.
  virtual std::vector<ParticipantSnapshot> remote_participants() const = 0;

  /// Liveliness observation (steady_clock milliseconds):
  /// last time any DATA/HEARTBEAT was received from the given remote writer.
  /// Returns false when the writer is unknown or nothing was received yet.
  virtual bool remote_writer_last_seen_ms(const Guid & writer, uint64_t & out_ms) const = 0;
  /// Last time an ANNOUNCE was received from the given remote participant
  /// (AUTOMATIC liveliness source). Returns false when unknown.
  virtual bool remote_participant_last_seen_ms(
    const Guid & participant, uint64_t & out_ms) const = 0;

  /// Called whenever the discovered graph changes (remote endpoints or nodes
  /// added/removed). Invoked from internal threads; keep it cheap.
  virtual void set_graph_callback(std::function<void()> cb) = 0;

  /// Force an immediate ANNOUNCE broadcast (normally automatic on changes).
  virtual void announce_now() = 0;
};

}  // namespace mdds

#endif  // MDDS__PARTICIPANT_HPP_
