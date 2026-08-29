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

#ifndef MDDS__TRANSPORT_HPP_
#define MDDS__TRANSPORT_HPP_

#include <cstddef>
#include <cstdint>
#include <memory>

namespace mdds
{

/// Opaque handle identifying a remote peer at the transport layer.
using PeerId = uint64_t;
constexpr PeerId kInvalidPeer = 0;

/// Default fragment payload budget per SendBytes call. Conservative so that
/// narrow channels (Proxy/BR, ~4 KB limit) still make progress; backends may
/// raise this per peer via max_payload().
constexpr size_t kDefaultMaxPayload = 3800;

class TransportListener
{
public:
  virtual ~TransportListener() = default;

  /// A peer became reachable (connection established).
  virtual void on_peer_up(PeerId peer) = 0;
  /// A peer went away; all its state must be dropped.
  virtual void on_peer_down(PeerId peer) = 0;
  /// One complete frame arrived from a peer. `data` is only valid for the
  /// duration of the call.
  virtual void on_bytes(PeerId peer, const uint8_t * data, size_t len) = 0;
};

struct TransportConfig
{
  uint32_t domain_id = 0;
  // UDP loopback backend knobs (ignored by other backends).
  uint16_t udp_base_port = 47811;   // first candidate listen port
  uint16_t udp_port_count = 32;     // ports scanned for bind + peer announce
  uint32_t udp_announce_ms = 500;   // presence announce period
  size_t udp_max_payload = 60000;   // advertised max frame size
};

/// Byte-pipe transport abstraction. Implementations must deliver each send()
/// as exactly one on_bytes() on the receiving side, in order, without loss on
/// an established connection (the DSoftBus Bytes channel already has these
/// semantics; the UDP loopback backend emulates them).
class Transport
{
public:
  virtual ~Transport() = default;

  virtual bool start(const TransportConfig & config, TransportListener * listener) = 0;
  virtual void stop() = 0;

  /// Queue/deliver one frame to a peer. Returns false on immediate failure
  /// (unknown peer, frame larger than max_payload, connection down).
  virtual bool send(PeerId peer, const uint8_t * data, size_t len) = 0;

  /// Maximum frame size accepted for this peer.
  virtual size_t max_payload(PeerId peer) const = 0;
};

/// UDP loopback transport: same-host inter-process traffic and host unit
/// tests. Cross-platform (Windows host, Linux, OHOS).
std::unique_ptr<Transport> make_udp_loopback_transport();

#ifdef MDDS_WITH_DSOFTBUS
/// DSoftBus Socket (DATA_TYPE_BYTES) transport: the cross-device data plane.
/// OpenHarmony only.
std::unique_ptr<Transport> make_dsoftbus_transport();
#endif

}  // namespace mdds

#endif  // MDDS__TRANSPORT_HPP_
