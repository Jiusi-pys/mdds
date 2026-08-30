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

// UDP transport: same-host inter-process byte pipe, host unit tests, and
// (with TransportConfig::udp_cross_device) cross-device traffic between
// IP-reachable hosts. One participant binds one UDP port in
// [base, base+count) on INADDR_ANY; presence datagrams keep the peer table
// alive; data frames are plain datagrams.
//
// Presence datagram layout (12 bytes, never dispatched to the listener):
//   0x00 'P' 'I' 'N' 'G' | domain:u32 (BE) | reserved:u3
// Data frames start with the 'MDDS' frame magic and are passed through
// verbatim.
//
// PeerId encodes the peer's address as (ipv4 << 16) | port. Same-host peers
// are always normalized to 127.0.0.1 (a host may learn about its own
// processes via both loopback and a self-received interface broadcast);
// cross-device peers key on the datagram source address. This gives every
// process on every host a distinct peer identity with no coordination —
// unlike the DSoftBus backend, whose session-server registration admits only
// one process per (pkgName, sessionName) per device.

#include "mdds/transport.hpp"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <map>
#include <mutex>
#include <set>
#include <thread>
#include <vector>

#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
using socket_t = SOCKET;
constexpr socket_t kInvalidSocket = INVALID_SOCKET;
#else
#include <arpa/inet.h>
#include <ifaddrs.h>
#include <net/if.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
using socket_t = int;
constexpr socket_t kInvalidSocket = -1;
#endif

namespace mdds
{

namespace
{

constexpr size_t kPresenceSize = 12;
constexpr uint32_t kPeerTimeoutFactor = 4;  // drop after 4x announce period
constexpr uint32_t kLoopbackIp = 0x7F000001;  // 127.0.0.1, host byte order

mdds::PeerId make_peer_key(uint32_t ip, uint16_t port)
{
  return (static_cast<mdds::PeerId>(ip) << 16) | port;
}

bool debug_enabled()
{
  static const bool on = getenv("MDDS_DEBUG") != nullptr;
  return on;
}

void debug_peer(const char * what, mdds::PeerId peer)
{
  if (!debug_enabled()) {
    return;
  }
  const uint32_t ip = static_cast<uint32_t>(peer >> 16);
  fprintf(
    stderr, "[mdds/udp] %s %u.%u.%u.%u:%u\n", what,
    (ip >> 24) & 0xFF, (ip >> 16) & 0xFF, (ip >> 8) & 0xFF, ip & 0xFF,
    static_cast<unsigned>(peer & 0xFFFF));
}

void close_socket(socket_t s)
{
#ifdef _WIN32
  closesocket(s);
#else
  close(s);
#endif
}

class UdpLoopbackTransport : public Transport
{
public:
  UdpLoopbackTransport() = default;
  ~UdpLoopbackTransport() override {stop();}

  bool start(const TransportConfig & config, TransportListener * listener) override
  {
    if (running_) {
      return false;
    }
    config_ = config;
    listener_ = listener;

#ifdef _WIN32
    WSADATA wsa;
    if (WSAStartup(MAKEWORD(2, 2), &wsa) != 0) {
      return false;
    }
#endif

    socket_ = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
    if (socket_ == kInvalidSocket) {
      return false;
    }

    // Fragmented samples are (re)sent as back-to-back bursts of up to
    // udp_max_payload-sized datagrams. The kernel-default receive buffer
    // (208 KiB on Linux) overflows mid-burst and drops the tail datagrams
    // of every retry alike, livelocking ACKNACK recovery. Raise the buffer;
    // the kernel clamps the request to rmem_max.
    {
      const int rcvbuf = 16 * 1024 * 1024;
      setsockopt(
        socket_, SOL_SOCKET, SO_RCVBUF,
        reinterpret_cast<const char *>(&rcvbuf), sizeof(rcvbuf));
    }

    // Find a free port in the configured range. Bind on ANY so cross-device
    // datagrams (addressed to a real interface) arrive on the same socket as
    // loopback traffic.
    bool bound = false;
    for (uint16_t i = 0; i < config_.udp_port_count; ++i) {
      const uint16_t port = config_.udp_base_port + i;
      sockaddr_in addr{};
      addr.sin_family = AF_INET;
      addr.sin_addr.s_addr = htonl(INADDR_ANY);
      addr.sin_port = htons(port);
      if (::bind(socket_, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) == 0) {
        port_ = port;
        bound = true;
        break;
      }
    }
    if (!bound) {
      close_socket(socket_);
      socket_ = kInvalidSocket;
      return false;
    }

    if (config_.udp_cross_device) {
      const int one = 1;
      setsockopt(
        socket_, SOL_SOCKET, SO_BROADCAST,
        reinterpret_cast<const char *>(&one), sizeof(one));
      enumerate_interfaces();
    }
    self_addrs_.insert(kLoopbackIp);

    running_ = true;
    recv_thread_ = std::thread([this] {recv_loop();});
    announce_thread_ = std::thread([this] {announce_loop();});
    return true;
  }

  void stop() override
  {
    if (!running_) {
      return;
    }
    running_ = false;
    // Wake the recv thread with a presence datagram to ourselves.
    send_presence(kLoopbackIp, port_);
    if (recv_thread_.joinable()) {
      recv_thread_.join();
    }
    if (announce_thread_.joinable()) {
      announce_thread_.join();
    }
    close_socket(socket_);
    socket_ = kInvalidSocket;
    std::lock_guard<std::mutex> lock(peers_mutex_);
    peers_.clear();
#ifdef _WIN32
    WSACleanup();
#endif
  }

  bool send(PeerId peer, const uint8_t * data, size_t len) override
  {
    if (!running_ || len > max_payload(peer)) {
      return false;
    }
    {
      std::lock_guard<std::mutex> lock(peers_mutex_);
      if (peers_.find(peer) == peers_.end()) {
        return false;
      }
    }
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(static_cast<uint32_t>(peer >> 16));
    addr.sin_port = htons(static_cast<uint16_t>(peer & 0xFFFF));
    const int rc = sendto(
      socket_, reinterpret_cast<const char *>(data), static_cast<int>(len), 0,
      reinterpret_cast<sockaddr *>(&addr), sizeof(addr));
    return rc == static_cast<int>(len);
  }

  size_t max_payload(PeerId /*peer*/) const override
  {
    return config_.udp_max_payload;
  }

private:
  void send_presence(uint32_t ip, uint16_t to_port)
  {
    uint8_t pkt[kPresenceSize] = {0x00, 'P', 'I', 'N', 'G'};
    const uint32_t domain_be = htonl(config_.domain_id);
    std::memcpy(pkt + 5, &domain_be, sizeof(domain_be));
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(ip);
    addr.sin_port = htons(to_port);
    sendto(
      socket_, reinterpret_cast<const char *>(pkt), sizeof(pkt), 0,
      reinterpret_cast<sockaddr *>(&addr), sizeof(addr));
  }

  /// Snapshot the host's IPv4 interfaces: self addresses (for same-host
  /// normalization and own-broadcast suppression) and broadcast addresses
  /// (cross-device presence targets). No-op on Windows, where the backend
  /// stays loopback-only (host unit tests never span devices).
  void enumerate_interfaces()
  {
#ifdef _WIN32
    return;
#else
    ifaddrs * ifas = nullptr;
    if (getifaddrs(&ifas) != 0) {
      return;
    }
    for (ifaddrs * ifa = ifas; ifa != nullptr; ifa = ifa->ifa_next) {
      if (ifa->ifa_addr == nullptr || ifa->ifa_addr->sa_family != AF_INET ||
        !(ifa->ifa_flags & IFF_UP))
      {
        continue;
      }
      const uint32_t ip = ntohl(
        reinterpret_cast<const sockaddr_in *>(ifa->ifa_addr)->sin_addr.s_addr);
      self_addrs_.insert(ip);
      if ((ifa->ifa_flags & IFF_BROADCAST) && !(ifa->ifa_flags & IFF_LOOPBACK) &&
        ifa->ifa_broadaddr != nullptr)
      {
        const uint32_t bcast = ntohl(
          reinterpret_cast<const sockaddr_in *>(ifa->ifa_broadaddr)->sin_addr.s_addr);
        if (bcast != 0) {
          broadcast_addrs_.insert(bcast);
        }
      }
    }
    freeifaddrs(ifas);
#endif
  }

  void announce_loop()
  {
    while (running_) {
      for (uint16_t i = 0; i < config_.udp_port_count && running_; ++i) {
        const uint16_t port = config_.udp_base_port + i;
        if (port != port_) {
          send_presence(kLoopbackIp, port);
        }
      }
      // Cross-device presence: probe the whole port range on every interface
      // broadcast address. Remote mdds processes answer from whichever port
      // they bound; same-host receivers of our own broadcasts are normalized
      // to the loopback peer we already announce to directly.
      if (config_.udp_cross_device) {
        for (uint32_t bcast : broadcast_addrs_) {
          for (uint16_t i = 0; i < config_.udp_port_count && running_; ++i) {
            send_presence(bcast, config_.udp_base_port + i);
          }
        }
      }
      sweep_peers();
      const auto period = std::chrono::milliseconds(config_.udp_announce_ms);
      std::unique_lock<std::mutex> lock(stop_mutex_);
      stop_cv_.wait_for(lock, period, [this] {return !running_.load();});
    }
  }

  void sweep_peers()
  {
    const auto now = std::chrono::steady_clock::now();
    const auto timeout =
      std::chrono::milliseconds(config_.udp_announce_ms * kPeerTimeoutFactor);
    std::vector<PeerId> dead;
    {
      std::lock_guard<std::mutex> lock(peers_mutex_);
      for (auto it = peers_.begin(); it != peers_.end(); ) {
        if (now - it->second > timeout) {
          dead.push_back(it->first);
          it = peers_.erase(it);
        } else {
          ++it;
        }
      }
    }
    for (PeerId p : dead) {
      debug_peer("peer down", p);
      listener_->on_peer_down(p);
    }
  }

  void recv_loop()
  {
    std::vector<uint8_t> buf(config_.udp_max_payload + 64);
    while (running_) {
      sockaddr_in from{};
#ifdef _WIN32
      int from_len = sizeof(from);
#else
      socklen_t from_len = sizeof(from);
#endif
      const int n = recvfrom(
        socket_, reinterpret_cast<char *>(buf.data()), static_cast<int>(buf.size()), 0,
        reinterpret_cast<sockaddr *>(&from), &from_len);
      if (n <= 0) {
        if (running_) {
          std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        continue;
      }
      const uint32_t from_ip = ntohl(from.sin_addr.s_addr);
      const uint16_t from_port = ntohs(from.sin_port);
      // Normalize same-host senders to their loopback identity: sibling
      // processes receive our interface broadcasts with the interface address
      // as source, but same-host peers are announced to (and addressed as)
      // 127.0.0.1.
      const bool same_host = self_addrs_.count(from_ip) != 0;
      if (from_port == port_ && same_host) {
        continue;  // our own datagram, whatever its type
      }
      const PeerId peer =
        make_peer_key(same_host ? kLoopbackIp : from_ip, from_port);

      if (static_cast<size_t>(n) == kPresenceSize && buf[0] == 0x00 &&
        buf[1] == 'P' && buf[2] == 'I' && buf[3] == 'N' && buf[4] == 'G')
      {
        uint32_t domain_be;
        std::memcpy(&domain_be, buf.data() + 5, sizeof(domain_be));
        if (ntohl(domain_be) != config_.domain_id) {
          continue;
        }
        bool is_new = false;
        {
          std::lock_guard<std::mutex> lock(peers_mutex_);
          auto [it, inserted] = peers_.emplace(peer, std::chrono::steady_clock::now());
          it->second = std::chrono::steady_clock::now();
          is_new = inserted;
        }
        if (is_new) {
          debug_peer("peer up", peer);
          listener_->on_peer_up(peer);
        }
        continue;
      }

      // Data frame: only accept from known peers.
      {
        std::lock_guard<std::mutex> lock(peers_mutex_);
        if (peers_.find(peer) == peers_.end()) {
          continue;
        }
      }
      listener_->on_bytes(peer, buf.data(), static_cast<size_t>(n));
    }
  }

  TransportConfig config_;
  TransportListener * listener_ = nullptr;
  socket_t socket_ = kInvalidSocket;
  uint16_t port_ = 0;
  std::atomic<bool> running_{false};
  std::thread recv_thread_;
  std::thread announce_thread_;
  std::mutex stop_mutex_;
  std::condition_variable stop_cv_;
  std::mutex peers_mutex_;
  std::map<PeerId, std::chrono::steady_clock::time_point> peers_;
  std::set<uint32_t> self_addrs_;       // host byte order, incl. 127.0.0.1
  std::set<uint32_t> broadcast_addrs_;  // host byte order, non-loopback only
};

}  // namespace

std::unique_ptr<Transport> make_udp_loopback_transport()
{
  return std::make_unique<UdpLoopbackTransport>();
}

}  // namespace mdds
