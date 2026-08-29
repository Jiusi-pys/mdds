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

// UDP loopback transport: same-host inter-process byte pipe and host unit
// tests. One participant binds one UDP port in [base, base+count); presence
// datagrams keep the peer table alive; data frames are plain datagrams.
//
// Presence datagram layout (12 bytes, never dispatched to the listener):
//   0x00 'P' 'I' 'N' 'G' | domain:u32 (BE) | reserved:u3
// Data frames start with the 'MDDS' frame magic and are passed through
// verbatim. PeerId is the peer's UDP port.

#include "mdds/transport.hpp"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <map>
#include <mutex>
#include <thread>
#include <vector>

#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
using socket_t = SOCKET;
constexpr socket_t kInvalidSocket = INVALID_SOCKET;
#else
#include <arpa/inet.h>
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

    // Find a free port in the configured range.
    bool bound = false;
    for (uint16_t i = 0; i < config_.udp_port_count; ++i) {
      const uint16_t port = config_.udp_base_port + i;
      sockaddr_in addr{};
      addr.sin_family = AF_INET;
      addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
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
    send_presence(port_);
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
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = htons(static_cast<uint16_t>(peer));
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
  void send_presence(uint16_t to_port)
  {
    uint8_t pkt[kPresenceSize] = {0x00, 'P', 'I', 'N', 'G'};
    const uint32_t domain_be = htonl(config_.domain_id);
    std::memcpy(pkt + 5, &domain_be, sizeof(domain_be));
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = htons(to_port);
    sendto(
      socket_, reinterpret_cast<const char *>(pkt), sizeof(pkt), 0,
      reinterpret_cast<sockaddr *>(&addr), sizeof(addr));
  }

  void announce_loop()
  {
    while (running_) {
      for (uint16_t i = 0; i < config_.udp_port_count && running_; ++i) {
        const uint16_t port = config_.udp_base_port + i;
        if (port != port_) {
          send_presence(port);
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
      const uint16_t from_port = ntohs(from.sin_port);

      if (static_cast<size_t>(n) == kPresenceSize && buf[0] == 0x00 &&
        buf[1] == 'P' && buf[2] == 'I' && buf[3] == 'N' && buf[4] == 'G')
      {
        uint32_t domain_be;
        std::memcpy(&domain_be, buf.data() + 5, sizeof(domain_be));
        if (ntohl(domain_be) != config_.domain_id || from_port == port_) {
          continue;
        }
        bool is_new = false;
        {
          std::lock_guard<std::mutex> lock(peers_mutex_);
          auto [it, inserted] = peers_.emplace(from_port, std::chrono::steady_clock::now());
          it->second = std::chrono::steady_clock::now();
          is_new = inserted;
        }
        if (is_new) {
          listener_->on_peer_up(from_port);
        }
        continue;
      }

      // Data frame: only accept from known peers.
      {
        std::lock_guard<std::mutex> lock(peers_mutex_);
        if (peers_.find(from_port) == peers_.end()) {
          continue;
        }
      }
      listener_->on_bytes(from_port, buf.data(), static_cast<size_t>(n));
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
};

}  // namespace

std::unique_ptr<Transport> make_udp_loopback_transport()
{
  return std::make_unique<UdpLoopbackTransport>();
}

}  // namespace mdds
