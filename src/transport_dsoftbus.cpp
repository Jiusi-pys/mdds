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

// DSoftBus Socket (DATA_TYPE_BYTES) transport backend — the cross-device data
// plane. OpenHarmony only; built when MDDS_WITH_DSOFTBUS is on.
//
// KaihongOS board facts that shape this code (verified 2026-08-29):
//  - The process must self-assign a native token carrying
//    ohos.permission.DISTRIBUTED_DATASYNC / DISTRIBUTED_SOFTBUS_CENTER via
//    GetAccessTokenId + SetSelfTokenID, and pkgName/session name must match
//    the device whitelist softbus_trans_permission.json: PKG_NAME
//    "com.kaihong.mdds", session names "com.kaihong.mdds.*".
//  - Synchronous Bind() fails with SOFTBUS_TRANS_PEER_SESSION_NOT_CREATED on
//    this build; BindAsync() + OnBind works.
//  - Server side: Listen socket accepts each peer on a NEW socket fd
//    delivered via OnBind.
//
// v1 limitations: one DsoftbusTransport instance per process (the C callback
// struct has no user data, so dispatch goes through a global table).

#include "mdds/transport.hpp"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <map>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "nativetoken_kit.h"
#include "socket.h"
#include "softbus_bus_center.h"
#include "softbus_error_code.h"
#include "token_setproc.h"

namespace mdds
{

namespace
{

constexpr const char * kPkgName = "com.kaihong.mdds";
constexpr const char * kSessionPrefix = "com.kaihong.mdds.d";
constexpr uint32_t kPeerPollMs = 2000;

class DsoftbusTransport : public Transport
{
public:
  DsoftbusTransport() = default;
  ~DsoftbusTransport() override {stop();}

  bool start(const TransportConfig & config, TransportListener * listener) override
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (running_ || s_instance != nullptr) {
      return false;
    }
    config_ = config;
    listener_ = listener;

    setup_token();

    char session_name[64];
    snprintf(session_name, sizeof(session_name), "%s%u", kSessionPrefix, config.domain_id);
    session_name_ = session_name;

    SocketInfo info{};
    info.name = session_name_.data();
    info.pkgName = const_cast<char *>(kPkgName);
    info.dataType = DATA_TYPE_BYTES;
    listen_fd_ = Socket(info);
    if (listen_fd_ < 0) {
      return false;
    }
    QosTV qos[] = {
      {QOS_TYPE_MIN_BW, 4 * 1024 * 1024},
      {QOS_TYPE_MAX_WAIT_TIMEOUT, 15000},
    };
    if (Listen(listen_fd_, qos, 2, &s_listener) != SOFTBUS_OK) {
      Shutdown(listen_fd_);
      listen_fd_ = -1;
      return false;
    }

    s_instance = this;
    running_ = true;
    poll_thread_ = std::thread([this] {poll_loop();});
    return true;
  }

  void stop() override
  {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (!running_) {
        return;
      }
      running_ = false;
    }
    if (poll_thread_.joinable()) {
      poll_thread_.join();
    }
    std::lock_guard<std::mutex> lock(mutex_);
    for (const auto & kv : peers_) {
      if (kv.second.socket_fd >= 0) {
        Shutdown(kv.second.socket_fd);
      }
    }
    peers_.clear();
    if (listen_fd_ >= 0) {
      Shutdown(listen_fd_);
      listen_fd_ = -1;
    }
    s_instance = nullptr;
  }

  bool send(PeerId peer, const uint8_t * data, size_t len) override
  {
    std::lock_guard<std::mutex> lock(mutex_);
    for (const auto & kv : peers_) {
      if (kv.second.peer_id == peer && kv.second.socket_fd >= 0) {
        return SendBytes(kv.second.socket_fd, data, static_cast<uint32_t>(len)) == SOFTBUS_OK;
      }
    }
    return false;
  }

  size_t max_payload(PeerId /*peer*/) const override
  {
    // Conservative: works even on narrow Proxy/BR channels; the fragmenter
    // handles the rest. GetSessionOption probing is a possible optimization.
    return kDefaultMaxPayload;
  }

  // ---- C callback dispatch (no user data in ISocketListener) ----
  static DsoftbusTransport * s_instance;

  static void dispatch_on_bind(int32_t fd, PeerSocketInfo info)
  {
    if (s_instance) {
      s_instance->on_bind(fd, info);
    }
  }
  static void dispatch_on_shutdown(int32_t fd, ShutdownReason reason)
  {
    if (s_instance) {
      s_instance->on_shutdown(fd, reason);
    }
  }
  static void dispatch_on_bytes(int32_t fd, const void * data, uint32_t len)
  {
    if (s_instance) {
      s_instance->on_bytes_received(fd, data, len);
    }
  }
  static void dispatch_on_error(int32_t fd, int32_t err)
  {
    if (s_instance) {
      s_instance->on_error(fd, err);
    }
  }

private:
  struct PeerState
  {
    PeerId peer_id = kInvalidPeer;
    std::string network_id;
    int32_t socket_fd = -1;   // connected channel fd (incoming or outgoing)
    int32_t pending_fd = -1;  // outgoing socket still binding
    bool up_notified = false;
  };

  static void setup_token()
  {
    const char * perms[] = {
      "ohos.permission.DISTRIBUTED_DATASYNC",
      "ohos.permission.DISTRIBUTED_SOFTBUS_CENTER",
    };
    NativeTokenInfoParams info{};
    info.permsNum = 2;
    info.perms = perms;
    info.processName = kPkgName;
    info.aplStr = "system_basic";
    uint64_t token_id = GetAccessTokenId(&info);
    SetSelfTokenID(token_id);
  }

  void poll_loop()
  {
    while (true) {
      {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!running_) {
          return;
        }
      }
      connect_online_peers();
      std::unique_lock<std::mutex> lock(stop_mutex_);
      stop_cv_.wait_for(
        lock, std::chrono::milliseconds(kPeerPollMs), [this] {return !running_.load();});
    }
  }

  /// Enumerate LNN peers and BindAsync to those not yet connected.
  void connect_online_peers()
  {
    NodeBasicInfo * nodes = nullptr;
    int32_t num = 0;
    if (GetAllNodeDeviceInfo(kPkgName, &nodes, &num) != SOFTBUS_OK || num <= 0) {
      if (nodes != nullptr) {
        FreeNodeInfo(nodes);
      }
      return;
    }
    std::vector<std::string> to_bind;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      for (int32_t i = 0; i < num; ++i) {
        const std::string nid = nodes[i].networkId;
        auto it = peers_.find(nid);
        if (it == peers_.end()) {
          PeerState st;
          st.peer_id = next_peer_id_++;
          st.network_id = nid;
          peers_[nid] = st;
          to_bind.push_back(nid);
        } else if (it->second.socket_fd < 0 && it->second.pending_fd < 0) {
          to_bind.push_back(nid);  // previous bind failed/cleared; retry
        }
      }
    }
    FreeNodeInfo(nodes);

    for (const std::string & nid : to_bind) {
      bind_peer(nid);
    }
  }

  void bind_peer(const std::string & network_id)
  {
    SocketInfo info{};
    char peer_name[64];
    snprintf(peer_name, sizeof(peer_name), "%s", session_name_.c_str());
    info.name = session_name_.data();
    info.peerName = peer_name;
    info.peerNetworkId = const_cast<char *>(network_id.c_str());
    info.pkgName = const_cast<char *>(kPkgName);
    info.dataType = DATA_TYPE_BYTES;
    int32_t fd = Socket(info);
    if (fd < 0) {
      return;
    }
    {
      std::lock_guard<std::mutex> lock(mutex_);
      auto it = peers_.find(network_id);
      if (it == peers_.end()) {
        Shutdown(fd);
        return;
      }
      it->second.pending_fd = fd;
    }
    QosTV qos[] = {
      {QOS_TYPE_MIN_BW, 4 * 1024 * 1024},
      {QOS_TYPE_MAX_WAIT_TIMEOUT, 15000},
    };
    // Sync Bind() is broken on this KaihongOS build; always BindAsync.
    if (BindAsync(fd, qos, 2, &s_listener) != SOFTBUS_OK) {
      std::lock_guard<std::mutex> lock(mutex_);
      auto it = peers_.find(network_id);
      if (it != peers_.end() && it->second.pending_fd == fd) {
        it->second.pending_fd = -1;
      }
      Shutdown(fd);
    }
  }

  void on_bind(int32_t fd, PeerSocketInfo info)
  {
    const std::string nid = info.networkId != nullptr ? info.networkId : "";
    if (nid.empty()) {
      return;
    }
    bool notify = false;
    PeerId peer_id = kInvalidPeer;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      auto it = peers_.find(nid);
      if (it == peers_.end()) {
        // Inbound connection from a peer we have not seen via LNN yet.
        PeerState st;
        st.peer_id = next_peer_id_++;
        st.network_id = nid;
        it = peers_.emplace(nid, st).first;
      }
      PeerState & st = it->second;
      if (st.pending_fd == fd) {
        st.pending_fd = -1;  // our outgoing bind completed
      }
      if (st.socket_fd < 0) {
        st.socket_fd = fd;
        st.up_notified = true;
        notify = true;
        peer_id = st.peer_id;
      } else if (st.socket_fd != fd) {
        // Duplicate channel (both sides bound); keep the first, drop this one.
        Shutdown(fd);
        return;
      }
    }
    if (notify) {
      listener_->on_peer_up(peer_id);
    }
  }

  void on_shutdown(int32_t fd, ShutdownReason /*reason*/)
  {
    PeerId peer_id = kInvalidPeer;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (fd == listen_fd_) {
        running_ = false;
        stop_cv_.notify_all();
        return;
      }
      for (auto & kv : peers_) {
        PeerState & st = kv.second;
        if (st.socket_fd == fd) {
          st.socket_fd = -1;
          if (st.up_notified) {
            st.up_notified = false;
            peer_id = st.peer_id;
          }
        } else if (st.pending_fd == fd) {
          st.pending_fd = -1;
        }
      }
    }
    if (peer_id != kInvalidPeer) {
      listener_->on_peer_down(peer_id);
    }
  }

  void on_bytes_received(int32_t fd, const void * data, uint32_t len)
  {
    PeerId peer_id = kInvalidPeer;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      for (const auto & kv : peers_) {
        if (kv.second.socket_fd == fd) {
          peer_id = kv.second.peer_id;
          break;
        }
      }
    }
    if (peer_id != kInvalidPeer) {
      listener_->on_bytes(peer_id, static_cast<const uint8_t *>(data), len);
    }
  }

  void on_error(int32_t fd, int32_t /*err*/)
  {
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto & kv : peers_) {
      if (kv.second.pending_fd == fd) {
        kv.second.pending_fd = -1;  // poll loop retries
        Shutdown(fd);
        return;
      }
    }
  }

  TransportConfig config_;
  TransportListener * listener_ = nullptr;
  int32_t listen_fd_ = -1;
  std::string session_name_;

  std::atomic<bool> running_{false};
  std::thread poll_thread_;
  std::mutex stop_mutex_;
  std::condition_variable stop_cv_;

  std::mutex mutex_;
  std::map<std::string, PeerState> peers_;  // keyed by networkId
  PeerId next_peer_id_ = 1;

  static const ISocketListener s_listener;
};

DsoftbusTransport * DsoftbusTransport::s_instance = nullptr;

const ISocketListener DsoftbusTransport::s_listener = {
  .OnBind = &DsoftbusTransport::dispatch_on_bind,
  .OnShutdown = &DsoftbusTransport::dispatch_on_shutdown,
  .OnBytes = &DsoftbusTransport::dispatch_on_bytes,
  .OnError = &DsoftbusTransport::dispatch_on_error,
};

}  // namespace

std::unique_ptr<Transport> make_dsoftbus_transport()
{
  return std::make_unique<DsoftbusTransport>();
}

}  // namespace mdds
