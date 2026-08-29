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

// mdds participant engine: transports, discovery, endpoint matching,
// fragmentation and reliability. See docs/design.md.
//
// Locking discipline (mirrors the DSoftBus rule): never invoke user callbacks
// (graph/data) while holding mutex_; collect them and fire after unlocking.

#include "mdds/participant.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <map>
#include <mutex>
#include <set>
#include <thread>

#include "mdds/fragment.hpp"
#include "mdds/frame.hpp"
#include "mdds/transport.hpp"

namespace mdds
{

namespace
{

constexpr size_t kKeepAllCap = 256;  // practical cap for KEEP_ALL writer history

uint64_t system_now_ms()
{
  return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
           std::chrono::system_clock::now().time_since_epoch()).count());
}

uint64_t steady_now_ms()
{
  return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
           std::chrono::steady_clock::now().time_since_epoch()).count());
}

/// DDS lifespan: a sample expires lifespan_ms after its publication time
/// (writer QoS; 0 = infinite). pub_time_ms == 0 (unknown) never expires.
bool lifespan_expired(uint64_t lifespan_ms, uint64_t pub_time_ms, uint64_t now_ms)
{
  return lifespan_ms > 0 && pub_time_ms > 0 && now_ms >= pub_time_ms + lifespan_ms;
}

// PeerId layout: high 8 bits = transport index, low 56 bits = backend id.
PeerId make_peer_id(size_t transport_index, PeerId backend_id)
{
  return (static_cast<PeerId>(transport_index) << 56) | (backend_id & 0x00ffffffffffffffULL);
}

size_t peer_transport_index(PeerId peer)
{
  return static_cast<size_t>(peer >> 56);
}

PeerId peer_backend_id(PeerId peer)
{
  return peer & 0x00ffffffffffffffULL;
}

class ParticipantImpl;

class WriterImpl : public Writer
{
  friend class ParticipantImpl;

public:
  WriterImpl(const std::string & topic, const std::string & type, const QosProfile & qos)
  : topic_(topic), type_(type), qos_(qos) {}

  Guid guid() const override {return guid_;}
  const std::string & topic() const override {return topic_;}
  const std::string & type_name() const override {return type_;}
  const QosProfile & qos() const override {return qos_;}
  size_t matched_count() const override {return matched_count_.load();}

  bool write(const uint8_t * data, size_t len) override;
  void assert_liveliness() override;

  Guid guid_;
  std::string topic_;
  std::string type_;
  QosProfile qos_;
  ParticipantImpl * owner_ = nullptr;
  std::atomic<uint64_t> seq_{0};
  std::atomic<size_t> matched_count_{0};

  // Writer history: recent samples for retransmission (reliable) and for
  // replay to late-joining readers (transient_local). Both paths share the
  // depth/keep_all cap and the lifespan sweep.
  struct CachedSample
  {
    uint64_t seq;
    uint64_t pub_time_ms;
    std::vector<uint8_t> data;
  };
  std::deque<CachedSample> history_;

  /// Cache one published sample for retransmission / late-joiner replay.
  /// Returns false when a KEEP_ALL history is already full: the sample is
  /// rejected (backpressure) instead of silently evicting the oldest entry,
  /// which would violate KEEP_ALL semantics.
  bool cache_sample(uint64_t seq, uint64_t pub_time_ms, const uint8_t * data, size_t len)
  {
    if (qos_.reliability != Reliability::RELIABLE &&
      qos_.durability != Durability::TRANSIENT_LOCAL)
    {
      return true;
    }
    const size_t cap =
      qos_.history == History::KEEP_ALL ? kKeepAllCap : std::max<size_t>(qos_.depth, 1);
    if (qos_.history == History::KEEP_ALL && history_.size() >= cap) {
      return false;
    }
    history_.push_back(CachedSample{seq, pub_time_ms, std::vector<uint8_t>(data, data + len)});
    while (history_.size() > cap) {
      history_.pop_front();
    }
    // Entries are appended in publication order, so expired ones sit at front.
    const uint64_t now = system_now_ms();
    while (!history_.empty() &&
      lifespan_expired(qos_.lifespan_ms, history_.front().pub_time_ms, now))
    {
      history_.pop_front();
    }
    return true;
  }
};

class ReaderImpl : public Reader
{
  friend class ParticipantImpl;

public:
  ReaderImpl(
    const std::string & topic, const std::string & type, const QosProfile & qos,
    bool ignore_local)
  : topic_(topic), type_(type), qos_(qos), ignore_local_(ignore_local) {}

  Guid guid() const override {return guid_;}
  const std::string & topic() const override {return topic_;}
  const std::string & type_name() const override {return type_;}
  const QosProfile & qos() const override {return qos_;}
  size_t matched_count() const override {return matched_count_.load();}

  bool take(std::vector<uint8_t> & out, MessageInfo & info) override
  {
    std::lock_guard<std::mutex> lock(queue_mutex_);
    sweep_expired_locked();
    if (queue_.empty()) {
      return false;
    }
    out = std::move(queue_.front().data);
    info = queue_.front().info;
    queue_.pop_front();
    return true;
  }

  size_t available() const override
  {
    std::lock_guard<std::mutex> lock(queue_mutex_);
    sweep_expired_locked();
    return queue_.size();
  }

  uint64_t messages_lost() const override
  {
    std::lock_guard<std::mutex> lock(queue_mutex_);
    return lost_count_;
  }

  void set_data_callback(std::function<void()> cb) override
  {
    std::lock_guard<std::mutex> lock(queue_mutex_);
    data_callback_ = std::move(cb);
  }

  /// Enqueue one sample from a (remote or local) writer. Returns true if the
  /// sample was queued and the data callback (if any) should be fired by the
  /// caller (outside any lock). Duplicates and lifespan-expired samples are
  /// dropped. Out-of-order receipts within a 64-seq window are tracked so
  /// retransmissions can heal holes; missing seqs are only counted lost when
  /// the writer GAPs them or the sequence jumps beyond the window.
  bool enqueue(
    const Guid & writer_guid, uint64_t seq, const uint8_t * data, size_t len,
    const QosProfile & writer_qos, uint64_t pub_time_ms,
    bool & gap_detected, uint64_t & gap_base, uint64_t & gap_bitmap)
  {
    std::lock_guard<std::mutex> lock(queue_mutex_);
    WriterRxState & st = rx_[writer_guid];
    if (seq <= st.last_seq) {
      return false;  // duplicate (also the gateway anti-loop guard)
    }
    const uint64_t offset = seq - st.last_seq - 1;
    if (offset >= 64) {
      // Too far ahead to track: declare the whole hole lost and jump. No
      // NACK is useful afterwards (resends would be <= last_seq), so this
      // does not set gap_detected.
      lost_count_ += offset;
      st.last_seq = seq;
      st.window = 0;
    } else if (offset > 0) {
      if (st.window & (1ULL << offset)) {
        return false;  // duplicate retransmit of an already-received sample
      }
      st.window |= (1ULL << offset);
      gap_detected =
        qos_.reliability == Reliability::RELIABLE &&
        writer_qos.reliability == Reliability::RELIABLE;
      gap_base = st.last_seq + 1;
      gap_bitmap = st.window;
    } else {
      // offset == 0, contiguous arrival. The bit for this position may
      // already be set: apply_gap() can slide the baseline up to an
      // out-of-order receipt; then a late retransmit of it is a duplicate.
      const bool already_received = (st.window & 1ULL) != 0;
      ++st.last_seq;
      st.window >>= 1;
      while (st.window & 1ULL) {
        ++st.last_seq;
        st.window >>= 1;
      }
      if (already_received) {
        return false;  // baseline slid, nothing to queue
      }
    }

    if (lifespan_expired(writer_qos.lifespan_ms, pub_time_ms, system_now_ms())) {
      return false;  // already expired on arrival: drop without queueing
    }
    QueuedSample sample;
    if (len > 0) {
      sample.data.assign(data, data + len);
    }
    sample.info = MessageInfo{writer_guid, seq, pub_time_ms};
    sample.lifespan_ms = writer_qos.lifespan_ms;
    queue_.push_back(std::move(sample));
    if (qos_.history == History::KEEP_LAST) {
      const size_t depth = std::max<size_t>(qos_.depth, 1);
      while (queue_.size() > depth) {
        queue_.pop_front();
      }
    }
    sweep_expired_locked();
    return true;
  }

  /// Current NACK state for a writer: (base, bitmap) where bit i of bitmap
  /// says seq base+i was already received. base is always the first missing
  /// seq. Used by the heartbeat handler to emit precise ACKNACKs.
  /// Caller must NOT hold queue_mutex_.
  std::pair<uint64_t, uint64_t> nack_state(const Guid & writer_guid)
  {
    std::lock_guard<std::mutex> lock(queue_mutex_);
    const auto it = rx_.find(writer_guid);
    if (it == rx_.end()) {
      return {1, 0};
    }
    return {it->second.last_seq + 1, it->second.window};
  }

  /// The writer declared [gap_start, gap_end] (inclusive) evicted forever.
  /// Advance the baseline past the range, counting not-received seqs as lost
  /// exactly once. Seqs already received (window bits) are not lost.
  void apply_gap(const Guid & writer_guid, uint64_t gap_start, uint64_t gap_end)
  {
    // gap_start only bounds the writer's announcement; seqs <= last_seq are
    // already accounted for, so the effective range is (last_seq, gap_end].
    static_cast<void>(gap_start);
    std::lock_guard<std::mutex> lock(queue_mutex_);
    const auto it = rx_.find(writer_guid);
    if (it == rx_.end()) {
      return;  // never received anything from this writer
    }
    WriterRxState & st = it->second;
    if (gap_end <= st.last_seq) {
      return;  // already accounted for
    }
    const uint64_t span = gap_end - st.last_seq;  // seqs (last_seq, gap_end]
    uint64_t received = 0;
    const uint64_t tracked = std::min<uint64_t>(span, 64);
    for (uint64_t i = 0; i < tracked; ++i) {
      if (st.window & (1ULL << i)) {
        ++received;
      }
    }
    lost_count_ += span - received;
    st.last_seq = gap_end;
    st.window = span >= 64 ? 0 : (st.window >> span);
  }

  void fire_callback()
  {
    std::function<void()> cb;
    {
      std::lock_guard<std::mutex> lock(queue_mutex_);
      cb = data_callback_;
    }
    if (cb) {
      cb();
    }
  }

  Guid guid_;
  std::string topic_;
  std::string type_;
  QosProfile qos_;
  bool ignore_local_ = false;
  ParticipantImpl * owner_ = nullptr;
  std::atomic<size_t> matched_count_{0};

  struct QueuedSample
  {
    std::vector<uint8_t> data;
    MessageInfo info;
    uint64_t lifespan_ms = 0;  // writer-side lifespan, 0 = infinite
  };

  mutable std::mutex queue_mutex_;
  mutable std::deque<QueuedSample> queue_;
  // Per-remote-writer receive state: contiguous baseline + a 64-seq window of
  // out-of-order receipts (bit i set <=> seq last_seq+1+i received). Drives
  // precise ACKNACK bitmaps, duplicate suppression and GAP healing.
  struct WriterRxState
  {
    uint64_t last_seq = 0;
    uint64_t window = 0;
  };
  std::map<Guid, WriterRxState> rx_;
  uint64_t lost_count_ = 0;            // cumulative sequence gaps
  std::function<void()> data_callback_;

private:
  /// Lazily drop queued samples past their writer's lifespan.
  /// Caller must hold queue_mutex_.
  void sweep_expired_locked() const
  {
    const uint64_t now = system_now_ms();
    for (auto it = queue_.begin(); it != queue_.end(); ) {
      if (lifespan_expired(it->lifespan_ms, it->info.pub_time_ms, now)) {
        it = queue_.erase(it);
      } else {
        ++it;
      }
    }
  }
};

class ParticipantImpl : public Participant
{
  friend class WriterImpl;

public:
  explicit ParticipantImpl(const ParticipantConfig & config)
  : config_(config)
  {
    prefix_ = generate_participant_prefix();
    guid_ = make_guid(prefix_, EntityKind::PARTICIPANT, 0);
  }

  ~ParticipantImpl() override
  {
    running_ = false;
    if (announce_thread_.joinable()) {
      announce_cv_.notify_all();
      announce_thread_.join();
    }
    for (auto & t : transports_) {
      if (t) {
        t->stop();
      }
    }
  }

  bool init()
  {
    TransportConfig tc;
    tc.domain_id = config_.domain_id;
    tc.udp_base_port = config_.udp_base_port;
    tc.udp_port_count = config_.udp_port_count;
    tc.udp_announce_ms = config_.udp_announce_ms;

    if (config_.use_udp_loopback) {
      start_transport(make_udp_loopback_transport(), tc);
    }
#ifdef MDDS_WITH_DSOFTBUS
    if (config_.use_dsoftbus) {
      auto t = make_dsoftbus_transport();
      if (t) {
        start_transport(std::move(t), tc);
      }
    }
#endif
    running_ = true;
    announce_thread_ = std::thread([this] {announce_loop();});
    return true;
  }

  Guid guid() const override {return guid_;}

  Writer * create_writer(
    const std::string & topic, const std::string & type_name, const QosProfile & qos) override
  {
    if (!qos_valid(qos)) {
      return nullptr;  // fail fast: never announce nonsense onto the wire
    }
    auto w = std::make_unique<WriterImpl>(topic, type_name, qos);
    w->guid_ = make_guid(prefix_, EntityKind::WRITER, ++entity_serial_);
    w->owner_ = this;
    WriterImpl * raw = w.get();
    {
      std::lock_guard<std::mutex> lock(mutex_);
      writers_.emplace(raw, std::move(w));
    }
    announce_now();
    recompute_matches();
    return raw;
  }

  Reader * create_reader(
    const std::string & topic, const std::string & type_name, const QosProfile & qos,
    bool ignore_local) override
  {
    if (!qos_valid(qos)) {
      return nullptr;  // fail fast, mirrors create_writer
    }
    auto r = std::make_unique<ReaderImpl>(topic, type_name, qos, ignore_local);
    r->guid_ = make_guid(prefix_, EntityKind::READER, ++entity_serial_);
    r->owner_ = this;
    ReaderImpl * raw = r.get();
    {
      std::lock_guard<std::mutex> lock(mutex_);
      readers_.emplace(raw, std::move(r));
    }
    announce_now();
    recompute_matches();
    replay_local_history(raw);
    return raw;
  }

  void destroy_writer(Writer * writer) override
  {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      writers_.erase(static_cast<WriterImpl *>(writer));
    }
    announce_now();
    recompute_matches();
  }

  void destroy_reader(Reader * reader) override
  {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      readers_.erase(static_cast<ReaderImpl *>(reader));
    }
    announce_now();
    recompute_matches();
  }

  void add_node(const std::string & ns, const std::string & name) override
  {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      NodeInfo n{ns, name};
      if (std::find(nodes_.begin(), nodes_.end(), n) == nodes_.end()) {
        nodes_.push_back(n);
      }
    }
    announce_now();
  }

  void remove_node(const std::string & ns, const std::string & name) override
  {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      nodes_.erase(
        std::remove(nodes_.begin(), nodes_.end(), NodeInfo{ns, name}), nodes_.end());
    }
    announce_now();
  }

  std::vector<ParticipantSnapshot> remote_participants() const override
  {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<ParticipantSnapshot> out;
    out.reserve(remote_.size());
    for (const auto & kv : remote_) {
      out.push_back(kv.second.snap);
    }
    return out;
  }

  bool remote_writer_last_seen_ms(const Guid & writer, uint64_t & out_ms) const override
  {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto it = writer_last_seen_.find(writer);
    if (it == writer_last_seen_.end()) {
      return false;
    }
    out_ms = it->second;
    return true;
  }

  bool remote_participant_last_seen_ms(const Guid & participant, uint64_t & out_ms) const override
  {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto it = remote_.find(participant);
    if (it == remote_.end() || it->second.last_announce_ms == 0) {
      return false;
    }
    out_ms = it->second.last_announce_ms;
    return true;
  }

  void set_graph_callback(std::function<void()> cb) override
  {
    std::lock_guard<std::mutex> lock(mutex_);
    graph_callback_ = std::move(cb);
  }

  void announce_now() override
  {
    announce_cv_.notify_all();
  }

  // ---- writer send path ----
  bool write_sample(WriterImpl * w, const uint8_t * data, size_t len)
  {
    const uint64_t pub_time_ms = system_now_ms();

    std::vector<PeerId> targets;
    std::vector<ReaderImpl *> local_readers;
    uint64_t seq = 0;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      // Claim the sequence number under the lock so a rejected cache write
      // (KEEP_ALL backpressure) does not burn a seq and open a hole.
      seq = w->seq_.load() + 1;
      if (!w->cache_sample(seq, pub_time_ms, data, len)) {
        return false;  // KEEP_ALL history full: rejected, nothing sent
      }
      w->seq_.store(seq);
      targets = send_targets_locked(*w);
      // Intra-participant delivery: matching local readers get the sample
      // straight from the writer history, no wire hop.
      for (auto & kv : readers_) {
        ReaderImpl * r = kv.first;
        if (r->ignore_local_) {
          continue;
        }
        if (r->topic_ == w->topic_ && r->type_ == w->type_ &&
          qos_accepted_by_reader(r->qos_, w->qos_))
        {
          local_readers.push_back(r);
        }
      }
    }

    for (ReaderImpl * r : local_readers) {
      bool gap = false;
      uint64_t gap_base = 0;
      uint64_t gap_bitmap = 0;
      if (r->enqueue(w->guid_, seq, data, len, w->qos_, pub_time_ms, gap, gap_base, gap_bitmap)) {
        r->fire_callback();
      }
    }

    const uint16_t flags =
      w->qos_.reliability == Reliability::RELIABLE ? kFlagReliable : 0;
    bool any = false;
    for (PeerId peer : targets) {
      any |= send_sample_to(peer, w->guid_, seq, pub_time_ms, data, len, flags);
    }
    return any || targets.empty();  // no matched peers is not a failure
  }

private:
  // One relay per transport: tags the backend PeerId with the transport index.
  struct ListenerRelay : public TransportListener
  {
    ParticipantImpl * owner = nullptr;
    size_t transport_index = 0;
    void on_peer_up(PeerId p) override {owner->handle_peer_up(make_peer_id(transport_index, p));}
    void on_peer_down(PeerId p) override {owner->handle_peer_down(make_peer_id(transport_index, p));}
    void on_bytes(PeerId p, const uint8_t * d, size_t n) override
    {
      owner->handle_bytes(make_peer_id(transport_index, p), d, n);
    }
  };

  void start_transport(std::unique_ptr<Transport> t, const TransportConfig & tc)
  {
    auto relay = std::make_unique<ListenerRelay>();
    relay->owner = this;
    relay->transport_index = transports_.size();
    if (t->start(tc, relay.get())) {
      relays_.push_back(std::move(relay));
      transports_.push_back(std::move(t));
    }
  }
  std::vector<std::unique_ptr<ListenerRelay>> relays_;

public:
  void handle_peer_up(PeerId peer)
  {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      peers_.insert(peer);
    }
    send_announce(peer);
  }

  void handle_peer_down(PeerId peer)
  {
    bool graph_changed = false;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      peers_.erase(peer);
      reassemblers_.erase(peer);
      for (auto it = remote_.begin(); it != remote_.end(); ) {
        if (it->second.peer == peer) {
          for (const auto & ep : it->second.snap.endpoints) {
            reader_join_seq_.erase(ep.guid);
          }
          it = remote_.erase(it);
          graph_changed = true;
        } else {
          ++it;
        }
      }
    }
    if (graph_changed) {
      recompute_matches();
      fire_graph_callback();
    }
  }

  void handle_bytes(PeerId peer, const uint8_t * data, size_t len)
  {
    FrameHeader hdr;
    if (!decode_header(data, len, hdr)) {
      return;
    }
    switch (hdr.type) {
      case FrameType::ANNOUNCE:
        handle_announce(peer, data, len);
        break;
      case FrameType::DATA: {
        DataBody body;
        if (decode_data(data, len, body)) {
          deliver_sample(
            peer, body.writer, body.seq, body.pub_time_ms, body.payload, body.payload_len);
        }
        break;
      }
      case FrameType::DATA_FRAG: {
        DataFragBody body;
        if (!decode_data_frag(data, len, body)) {
          break;
        }
        std::vector<uint8_t> full;
        bool complete = false;
        {
          std::lock_guard<std::mutex> lock(mutex_);
          complete = reassemblers_[peer].add_fragment(body, full);
        }
        if (complete) {
          deliver_sample(peer, body.writer, body.seq, body.pub_time_ms, full.data(), full.size());
        }
        break;
      }
      case FrameType::ACKNACK:
        handle_acknack(peer, data, len);
        break;
      case FrameType::HEARTBEAT:
        handle_heartbeat(peer, data, len);
        break;
      case FrameType::GAP:
        handle_gap(peer, data, len);
        break;
    }
  }

private:
  void handle_announce(PeerId peer, const uint8_t * data, size_t len)
  {
    ParticipantSnapshot snap;
    uint64_t seq = 0;
    if (!decode_announce(data, len, snap, seq)) {
      return;
    }
    bool changed = false;
    std::vector<EndpointRecord> new_readers;  // reader endpoints not seen before
    {
      std::lock_guard<std::mutex> lock(mutex_);
      auto it = remote_.find(snap.participant_guid);
      if (it != remote_.end()) {
        // Any announce (even a stale/duplicate one) proves AUTOMATIC liveliness.
        it->second.last_announce_ms = steady_now_ms();
        if (it->second.announce_seq >= seq) {
          return;  // stale or duplicate announce (dedup; also breaks gateway loops)
        }
      }
      if (it == remote_.end() ||
        !(it->second.snap.nodes == snap.nodes && it->second.snap.endpoints == snap.endpoints))
      {
        changed = true;
      }
      // Collect newly appeared readers for transient_local history replay.
      for (const auto & ep : snap.endpoints) {
        if (ep.kind != EntityKind::READER) {
          continue;
        }
        bool known = false;
        if (it != remote_.end()) {
          for (const auto & old : it->second.snap.endpoints) {
            if (old.guid == ep.guid) {
              known = true;
              break;
            }
          }
        }
        if (!known) {
          new_readers.push_back(ep);
        }
      }
      // Join watermark: for each newly seen reader, remember the current high
      // watermark of matching local writers. Volatile readers must never pull
      // samples that predate their join (see handle_acknack clamp).
      for (const auto & ep : new_readers) {
        for (auto & kv : writers_) {
          WriterImpl * w = kv.first;
          if (ep.topic == w->topic_ && ep.type_name == w->type_ &&
            qos_accepted_by_reader(ep.qos, w->qos_))
          {
            reader_join_seq_[ep.guid][w->guid_] = w->seq_.load();
          }
        }
      }
      RemoteParticipant rp;
      rp.peer = peer;
      rp.announce_seq = seq;
      rp.snap = snap;
      rp.last_announce_ms = steady_now_ms();
      remote_[snap.participant_guid] = std::move(rp);
    }
    replay_history_to_peer(peer, new_readers);
    if (changed) {
      recompute_matches();
      fire_graph_callback();
    }
  }

  void handle_acknack(PeerId peer, const uint8_t * data, size_t len)
  {
    AckNackBody body;
    if (!decode_acknack(data, len, body)) {
      return;
    }
    // Collect retransmits under lock, send after unlocking.
    std::vector<WriterImpl::CachedSample> resend;
    Guid writer_guid = body.writer;
    uint16_t flags = 0;
    uint64_t gap_end = 0;  // highest requested seq already evicted from history
    {
      std::lock_guard<std::mutex> lock(mutex_);
      WriterImpl * w = find_writer_locked(writer_guid);
      if (w == nullptr) {
        return;
      }
      flags = w->qos_.reliability == Reliability::RELIABLE ? kFlagReliable : 0;
      // Volatile readers must not pull history from before their join: clamp
      // resends to seqs above the recorded join watermark.
      uint64_t join_floor = 0;
      const EndpointRecord * reader_ep = find_remote_endpoint_locked(body.reader);
      if (reader_ep != nullptr && reader_ep->qos.durability == Durability::VOLATILE) {
        auto rit = reader_join_seq_.find(body.reader);
        if (rit != reader_join_seq_.end()) {
          auto wit = rit->second.find(writer_guid);
          if (wit != rit->second.end()) {
            join_floor = wit->second;
          }
        }
      }
      // The history is contiguous: it holds [first_avail, latest published].
      const uint64_t first_avail =
        w->history_.empty() ? 0 : w->history_.front().seq;
      for (uint64_t i = 0; i < 64; ++i) {
        const uint64_t seq = body.base_seq + i;
        if (body.bitmap & (1ULL << i) || seq <= join_floor) {
          continue;  // already received, or predates the volatile reader's join
        }
        bool found = false;
        for (const auto & entry : w->history_) {
          if (entry.seq == seq) {
            resend.push_back(entry);
            found = true;
            break;
          }
        }
        if (!found && first_avail > 0 && seq < first_avail) {
          gap_end = seq;  // evicted: will never be retransmitted, tell the reader
        }
      }
    }
    for (const auto & entry : resend) {
      send_sample_to(
        peer, writer_guid, entry.seq, entry.pub_time_ms,
        entry.data.data(), entry.data.size(), flags);
    }
    if (gap_end > 0) {
      // GAP terminates the NACK loop for evicted seqs: the reader advances
      // its baseline and counts them lost exactly once.
      auto frame = encode_gap(writer_guid, body.base_seq, gap_end);
      send_frame(peer, frame.data(), frame.size());
    }
  }

  /// GAP from a remote writer: [gap_start, gap_end] was evicted and will
  /// never be retransmitted. Applied to every local reader that has state
  /// for the writer (the endpoint match was validated when data was first
  /// enqueued, so this stays correct while discovery is churning).
  void handle_gap(PeerId peer, const uint8_t * data, size_t len)
  {
    static_cast<void>(peer);
    GapBody body;
    if (!decode_gap(data, len, body) || body.gap_end < body.gap_start) {
      return;
    }
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto & kv : readers_) {
      kv.first->apply_gap(body.writer, body.gap_start, body.gap_end);
    }
  }

  void handle_heartbeat(PeerId peer, const uint8_t * data, size_t len)
  {
    HeartbeatBody body;
    if (!decode_heartbeat(data, len, body)) {
      return;
    }
    struct Nack
    {
      Guid reader;
      uint64_t base;
      uint64_t bitmap;
    };
    // Any matched reliable reader that is behind sends a precise ACKNACK.
    std::vector<Nack> nacks;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      // A heartbeat refreshes the writer's liveliness (MANUAL_BY_TOPIC assert
      // also travels as a HEARTBEAT frame).
      writer_last_seen_[body.writer] = steady_now_ms();
      const EndpointRecord * writer_ep = find_remote_endpoint_locked(body.writer);
      if (writer_ep == nullptr) {
        return;
      }
      for (auto & kv : readers_) {
        ReaderImpl * r = kv.first;
        if (r->qos_.reliability != Reliability::RELIABLE) {
          continue;  // best-effort readers never request retransmission
        }
        if (!endpoints_match(r->topic_, r->type_, r->qos_, *writer_ep)) {
          continue;
        }
        const auto st = r->nack_state(body.writer);  // (base, bitmap)
        if (st.first <= body.last_seq) {
          nacks.push_back(Nack{r->guid_, st.first, st.second});
        }
      }
    }
    for (const auto & nk : nacks) {
      auto frame = encode_acknack(nk.reader, body.writer, nk.base, nk.bitmap);
      send_frame(peer, frame.data(), frame.size());
    }
  }

  void deliver_sample(
    PeerId peer, const Guid & writer_guid, uint64_t seq, uint64_t pub_time_ms,
    const uint8_t * data, size_t len)
  {
    struct NackRequest
    {
      Guid reader;
      uint64_t base;
      uint64_t bitmap;
    };
    std::vector<ReaderImpl *> to_notify;
    std::vector<NackRequest> gaps;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      // Received data refreshes the writer's liveliness regardless of matching.
      writer_last_seen_[writer_guid] = steady_now_ms();
      const EndpointRecord * writer_ep = find_remote_endpoint_locked(writer_guid);
      if (writer_ep == nullptr) {
        return;  // data from a writer we have not discovered yet; drop
      }
      for (auto & kv : readers_) {
        ReaderImpl * r = kv.first;
        if (!endpoints_match(r->topic_, r->type_, r->qos_, *writer_ep)) {
          continue;
        }
        bool gap = false;
        uint64_t gap_base = 0;
        uint64_t gap_bitmap = 0;
        if (r->enqueue(
            writer_guid, seq, data, len, writer_ep->qos, pub_time_ms,
            gap, gap_base, gap_bitmap))
        {
          to_notify.push_back(r);
        }
        if (gap) {
          gaps.push_back(NackRequest{r->guid_, gap_base, gap_bitmap});
        }
      }
    }
    for (const auto & g : gaps) {
      auto frame = encode_acknack(g.reader, writer_guid, g.base, g.bitmap);
      send_frame(peer, frame.data(), frame.size());
    }
    for (ReaderImpl * r : to_notify) {
      r->fire_callback();
    }
  }

  // ---- helpers ----

  /// Late-joining local reader on a transient_local writer: replay the
  /// writer's cached history straight into the reader queue. Only readers
  /// requesting TRANSIENT_LOCAL get historical data (DDS durability RxO);
  /// the reader's per-writer seq dedup makes overlap with the live stream
  /// harmless.
  void replay_local_history(ReaderImpl * r)
  {
    if (r->qos_.durability != Durability::TRANSIENT_LOCAL || r->ignore_local_) {
      return;
    }
    bool any = false;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      const uint64_t now = system_now_ms();
      for (auto & kv : writers_) {
        WriterImpl * w = kv.first;
        if (w->qos_.durability != Durability::TRANSIENT_LOCAL ||
          w->topic_ != r->topic_ || w->type_ != r->type_ ||
          !qos_accepted_by_reader(r->qos_, w->qos_))
        {
          continue;
        }
        for (const auto & e : w->history_) {
          if (lifespan_expired(w->qos_.lifespan_ms, e.pub_time_ms, now)) {
            continue;
          }
          bool gap = false;
          uint64_t gap_base = 0;
          uint64_t gap_bitmap = 0;
          any |= r->enqueue(
            w->guid_, e.seq, e.data.data(), e.data.size(), w->qos_, e.pub_time_ms,
            gap, gap_base, gap_bitmap);
        }
      }
    }
    if (any) {
      r->fire_callback();
    }
  }

  /// Newly discovered remote readers on local transient_local writers get the
  /// cached history sent to their peer (same RxO rule as the local path).
  void replay_history_to_peer(PeerId peer, const std::vector<EndpointRecord> & new_readers)
  {
    if (new_readers.empty()) {
      return;
    }
    std::vector<WriterImpl::CachedSample> todo;
    std::vector<Guid> todo_writer;
    std::vector<uint16_t> todo_flags;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      const uint64_t now = system_now_ms();
      for (const auto & ep : new_readers) {
        if (ep.qos.durability != Durability::TRANSIENT_LOCAL) {
          continue;  // volatile readers only receive new data
        }
        for (auto & kv : writers_) {
          WriterImpl * w = kv.first;
          if (w->qos_.durability != Durability::TRANSIENT_LOCAL ||
            ep.topic != w->topic_ || ep.type_name != w->type_ ||
            !qos_accepted_by_reader(ep.qos, w->qos_))
          {
            continue;
          }
          for (const auto & e : w->history_) {
            if (!lifespan_expired(w->qos_.lifespan_ms, e.pub_time_ms, now)) {
              todo.push_back(e);
              todo_writer.push_back(w->guid_);
              todo_flags.push_back(
                w->qos_.reliability == Reliability::RELIABLE ? kFlagReliable : 0);
            }
          }
        }
      }
    }
    for (size_t i = 0; i < todo.size(); ++i) {
      send_sample_to(
        peer, todo_writer[i], todo[i].seq, todo[i].pub_time_ms,
        todo[i].data.data(), todo[i].data.size(), todo_flags[i]);
    }
  }

  static bool endpoints_match(
    const std::string & reader_topic, const std::string & reader_type,
    const QosProfile & reader_qos, const EndpointRecord & writer_ep)
  {
    return writer_ep.topic == reader_topic && writer_ep.type_name == reader_type &&
           qos_accepted_by_reader(reader_qos, writer_ep.qos);
  }

  /// Peers that host at least one remote reader matching this writer.
  /// Caller must hold mutex_.
  std::vector<PeerId> send_targets_locked(const WriterImpl & w)
  {
    std::set<PeerId> out;
    for (const auto & kv : remote_) {
      for (const auto & ep : kv.second.snap.endpoints) {
        if (ep.kind != EntityKind::READER) {
          continue;
        }
        if (ep.topic == w.topic_ && ep.type_name == w.type_ &&
          qos_accepted_by_reader(ep.qos, w.qos_))
        {
          out.insert(kv.second.peer);
          break;
        }
      }
    }
    return {out.begin(), out.end()};
  }

  bool send_sample_to(
    PeerId peer, const Guid & writer_guid, uint64_t seq, uint64_t pub_time_ms,
    const uint8_t * data, size_t len, uint16_t flags = 0)
  {
    const size_t max_p = max_payload(peer);
    if (max_p == 0) {
      return false;
    }
    auto frames = Fragmenter::fragment(
      writer_guid, seq, pub_time_ms, data, static_cast<uint32_t>(len), max_p, flags);
    bool ok = true;
    for (const auto & f : frames) {
      ok = send_frame(peer, f.data(), f.size()) && ok;
    }
    return ok && !frames.empty();
  }

  bool send_frame(PeerId peer, const uint8_t * data, size_t len)
  {
    if (config_.test_send_hook && config_.test_send_hook(data, len)) {
      return true;  // test-simulated loss: invisible to the sender
    }
    const size_t idx = peer_transport_index(peer);
    if (idx >= transports_.size() || !transports_[idx]) {
      return false;
    }
    return transports_[idx]->send(peer_backend_id(peer), data, len);
  }

  size_t max_payload(PeerId peer) const
  {
    const size_t idx = peer_transport_index(peer);
    if (idx >= transports_.size() || !transports_[idx]) {
      return 0;
    }
    return transports_[idx]->max_payload(peer_backend_id(peer));
  }

  void send_announce(PeerId peer)
  {
    ParticipantSnapshot snap;
    uint64_t seq;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      snap = snapshot_locked();
      seq = announce_seq_;
    }
    auto frame = encode_announce(snap, seq);
    send_frame(peer, frame.data(), frame.size());
  }

  void broadcast_announce()
  {
    std::set<PeerId> peers;
    ParticipantSnapshot snap;
    uint64_t seq;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      snap = snapshot_locked();
      seq = announce_seq_;
      peers = peers_;
    }
    auto frame = encode_announce(snap, seq);
    for (PeerId p : peers) {
      send_frame(p, frame.data(), frame.size());
    }
  }

  /// Caller must hold mutex_.
  ParticipantSnapshot snapshot_locked()
  {
    ParticipantSnapshot snap;
    snap.participant_guid = guid_;
    snap.nodes = nodes_;
    for (const auto & kv : writers_) {
      snap.endpoints.push_back(
        EndpointRecord{kv.first->guid_, EntityKind::WRITER, kv.first->topic_,
          kv.first->type_, kv.first->qos_});
    }
    for (const auto & kv : readers_) {
      snap.endpoints.push_back(
        EndpointRecord{kv.first->guid_, EntityKind::READER, kv.first->topic_,
          kv.first->type_, kv.first->qos_});
    }
    return snap;
  }

  /// Caller must hold mutex_.
  WriterImpl * find_writer_locked(const Guid & g)
  {
    for (auto & kv : writers_) {
      if (kv.first->guid_ == g) {
        return kv.first;
      }
    }
    return nullptr;
  }

  /// Caller must hold mutex_.
  const EndpointRecord * find_remote_endpoint_locked(const Guid & g)
  {
    for (const auto & kv : remote_) {
      for (const auto & ep : kv.second.snap.endpoints) {
        if (ep.guid == g) {
          return &ep;
        }
      }
    }
    return nullptr;
  }

  void recompute_matches()
  {
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto & kv : writers_) {
      WriterImpl * w = kv.first;
      size_t count = 0;
      for (const auto & rp : remote_) {
        for (const auto & ep : rp.second.snap.endpoints) {
          if (ep.kind == EntityKind::READER && ep.topic == w->topic_ &&
            ep.type_name == w->type_ && qos_compatible(ep.qos, w->qos_))
          {
            ++count;
          }
        }
      }
      for (auto & rk : readers_) {
        ReaderImpl * r = rk.first;
        if (r->topic_ == w->topic_ && r->type_ == w->type_ &&
          qos_compatible(r->qos_, w->qos_))
        {
          ++count;
        }
      }
      w->matched_count_ = count;
    }
    for (auto & kv : readers_) {
      ReaderImpl * r = kv.first;
      size_t count = 0;
      for (const auto & rp : remote_) {
        for (const auto & ep : rp.second.snap.endpoints) {
          if (ep.kind == EntityKind::WRITER &&
            endpoints_match(r->topic_, r->type_, r->qos_, ep))
          {
            ++count;
          }
        }
      }
      for (auto & wk : writers_) {
        WriterImpl * w = wk.first;
        if (w->topic_ == r->topic_ && w->type_ == r->type_ &&
          qos_accepted_by_reader(r->qos_, w->qos_))
        {
          ++count;
        }
      }
      r->matched_count_ = count;
    }
  }

  void fire_graph_callback()
  {
    std::function<void()> cb;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      cb = graph_callback_;
    }
    if (cb) {
      cb();
    }
  }

  /// Drop stale partial fragmented samples (default 5 s per Reassembler).
  /// Called every announce tick so partials from a silent-but-not-yet-down
  /// peer do not accumulate. Caller must hold mutex_.
  void sweep_reassemblers_locked()
  {
    for (auto & kv : reassemblers_) {
      kv.second.sweep_expired();
    }
  }

  void announce_loop()
  {
    while (running_) {
      {
        std::lock_guard<std::mutex> lock(mutex_);
        ++announce_seq_;
        sweep_reassemblers_locked();
      }
      broadcast_announce();
      send_heartbeats();

      std::unique_lock<std::mutex> lock(announce_mutex_);
      const auto period = std::chrono::milliseconds(config_.announce_period_ms);
      announce_cv_.wait_for(lock, period, [this] {return !running_.load();});
    }
  }

  void send_heartbeats()
  {
    struct Hb
    {
      Guid writer;
      uint64_t first;
      uint64_t last;
    };
    std::set<PeerId> peers;
    std::vector<Hb> hbs;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      peers = peers_;
      for (auto & kv : writers_) {
        WriterImpl * w = kv.first;
        if (w->qos_.reliability != Reliability::RELIABLE || w->history_.empty()) {
          continue;
        }
        hbs.push_back(Hb{w->guid_, w->history_.front().seq, w->seq_.load()});
      }
    }
    for (PeerId p : peers) {
      for (const auto & hb : hbs) {
        auto frame = encode_heartbeat(hb.writer, hb.first, hb.last);
        send_frame(p, frame.data(), frame.size());
      }
    }
  }

  /// Liveliness assertion: broadcast an immediate HEARTBEAT for this writer
  /// to every peer (also covers writers with an empty history).
  void broadcast_writer_heartbeat(WriterImpl * w)
  {
    std::set<PeerId> peers;
    uint64_t first = 0;
    uint64_t last = 0;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      peers = peers_;
      last = w->seq_.load();
      first = w->history_.empty() ? last : w->history_.front().seq;
    }
    auto frame = encode_heartbeat(w->guid_, first, last);
    for (PeerId p : peers) {
      send_frame(p, frame.data(), frame.size());
    }
  }

  struct RemoteParticipant
  {
    PeerId peer;
    uint64_t announce_seq = 0;
    ParticipantSnapshot snap;
    /// steady_clock ms of the most recent ANNOUNCE from this participant
    /// (AUTOMATIC liveliness source).
    uint64_t last_announce_ms = 0;
  };

  ParticipantConfig config_;
  std::array<uint8_t, 12> prefix_{};
  Guid guid_;
  std::atomic<uint32_t> entity_serial_{0};

  std::vector<std::unique_ptr<Transport>> transports_;

  mutable std::mutex mutex_;
  std::map<WriterImpl *, std::unique_ptr<WriterImpl>> writers_;
  std::map<ReaderImpl *, std::unique_ptr<ReaderImpl>> readers_;
  std::vector<NodeInfo> nodes_;
  std::set<PeerId> peers_;
  std::map<Guid, RemoteParticipant> remote_;
  /// steady_clock ms of the last DATA/HEARTBEAT received per remote writer
  /// (MANUAL_BY_TOPIC liveliness source).
  std::map<Guid, uint64_t> writer_last_seen_;
  // reader guid -> (writer guid -> writer high watermark when the reader was
  // first announced); used to keep volatile readers from pulling pre-join
  // history via ACKNACK.
  std::map<Guid, std::map<Guid, uint64_t>> reader_join_seq_;
  std::map<PeerId, Reassembler> reassemblers_;
  std::function<void()> graph_callback_;
  uint64_t announce_seq_ = 0;

  std::atomic<bool> running_{false};
  std::thread announce_thread_;
  std::mutex announce_mutex_;
  std::condition_variable announce_cv_;
};

bool WriterImpl::write(const uint8_t * data, size_t len)
{
  if (owner_ == nullptr || (data == nullptr && len > 0)) {
    return false;
  }
  return owner_->write_sample(this, data, len);
}

void WriterImpl::assert_liveliness()
{
  if (owner_ != nullptr) {
    owner_->broadcast_writer_heartbeat(this);
  }
}

}  // namespace

std::unique_ptr<Participant> Participant::create(const ParticipantConfig & config)
{
  auto impl = std::make_unique<ParticipantImpl>(config);
  if (!impl->init()) {
    return nullptr;
  }
  return impl;
}

}  // namespace mdds
