# mdds 设计文档

> 状态：v3（阶段四：精确 NACK/GAP 可靠性补全 + 发送 lane + QoS 预设；
> 本版已逐条核对 src/ 现行实现并修正）
>
> mdds 是以 OpenHarmony DSoftBus 为跨设备传输基座的 ROS 2 中间件传输层。
> DSoftBus 仅作为**透明 Bytes 字节管道**（Socket API，`DATA_TYPE_BYTES`），
> QoS、分帧、分片、发现全部在 mdds 层自行实现，对 DSoftBus 透明。

## 1. 分层架构

```
rclcpp / rclpy
    │  RMW API (rmw.h)
rmw_mdds            ros2/src/ros2/rmw_mdds   — RMW 绑定、CDR 序列化、graph/wait set
    │  mdds C++ API
mdds core           ros2/src/ros2/mdds       — 平台无关：帧编解码、分片重组、发现、
    │                                           QoS（reliability/history）、端点管理
    │  transport 抽象接口 (mdds::Transport)
    ├─ transport_dsoftbus   跨设备数据面（仅 OHOS，DSoftBus Socket Bytes）
    └─ transport_udp        UDP loopback：主机单元测试 + 同机多进程互通
```

关键约束（来自 DSoftBus 源码事实）：

- 一次 `SendBytes` 对应对端一次完整 `OnBytes`，管道保序、可靠、加密——mdds 不做 CRC/重排序。
- 单包上限：TCP Direct 默认 4 MB，Proxy/BR 通道 4 KB 级 → mdds 必须自分片。
  dsoftbus 后端在通道建立时用 `GetSessionOption(SESSION_OPTION_MAX_SENDBYTES_SIZE)`
  探测该通道的真实上限作为 `max_payload()`（分片粒度跟随链路能力）；探测失败回退
  保守值 `kDefaultMaxPayload = 3800`。
- 每进程 socket 上限 15 个 → 每 peer 一条全双工 socket，v1 支持的对端数受限（记录为已知限制）。
- 无多播 → 发现依赖 LNN 组网枚举 + 逐 peer 连接。
- DSoftBus 只在 OpenHarmony 运行 → 非 OHOS 设备互通由 mdds_gateway（阶段三）解决。

## 2. 身份与编址

- **GUID**：16 字节 = 12 字节 participant 前缀（启动时随机生成）+ 4 字节 entity id
  （1 字节 kind：participant/writer/reader + 3 字节递增序号）。
  映射到 `rmw_gid_t`（24 字节数组，放 16 字节 GUID，其余清零），与 CycloneDDS 做法一致。
- **对端标识**：transport 层用 `PeerId`（dsoftbus backend 为递增序号 `next_peer_id_`，
  `peers_` map 以 networkId 字符串为 key 维护序号 ↔ socket fd 映射；udp backend 为对端
  UDP 端口号，loopback 隐含 IP）。mdds core 只对 `PeerId`（uint64 不透明句柄）编程：
  高 8 位为 transport index，低 56 位为 backend id（`participant.cpp:make_peer_id()`）。

## 3. 连接拓扑

- 每个 mdds participant 创建一个 DSoftBus socket 并 `Listen`，session name =
  `com.kaihong.mdds.d<domain_id>`（pkgName 固定 `com.kaihong.mdds`，须匹配设备
  softbus_trans_permission.json 白名单，见 §9）；同时由轮询线程（`kPeerPollMs = 2000`）
  向 LNN 组网内每个在线 peer 的同名 socket 发起 `BindAsync`（失败由下轮轮询重试）。
- 连接建立后双向收发，一 peer 一条 socket。`OnShutdown` → 视该 peer 下线，清除其全部远端端点。
- 同设备多进程：`transport_udp`（127.0.0.1）承载，避免占用 socket 配额与同机 session 不确定性。
- **发送 lane（dsoftbus 后端）**：每个已连接 peer 一条 `SendLane`（`src/send_lane.hpp`）——
  有界字节队列 + 专属 worker 线程。`Transport::send()` 只入队拷贝、立即返回，**绝不在
  DSoftBus 回调线程上做网络 IO**（OnBytes 回调里触发的 ACKNACK/重传同样只入队）。
  背压由双重字节预算承载：每 lane 8 MiB + 全 transport 共享 32 MiB；预算满则**丢新帧**并计数
  （`SendLane::dropped()`），语义等同网络丢包——reliable 配对由 ACKNACK/GAP 自愈，
  best_effort 直接丢失。lane 随通道生灭：`OnShutdown` 丢弃该 peer 残余队列；
  worker 做 `SendBytes` 前在锁内复核 (peer, fd) 仍为当前通道（pin & touch 的简化版），
  IO 本身不持锁。UDP loopback 后端保持同步 `sendto` 直发（本机内核管道，无慢对端风险）。

## 4. 帧格式 v3

所有多字节字段为网络字节序（big-endian）。一次 mdds 帧 = 一次 `SendBytes` 调用。
v3 相对 v2 的变化：DATA / DATA_FRAG body 增加显式 `payload_len`，体尾可挂 TLV 扩展
（网关防环预留 originGuid/originSeq）；flags 启用 `kFlagReliable`；新增 GAP 帧。
version 字段升为 3，v2/v3 不互通（延续 v1→v2 立场；gateway 转发无需转换，帧体原样透传）。

公共头（12 B）：

| 偏移 | 字段 | 说明 |
|---|---|---|
| 0 | char magic[4] | `"MDDS"` |
| 4 | uint8 version | = 3 |
| 5 | uint8 frame_type | 1=DATA, 2=DATA_FRAG, 3=ACKNACK, 4=HEARTBEAT, 5=ANNOUNCE, 6=GAP |
| 6 | uint16 flags | bit0 `kFlagReliable`：来自 RELIABLE writer 的 DATA/DATA_FRAG（重传与首发同置） |
| 8 | uint32 body_len | 帧头之后的 body 长度 |

DATA body（36 + N + T B）：
`writer_guid(16) | seq:uint64 | pub_time_ms:uint64 | payload_len:uint32 | payload(N) | trailer TLV(T)`。
payload 为 CDR 序列化样本；`payload_len` 把载荷与体尾 TLV 分开，解码器对未知/截断 TLV
容忍跳过（不丢 payload）。已定义 TLV（type:u8 | len:u8 | value）：
`kTlvOriginGuid=1`（16 B）、`kTlvOriginSeq=2`（8 B）——网关跨域转发时保留原始身份用，
当前无生产者/消费者（阶段五预留）。
`pub_time_ms` = writer 写出时刻的 system_clock 毫秒（跨设备时钟不严格同步，仅作 lifespan
相对时长参考；时钟偏差过大时 lifespan 判定会相应偏移——记录为已知限制）。

DATA_FRAG body（44 + N + T B）：
`writer_guid(16) | seq:uint64 | pub_time_ms:uint64 | total_size:uint32 | frag_offset:uint32 | frag_len:uint32 | frag_payload(N) | trailer TLV(T)`。
同一样本的所有分片带相同 `pub_time_ms`。同一 (writer_guid, seq) 的分片在接收方重组
（`Reassembler`，按已合并字节区间判定完成）；乱序到达允许（重传场景）。
`Reassembler::sweep_expired()`（默认 5 s 超时丢弃）由 participant 主循环每个
announce tick 周期调用（`sweep_reassemblers_locked()`）；peer 下线时仍随
`reassemblers_.erase(peer)` 整 peer 立即清除。

ACKNACK body（48 B）：
`reader_guid(16) | writer_guid(16) | base_seq:uint64 | bitmap:uint64`。
bitmap 第 i 位表示 seq = base_seq + i **已收到**（精确 NACK，v3 起生效）——writer 侧
按位扫描 [base, base+63]，只补发 bitmap 未置位的 seq，已在窗口里的乱序样本不再重发。
**无周期 ACKNACK 定时器**，仅两处触发：
reader 在数据到达时检出 reliable 配对的 seq 空洞（`deliver_sample` 立即发送），以及
收到 HEARTBEAT 发现自身落后（`handle_heartbeat`，base = 自身基线 +1，bitmap = 接收窗口快照）。

GAP body（32 B）：`writer_guid(16) | gap_start:uint64 | gap_end:uint64`（闭区间）。
writer 在 ACKNACK 请求的 seq 已从历史缓存驱逐（`seq < first_avail`）时回复 GAP，
宣告该区间**永不重传**；reader 的 `apply_gap` 把基线推过 gap_end，
窗口已收位不算丢失，其余按丢失计一次（`messages_lost`），NACK 循环就此终止。
writer 侧 volatile reader 的 join 水位线（`reader_join_seq_`）优先于 GAP 判定：
join 之前的 seq 既不重传也不 GAP（晚加入的 volatile reader 不会拉取加入前的历史）。

reader 接收窗口：每 writer 一个 64 位 bitmap 跟踪 `(last_seq, last_seq+64]` 的乱序到达；
重复样本（含 apply_gap 滑窗后迟到的重传）按位去重；offset ≥ 64 的跳变直接计丢失并跳基线
（超出窗口的洞无法 NACK 治愈）。**丢失只在 GAP 到达或窗口跳变时计数**，
检出空洞本身不计（可能被重传治愈，见 RetransmitHealsDroppedFrame 测试）。

HEARTBEAT body（32 B）：`writer_guid(16) | first_seq:uint64 | last_seq:uint64`。
周期发送与 ANNOUNCE 共用 `announce_period_ms`（默认 2000 ms），由 `announce_loop`
每轮发出（`send_heartbeats()`），仅覆盖 RELIABLE 且 history 非空的 writer
（first = history 头部 seq，last = 当前 seq）；断连重连后无单独触发，由下一轮周期帧
完成对齐。reader 据此发现空洞并 NACK。
**阶段二起 HEARTBEAT 兼作 liveliness 断言帧**：`Writer::assert_liveliness()` 立即向全部
peer 广播一帧 HEARTBEAT（history 为空时 first=last=当前 seq）；接收方用它刷新
该 writer 的 last-seen 时间戳（MANUAL_BY_TOPIC 存活性判定来源）。

ANNOUNCE body（**全量快照**，非增量；`src/discovery.cpp:encode_announce()`）：

```
participant_guid(16) | announce_seq:uint64
| node_count:uint16 | [ ns(string) + name(string) ] × node_count
| endpoint_count:uint16
| [ entity_guid(16) + kind:u8(1=writer,2=reader) + topic + type_name + qos(32 B) ] × endpoint_count
```

每次 ANNOUNCE 都携带本地全部节点与端点；没有 GONE 条目，节点/端点下线通过「从下一次
全量快照中消失」表达，接收方只保留 announce_seq 更大的快照（去重，见 §6）。

string = uint16 长度 + UTF-8 字节。qos 32 B 编码：
`reliability:u8 | durability:u8 | history:u8 | liveliness:u8 | depth:u32 | deadline_ms:u64 | lifespan_ms:u64 | liveliness_lease_ms:u64`。
阶段二起 durability/lifespan/liveliness(_lease) 全部生效（见 §5）；deadline 仍在 rmw 层
自行跟踪（mdds 不消费，但线上字段供对端展示/将来使用）。

## 5. QoS 语义（mdds 层）

阶段一（已板上验证，v3 起发送路径改道 lane，见 §3）：

- **best_effort**：不缓存历史、不重传；dsoftbus 后端经 send lane 异步发送
  （UDP loopback 仍同步 `sendto` 直发）；lane 预算满或通道失败即丢（丢新帧），
  对 `write()` 表现为该 peer 发送失败/入队失败。
- **reliable + keep_last(depth)**：
  - writer 历史缓存保留最近 depth 条样本；**KEEP_ALL 时 cap 为 `kKeepAllCap = 256`，
    缓存满时 `write()` 直接返回 false**（KEEP_ALL 语义不可协商，不静默驱逐最旧样本；
    拒绝发生在 seq 分配之前，不产生 seq 空洞）。
  - 丢失自愈闭环：reader 检出 seq 空洞 → 精确 ACKNACK（base + 64 位接收窗口 bitmap）
    → writer 只补发窗口未收到的 seq；已驱逐的 seq 回 GAP 终止 NACK 循环并计丢失
    （详见 §4 ACKNACK/GAP 段）。断连重连后由周期 HEARTBEAT 驱动同一闭环对齐基线。
  - reader 按 writer 维序：基线之前的重复样本丢弃（去重，gateway 防环也依赖此），
    乱序样本先入队、用窗口位跟踪，重传/滑窗导致的重复按位去重。
- **QoS 兼容性**：`qos_compatible` 为严格 RxO（writer 侧计数/判兼容用）；
  `qos_accepted_by_reader` 是 reader 侧放宽版（reliability 放宽、durability 严格）——
  rmw 测试契约要求这个非对称，不可改动。

预设与入口校验（v3 起）：

- `QosProfile::preset_default()/preset_best_effort()/preset_sensor_data()/
  preset_transient_local()/preset_bulk_data()` 对应 ROS 2 rmw 预设语义
  （sensor_data = BEST_EFFORT + depth 5 + deadline 100 ms；bulk_data = KEEP_ALL）。
- `qos_valid()`：枚举值域检查 + KEEP_LAST 要求 depth ≥ 1；
  `create_writer/create_reader` 入口校验，非法直接返回 nullptr（fail fast，
  不让非法 QoS 上线）。rmw 层已在 `rmw_qos_to_mdds` 归一化 SYSTEM_DEFAULT/depth 0，
  不受影响。

阶段二（本文档 v2 起落地）：

- **transient_local**：writer 历史缓存对 reliable **或** transient_local 都生效（同一
  depth/keep_all cap）。新匹配出现时补发历史——本地：`create_reader` 时把匹配的本地
  transient_local writer 缓存直接 enqueue 进新 reader；远端：`handle_announce` 发现新增
  reader 端点（对比新旧 snapshot）后把缓存样本发给该 peer。两条路径都只对
  **请求 TRANSIENT_LOCAL 的 reader** 补发（durability RxO：volatile reader 只收新数据），
  reader 端 seq 去重天然防重（补发与实时流交叠无副作用）。
- **lifespan（writer QoS）**：样本带 `pub_time_ms` 上线；reader enqueue 时已过期的样本
  丢弃不入队（seq 基线照常推进，避免 reliable 配对对过期样本无限 NACK）；reader 队列在
  take/available 时惰性清除过期样本；writer 历史在缓存新样本时顺带清头部过期项。
  补发路径同样跳过过期样本。
- **liveliness 观测**（引擎只提供观测，事件合成在 rmw 层）：
  - AUTOMATIC：`Participant::remote_participant_last_seen_ms()`——每个 remote participant
    最近一次收到其 ANNOUNCE 的时刻（重复/过期 ANNOUNCE 也刷新）。
  - MANUAL_BY_TOPIC：`Participant::remote_writer_last_seen_ms()`——每个 remote writer
    最近一次收到其 DATA/HEARTBEAT 的时刻；`Writer::assert_liveliness()` 立即广播
    HEARTBEAT。从发现起从未收到过该 writer 任何帧时视为存活（lease 从首次接触起算）。
- **message lost**：reader 对任意 reliability 的 seq 空洞累计计数
  （`Reader::messages_lost()`），供 rmw 层合成 RMW_EVENT_MESSAGE_LOST。

## 6. 发现协议

- participant 上线：transport 枚举 peers、建连后**双方互发全量 ANNOUNCE**（announce_seq 单调递增）。
- 端点/节点增删：`announce_now()` 唤醒 announce 线程，向全部 peers 立即发**全量**
  ANNOUNCE（无增量条目，从快照中消失即表示下线）。
- 周期全量 ANNOUNCE（默认 2 s，`ParticipantConfig::announce_period_ms = 2000`，兼作
  reliable writer 的 HEARTBEAT 周期）兜底丢包；收到 announce_seq ≤ 已见值则按去重处理
  （网关场景必需），但丢弃前仍刷新该 participant 的 AUTOMATIC liveliness 时间戳。
- peer 下线（`OnShutdown` / UDP presence 超时 4×周期，`kPeerTimeoutFactor = 4`）：清除
  其全部远端端点、join 水位线与分片重组器，触发 graph 变更。

匹配规则：本地 reader × 远端 writer（及反向）按 topic + type_name 完全匹配；
匹配变化 → 回调 rmw 层触发 graph guard condition。注意 mdds 的 graph 回调
（`set_graph_callback`）仅在**远端**变化时触发（`handle_announce` 检出快照实质变化、
`handle_peer_down`）；本地端点/节点增删不触发。

## 7. rmw_mdds 映射要点

- `rmw_init` → 创建 mdds participant（domain_id 取自 `rmw_init_options_t`）。
- `rmw_create_node` → bookkeeping + 节点进入 ANNOUNCE 快照 nodes 列表（端点挂在
  participant 上，同 CycloneDDS 模型）。
- `rmw_create_publisher` → mdds writer（进入 ANNOUNCE 快照 endpoints 列表）；
  `rmw_publish` → introspection
  typesupport 把消息序列化为 CDR（参照 rmw_cyclonedds 的 serdes）→ `mdds_write`。
- `rmw_create_subscription` → mdds reader；DATA 到达 → 入 reader 接收队列 →
  `rmw_wait` 轮询发现可读 → `rmw_take` 反序列化出用户消息。
- guard condition：标志位 + `check_and_clear`；wait set 为**轮询模型**（`rmw_wait`
  循环检查全部 waitable 的就绪状态，间隔 2 ms，见 `rmw_mdds/src/wait.cpp`），非条件
  变量唤醒。`rmw_*_set_on_new_*_callback` 系列为 stub（返回 `RMW_RET_UNSUPPORTED`；
  rclcpp 默认 executor 走 wait set 轮询，不依赖）。
- `rmw_serialize/deserialize`：同一套 CDR serdes，序列化格式标识 `"cdr"`。

### 7.2 QoS 事件映射（阶段二）

mdds 引擎无监听器，全部事件状态由 rmw 层轮询合成（`notify_events` 在端点增删、graph
回调、`rmw_wait` 轮询时刷新；`set_callback` 对未消费状态立即补调，与 matched 事件同模式）：

- `OFFERED/REQUESTED_DEADLINE_MISSED`：rmw 层跟踪每个 publisher 的 last-publish 与每个
  subscription 的 last-receive（收样 data callback + take 两条钩子），超期且该周期未计过时
  total_count++，回到周期内后重置计数臂。
- `LIVELINESS_CHANGED`（subscription 状态，对齐 DDS/cyclonedds 侧别）：alive/not_alive =
  匹配 writer 数按存活性拆分——远端 writer 按 lease 检查（AUTOMATIC 看 participant
  ANNOUNCE、MANUAL_BY_TOPIC 看 DATA/HEARTBEAT last-seen，从未收到帧视为存活），本地 writer
  同进程恒活。
- `LIVELINESS_LOST`（publisher 状态）：本 writer 为 MANUAL_BY_TOPIC 且
  `now - last_assert > lease` 时每段丢失期计一次（create/publish/assert_liveliness 都算断言，
  恢复后重新武装）；AUTOMATIC writer 恒不丢失。
- `OFFERED/REQUESTED_QOS_INCOMPATIBLE`：对已发现的对端端点做严格 `qos_compatible` 检查，
  按 GUID 去重计数，`last_policy_kind` 记最近失败策略（reliability/durability）。
- `MESSAGE_LOST`：镜像 reader 的 seq 空洞累计计数。
- `rmw_publisher_assert_liveliness` → `mdds::Writer::assert_liveliness()`（广播 HEARTBEAT）。

已知取舍：deadline/lease 的检测时机是轮询（rmw_wait/回调注册时），无后台线程，
检测延迟 ≤ wait 轮询周期；`LIVELINESS_LOST` 的远端枚举在每次 poll 复制一次
snapshot，端点规模小时可接受。

### 7.1 service/client 映射与关联头格式

mdds 库不变（无原生 RPC），service/client 在 rmw 层用一对派生 topic 实现：

- client：writer 于 `<service>/_request`（类型 `<pkg>/srv/<Srv>_Request`），
  reader 于 `<service>/_response`（类型 `<pkg>/srv/<Srv>_Response`）。
- service 镜像：reader 于 `<service>/_request`，writer 于 `<service>/_response`。
- 图推导（`rmw_get_service_names_and_types` 等）：topic 以 `/_response` 结尾的
  WRITER = service 端，以 `/_request` 结尾的 WRITER = client 端；service 名 =
  topic 去后缀，service 类型 = 端点 type_name 去 `_Response`/`_Request` 后缀。

**关联头（跨实现契约，gateway 阶段三必须原样转发）**：两个派生 topic 上的每个样本
payload = `client_guid(16) | seq:uint64 big-endian | CDR(...)`，共 24 B 头 + CDR
（CDR 含自身 4 B 封装头）。`client_guid` 是**发起请求的 client 的 request writer
GUID**；`seq` 是该 client 内单调递增计数器（`rmw_send_request` 返回值）。
service 端 `rmw_take_request` 剥头填入 `rmw_request_id_t`（16 B GUID 前置拷贝进
24 B 数组、余下置 0，`sequence_number` = seq），`rmw_send_response` 原样回显该头；
client 端 `rmw_take_response` 按头中 GUID 过滤非本 client 的响应（丢弃继续取下一条）。
gateway 转发时必须保留完整 24 B 头，不得重写 GUID/seq（client 靠它配对响应）。

## 8. 包结构与构建

```
ros2/src/ros2/mdds/
├── package.xml                ament_cmake，无 ROS 运行时依赖（core 平台无关）
├── CMakeLists.txt             libmdds；选项 MDDS_WITH_DSOFTBUS（OHOS 交叉时 ON）、
│                              BUILD_TESTING（colcon 路径用 ament_cmake_gtest）
├── include/mdds/              公共头：participant/writer/reader/qos/guid/transport
├── src/                       frame、fragment、discovery、participant、qos、
│                              transport_udp、transport_dsoftbus（条件编译）
└── test/                      gtest 单测：frame/fragment 编解码、udp_loopback 双
                               participant 互通、send_lane 背压/保序、qos 预设与校验、
                               participant 可靠性（精确 NACK 重传治愈、GAP 丢失判定、
                               KEEP_ALL 背压）
```

- mdds core **不依赖 ROS 头文件**；本 Windows 主机没有可用的 C++ 工具链（无 MSVC，
  conda-forge `cxx-compiler` 只装了 ucrt 不含编译器二进制），因此单元测试走
  **板上 ctest** 路径（colcon `BUILD_TESTING=ON` + `ament_cmake_gtest`，gtest 由
  gtest_vendor 交叉编译，`run_board_tests.sh` 推送运行）。CMakeLists 保留了脱离
  ament 的 standalone 静态库模式（`find_package(GTest)`），供将来有主机工具链时使用。
- OHOS 构建由 `build_ohos.sh` 收编（`--base-paths src` 自动扫描到新包），
  需从 `PACKAGES_SKIP` 移除 `rmw_mdds`，并把 `run_board_tests.sh` 中 `__rmw_mdds`
  排除规则改为放行。
- `transport_dsoftbus` 编译需要 DSoftBus SDK 头文件（vendor 自
  `communication_dsoftbus/interfaces/kits/`，只拷头文件，不改生产代码）；头文件与
  板上拉取的 `libsoftbus_client.z.so` / `libnativetoken_shared.z.so` /
  `libtokensetproc_shared.z.so` 一起 vendored 在 `third_party/dsoftbus/`，链接方式
  已定：`-l:<name>` + `-Wl,--allow-shlib-undefined -Wl,-rpath,/system/lib64/platformsdk`
  （设备上同名库位于该系统目录，见 CMakeLists.txt）。

## 9. 首要风险与验证顺序

1. ~~权限风险~~ **已解决**（2026-08-29，设备实测结论）：
   - shell 进程默认继承 hdcd token，被 softbus_server 拒绝
     `ohos.permission.DISTRIBUTED_DATASYNC`。解决：进程内调用
     `GetAccessTokenId` + `SetSelfTokenID` 设置 native token
     （DISTRIBUTED_DATASYNC + DISTRIBUTED_SOFTBUS_CENTER），且 pkgName/会话名
     必须匹配设备 `/system/etc/communication/softbus/softbus_trans_permission.json`
     白名单——复用已预注册的 `com.kaihong.mdds.*` 条目。
     → `transport_dsoftbus` 初始化必须固定使用白名单 pkgName 并先 SetSelfTokenID。
   - **同步 `Bind()` 在 KaihongOS 该版本返回 -426115007
     （SOFTBUS_TRANS_PEER_SESSION_NOT_CREATED），必须使用 `BindAsync()`**。
   - hdc 在该 Windows 主机上 `file send`/stdin 管道不稳定（时好时坏），
     备用部署路径：base64 分块 `echo` 到 `/data/local/tmp` 再解码。
2. 同机多进程互通走 UDP loopback 是否满足 demo/测试拓扑（talker/listener 同板不同进程）。
3. ~~4 KB 级 Proxy/BR 通道下的分片开销与吞吐~~ **已验证**（2026-08-29，
   `examples/mdds_e2e` 双板实测）：A↔B 双向各 100% 到达、0 重复；
   64 KB 载荷（约 16 片/条）20/20 重组成功。构建 `./examples/build_e2e.sh`，
   部署 `/data/local/tmp/mdds_e2e/`（二进制 + libmdds.so + libc++_shared.so）。

## 10. 已知限制（v3）

- 每进程最多 15 条 dsoftbus socket → 单进程可见跨设备 peer 数 ≤ ~14；dsoftbus
  transport 每进程单实例（`ISocketListener` 回调无 user data，经全局表分发）。
- 不支持多播；大规模节点拓扑未优化（全互联）。
- lifespan 以 `pub_time_ms`（system_clock）判定，跨设备时钟不同步时过期判定按接收方
  时钟偏移。
- deadline/liveliness 事件为轮询合成，无后台定时线程。
- writer history 的 KEEP_ALL 实为有限缓存（`kKeepAllCap = 256`），满时 `write()` 拒绝
  （背压显式上抛，不静默驱逐）。
- dsoftbus 发送为 lane 异步： `Transport::send()` 返回 true 仅表示已入队；lane 预算满
  丢新帧由 reliable 闭环自愈，best_effort 流量在持续超预算时表现为丢包（计数于
  `SendLane::dropped()`，暂无对上层统计出口）。
- 单帧发送上限跟随 `GetSessionOption` 探测值；探测失败的通道按 3800 B 分片。
- gateway 阶段三。（service/client 已由 rmw_mdds 以派生 topic 实现，见 7.1 节。）

## 11. gateway 桥接契约（阶段三定稿）

外部评审结论（见 docs/gateway/design.md）：mdds **不**引入 SPDP/SEDP 或任何
RTPS 概念；gateway 是有状态的协议终结点，在两个域各自创建真实端点（影子端点），
而不是翻译发现报文。为此 mdds 对外承诺一个稳定的桥接接口面（现有公共 API 的子集，
gateway 只依赖这些，不触碰内部结构）：

- **graph snapshot**：`Participant::remote_participants()` 返回全部远端 participant
  及其端点（`EndpointRecord`：guid、kind、topic、type_name、qos——**无 last_seen
  字段**；last-seen 由 `remote_writer_last_seen_ms()` /
  `remote_participant_last_seen_ms()` 单独查询）。type_name 为 ROS 风格
  `pkg/msg/Type`，与 rclcpp graph 同格式，gateway 据此做类型解析。
- **graph delta**：无独立 watch 接口；gateway 以 ≤1 s 周期轮询 snapshot 即得增量
  （ANNOUNCE 本身有 announce_seq 单调去重，轮询不会漏掉"出现过"的端点，只会
  延迟发现）。端点下线经 `OnShutdown`/心跳超时从 snapshot 消失。
- **proxy/影子端点**：`create_writer` / `create_reader`（含 `ignore_local` 标志——
  gateway 用它避免收到自己转发回的副本）。
- **数据面**：`Writer::write(bytes)` 原样上线；`Reader::take(payload, MessageInfo)`
  取出原始 CDR 字节与 `MessageInfo{writer_guid, seq, pub_time_ms}`。writer_guid/seq
  供统计诊断；环回去重不使用身份键（环回副本已被转发方换身份，见
  mdds_gateway/docs/design.md 第 4 节）。
- **service 关联头**：派生 topic 的 24 B 头（client_guid+seq）随 payload 原样
  透传，gateway 不得重写（§7.1 契约）。

QoS 语义边界：gateway 把端到端拆成两段 hop-by-hop 链路；transient_local、
deadline、lifespan 等策略不跨网关延续（目标侧看到的是 gateway 端点的 QoS）。
lifespan 样本经网关转发后 `pub_time_ms` 由 gateway 的 writer 重新打戳，
原发布时刻不保留——记录为已知限制。
