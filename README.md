# mdds

A minimal DDS (RTPS-like) middleware transport for ROS 2 on OpenHarmony.

mdds uses DSoftBus sockets (`DATA_TYPE_BYTES`) as a transparent byte pipe
between devices and implements framing, fragmentation/reassembly, discovery
and QoS (reliability/history) at its own layer, transparent to DSoftBus.

## Features (wire v3)

- Custom wire format with per-fragment flags and TLV extensions
- Precise NACK repair and GAP-based healing for `BEST_EFFORT`/`RELIABLE`
- Per-writer send lanes and `KEEP_ALL` backpressure
- QoS presets with entry-point validation
- `max_payload` probing per DSoftBus channel (falls back to a conservative
  default)

## Transports

- `transport_dsoftbus` — cross-device data plane on OpenHarmony (DSoftBus
  Socket Bytes)
- `transport_udp` — UDP loopback for host unit tests and same-machine
  multi-process interop

## Usage

mdds is an ament package; build it as part of a ROS 2 Jazzy workspace
(`colcon build --packages-up-to mdds`), or cross-compile for OpenHarmony
via the manifest repo at `github.com/Jiusi-pys/ros2` (branch `jazzy_ohos`,
which registers this repo in `ros2.repos` as `Jiusi-pys/mdds`).

The RMW binding lives in the companion repo
[rmw_mdds](https://github.com/Jiusi-pys/rmw_mdds), which also hosts the
`mdds_gateway` DDS<->DSoftBus bridge node.

See `docs/design.md` for the full design document.
