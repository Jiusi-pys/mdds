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

#ifndef MDDS__GUID_HPP_
#define MDDS__GUID_HPP_

#include <array>
#include <cstdint>
#include <cstring>
#include <string>

namespace mdds
{

/// 16-byte globally unique identifier: 12-byte participant prefix + 4-byte entity id.
/// Maps into the 24-byte rmw_gid_t array with trailing zero padding.
struct Guid
{
  std::array<uint8_t, 16> bytes{};

  bool operator==(const Guid & other) const {return bytes == other.bytes;}
  bool operator!=(const Guid & other) const {return bytes != other.bytes;}
  bool operator<(const Guid & other) const {return bytes < other.bytes;}

  bool is_zero() const
  {
    for (auto b : bytes) {
      if (b != 0) {
        return false;
      }
    }
    return true;
  }

  std::string to_string() const
  {
    static const char hex[] = "0123456789abcdef";
    std::string out;
    out.reserve(32);
    for (auto b : bytes) {
      out.push_back(hex[b >> 4]);
      out.push_back(hex[b & 0x0f]);
    }
    return out;
  }
};

enum class EntityKind : uint8_t
{
  PARTICIPANT = 0,
  WRITER = 1,
  READER = 2,
};

/// Compose a Guid from a 12-byte participant prefix, an entity kind and a serial.
inline Guid make_guid(
  const std::array<uint8_t, 12> & prefix, EntityKind kind, uint32_t serial)
{
  Guid g;
  std::memcpy(g.bytes.data(), prefix.data(), 12);
  g.bytes[12] = static_cast<uint8_t>(kind);
  g.bytes[13] = static_cast<uint8_t>((serial >> 16) & 0xff);
  g.bytes[14] = static_cast<uint8_t>((serial >> 8) & 0xff);
  g.bytes[15] = static_cast<uint8_t>(serial & 0xff);
  return g;
}

/// Generate a random 12-byte participant prefix. Not cryptographically secure;
/// uniqueness within a fleet of networked devices is all that is required.
std::array<uint8_t, 12> generate_participant_prefix();

}  // namespace mdds

#endif  // MDDS__GUID_HPP_
