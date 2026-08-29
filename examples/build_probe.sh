#!/usr/bin/env bash
# Build dsoftbus_probe for OpenHarmony (aarch64) with the NDK clang.
# Links against the board-pulled libsoftbus_client.z.so in third_party/dsoftbus/lib.
# Run from the mdds package root:  ./examples/build_probe.sh
set -euo pipefail
cd "$(dirname "$0")/.."

OHOS_NATIVE="${OHOS_NATIVE_SDK:-C:/Users/17715/Downloads/commandline-tools-windows-x64-6.1.1.300/command-line-tools/sdk/default/openharmony/native}"
CC="$OHOS_NATIVE/llvm/bin/clang.exe"

"$CC" --target=aarch64-linux-ohos --sysroot="$OHOS_NATIVE/sysroot" \
  -O2 -fPIC \
  -I third_party/dsoftbus/include \
  examples/dsoftbus_probe.c \
  -L third_party/dsoftbus/lib \
  -l:libsoftbus_client.z.so -l:libnativetoken_shared.z.so -l:libtokensetproc_shared.z.so \
  -Wl,--allow-shlib-undefined \
  -Wl,-rpath,/data/local/tmp/mdds_probe \
  -o examples/dsoftbus_probe

echo "built examples/dsoftbus_probe"
