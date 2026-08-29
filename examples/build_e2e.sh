#!/usr/bin/env bash
# Build mdds_e2e for OpenHarmony (aarch64) with the NDK clang++.
# Links against the freshly cross-built libmdds.so (build_ohos/mdds) and the
# board-pulled DSoftBus libs in third_party/dsoftbus/lib.
# Run from the mdds package root:  ./examples/build_e2e.sh
set -euo pipefail
cd "$(dirname "$0")/.."

OHOS_NATIVE="${OHOS_NATIVE_SDK:-C:/Users/17715/Downloads/commandline-tools-windows-x64-6.1.1.300/command-line-tools/sdk/default/openharmony/native}"
CXX="$OHOS_NATIVE/llvm/bin/clang++.exe"
MDDS_BUILD_DIR="${MDDS_BUILD_DIR:-$(cd ../../../build_ohos/mdds 2>/dev/null && pwd -W)}"
if [ -z "${MDDS_BUILD_DIR}" ] || [ ! -f "${MDDS_BUILD_DIR}/libmdds.so" ]; then
  echo "error: libmdds.so not found; cross-build mdds first (or set MDDS_BUILD_DIR)" >&2
  exit 1
fi

"$CXX" --target=aarch64-linux-ohos --sysroot="$OHOS_NATIVE/sysroot" \
  -O2 -fPIC -std=gnu++17 \
  -I include \
  -I third_party/dsoftbus/include \
  examples/mdds_e2e.cpp \
  -L "${MDDS_BUILD_DIR}" -lmdds \
  -L third_party/dsoftbus/lib \
  -l:libsoftbus_client.z.so -l:libnativetoken_shared.z.so -l:libtokensetproc_shared.z.so \
  -Wl,--allow-shlib-undefined \
  -Wl,-rpath,/data/local/tmp/mdds_e2e \
  -Wl,-rpath,/system/lib64/platformsdk \
  -o examples/mdds_e2e

echo "built examples/mdds_e2e (against ${MDDS_BUILD_DIR}/libmdds.so)"
