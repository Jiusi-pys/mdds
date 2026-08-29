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

// dsoftbus_probe: minimal feasibility check for running DSoftBus sockets from
// a shell-launched native process (the mdds risk #1 validation).
//
// Modes:
//   dsoftbus_probe listen                 - Socket + Listen, echo received bytes
//   dsoftbus_probe nodes                  - print local + all networked LNN nodes
//   dsoftbus_probe send <networkId> [msg] - Bind + SendBytes, wait for echo
//
// Build: examples/build_probe.sh (NDK clang, links libsoftbus_client.z.so).

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "socket.h"
#include "softbus_bus_center.h"
#include "softbus_error_code.h"
#include "nativetoken_kit.h"
#include "token_setproc.h"

// Whitelisted per /system/etc/communication/softbus/softbus_trans_permission.json
#define PKG_NAME "com.kaihong.mdds"
#define SOCKET_NAME "com.kaihong.mdds.probe"

static volatile int g_bound = 0;

static void setup_token(void)
{
    const char *perms[] = {
        "ohos.permission.DISTRIBUTED_DATASYNC",
        "ohos.permission.DISTRIBUTED_SOFTBUS_CENTER",
    };
    NativeTokenInfoParams info = {
        .dcapsNum = 0,
        .permsNum = 2,
        .aclsNum = 0,
        .dcaps = NULL,
        .perms = perms,
        .acls = NULL,
        .processName = PKG_NAME,
        .aplStr = "system_basic",
    };
    uint64_t tokenId = GetAccessTokenId(&info);
    int32_t ret = SetSelfTokenID(tokenId);
    printf("[token] GetAccessTokenId=%llu SetSelfTokenID=%d\n",
        (unsigned long long)tokenId, ret);
    fflush(stdout);
}

static void OnBytesCb(int32_t socket, const void *data, uint32_t dataLen)
{
    printf("[OnBytes] socket=%d len=%u data=%.*s\n", socket, dataLen, (int)dataLen, (const char *)data);
    fflush(stdout);
    if (strncmp((const char *)data, "ping", dataLen > 4 ? 4 : dataLen) == 0) {
        const char pong[] = "pong";
        int32_t ret = SendBytes(socket, pong, sizeof(pong) - 1);
        printf("[echo] SendBytes(pong)=%d\n", ret);
        fflush(stdout);
    }
}

static void OnBindCb(int32_t socket, PeerSocketInfo info)
{
    printf("[OnBind] socket=%d peer=%s networkId=%s\n", socket, info.name, info.networkId);
    fflush(stdout);
    g_bound = 1;
}

static void OnShutdownCb(int32_t socket, ShutdownReason reason)
{
    printf("[OnShutdown] socket=%d reason=%d\n", socket, reason);
    fflush(stdout);
}

static void OnBytesSentCb(int32_t socket, uint32_t dataSeq, int32_t errCode)
{
    printf("[OnBytesSent] socket=%d seq=%u err=%d\n", socket, dataSeq, errCode);
    fflush(stdout);
}

static void OnErrorCb(int32_t socket, int32_t errCode)
{
    printf("[OnError] socket=%d err=%d\n", socket, errCode);
    fflush(stdout);
}

static const ISocketListener g_listener = {
    .OnBind = OnBindCb,
    .OnShutdown = OnShutdownCb,
    .OnBytes = OnBytesCb,
    .OnBytesSent = OnBytesSentCb,
    .OnError = OnErrorCb,
};

static int do_listen(void)
{
    SocketInfo info = {
        .name = (char *)SOCKET_NAME,
        .pkgName = (char *)PKG_NAME,
        .dataType = DATA_TYPE_BYTES,
    };
    int32_t sock = Socket(info);
    printf("[listen] Socket()=%d\n", sock);
    if (sock < 0) {
        return 1;
    }
    QosTV qos[] = {{.qos = QOS_TYPE_MIN_BW, .value = 64 * 1024}};
    int32_t ret = Listen(sock, qos, 1, &g_listener);
    printf("[listen] Listen()=%d (SOFTBUS_OK=%d)\n", ret, SOFTBUS_OK);
    if (ret != SOFTBUS_OK) {
        return 1;
    }
    printf("[listen] listening as '%s', waiting for peers...\n", SOCKET_NAME);
    fflush(stdout);
    for (;;) {
        sleep(3600);
    }
    return 0;
}

static int do_nodes(void)
{
    NodeBasicInfo local;
    int32_t ret = GetLocalNodeDeviceInfo(PKG_NAME, &local);
    printf("[nodes] GetLocalNodeDeviceInfo=%d networkId=%s deviceName=%s\n",
        ret, ret == SOFTBUS_OK ? local.networkId : "?", ret == SOFTBUS_OK ? local.deviceName : "?");

    NodeBasicInfo *info = NULL;
    int32_t num = 0;
    ret = GetAllNodeDeviceInfo(PKG_NAME, &info, &num);
    printf("[nodes] GetAllNodeDeviceInfo=%d num=%d\n", ret, num);
    for (int32_t i = 0; i < num; ++i) {
        printf("[nodes]   peer[%d] networkId=%s deviceName=%s type=%u\n",
            i, info[i].networkId, info[i].deviceName, info[i].deviceTypeId);
    }
    if (info != NULL) {
        FreeNodeInfo(info);
    }
    fflush(stdout);
    return 0;
}

static int do_send(const char *peer_network_id, const char *msg)
{
    SocketInfo info = {
        .name = (char *)SOCKET_NAME,
        .peerName = (char *)SOCKET_NAME,
        .peerNetworkId = (char *)peer_network_id,
        .pkgName = (char *)PKG_NAME,
        .dataType = DATA_TYPE_BYTES,
    };
    int32_t sock = Socket(info);
    printf("[send] Socket()=%d\n", sock);
    if (sock < 0) {
        return 1;
    }
    QosTV qos[] = {
        {.qos = QOS_TYPE_MIN_BW, .value = 64 * 1024},
        {.qos = QOS_TYPE_MAX_WAIT_TIMEOUT, .value = 15000},
    };
    // NOTE: synchronous Bind() returns SOFTBUS_TRANS_PEER_SESSION_NOT_CREATED
    // on this KaihongOS build; async bind works.
    int32_t ret = BindAsync(sock, qos, 2, &g_listener);
    printf("[send] BindAsync()=%d (SOFTBUS_OK=%d)\n", ret, SOFTBUS_OK);
    if (ret != SOFTBUS_OK) {
        Shutdown(sock);
        return 1;
    }
    for (int i = 0; i < 150 && !g_bound; ++i) {
        usleep(100 * 1000);
    }
    if (!g_bound) {
        printf("[send] bind timed out\n");
        Shutdown(sock);
        return 1;
    }
    ret = SendBytes(sock, msg, (uint32_t)strlen(msg));
    printf("[send] SendBytes('%s')=%d\n", msg, ret);
    fflush(stdout);
    // wait a bit for the echo
    for (int i = 0; i < 50; ++i) {
        usleep(100 * 1000);
    }
    Shutdown(sock);
    printf("[send] done\n");
    return 0;
}

int main(int argc, char *argv[])
{
    if (argc < 2) {
        fprintf(stderr, "usage: %s listen | nodes | send <networkId> [msg]\n", argv[0]);
        return 2;
    }
    setup_token();
    if (strcmp(argv[1], "listen") == 0) {
        return do_listen();
    }
    if (strcmp(argv[1], "nodes") == 0) {
        return do_nodes();
    }
    if (strcmp(argv[1], "send") == 0 && argc >= 3) {
        return do_send(argv[2], argc >= 4 ? argv[3] : "ping");
    }
    fprintf(stderr, "usage: %s listen | nodes | send <networkId> [msg]\n", argv[0]);
    return 2;
}
