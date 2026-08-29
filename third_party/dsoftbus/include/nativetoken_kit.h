// Minimal declarations vendored from OpenHarmony security/access_token
// (interfaces/innerkits/nativetoken/include/nativetoken_kit.h). Only what the
// DSoftBus transport needs. Links against the board's libnativetoken_shared.z.so.
#ifndef MDDS_THIRD_PARTY_NATIVETOKEN_KIT_H
#define MDDS_THIRD_PARTY_NATIVETOKEN_KIT_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
    int32_t dcapsNum;
    int32_t permsNum;
    int32_t aclsNum;
    const char **dcaps;
    const char **perms;
    const char **acls;
    const char *processName;
    const char *aplStr;
} NativeTokenInfoParams;

uint64_t GetAccessTokenId(NativeTokenInfoParams *infoInstance);

#ifdef __cplusplus
}
#endif
#endif
