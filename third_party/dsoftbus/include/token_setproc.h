// Minimal declaration vendored from OpenHarmony security/access_token
// (interfaces/innerkits/token_setproc/include/token_setproc.h). Links against
// the board's libtokensetproc_shared.z.so.
#ifndef MDDS_THIRD_PARTY_TOKEN_SETPROC_H
#define MDDS_THIRD_PARTY_TOKEN_SETPROC_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

int32_t SetSelfTokenID(uint64_t tokenID);
uint64_t GetSelfTokenID(void);

#ifdef __cplusplus
}
#endif
#endif
