/*
 * arch/cc.h - Architecture/compiler definitions for MinGW-w64 (GCC on Windows)
 * Used by lwIP in NO_SYS mode for the VpnHood TCP stack shim.
 */
#ifndef LWIP_ARCH_CC_H
#define LWIP_ARCH_CC_H

/* Use standard C library errno */
#include <errno.h>

/* Use GCC's built-in byte order detection */
#ifndef BYTE_ORDER
#define BYTE_ORDER LITTLE_ENDIAN
#endif

/* Protection type (unused in NO_SYS mode) */
typedef int sys_prot_t;

/* Compiler hints for packing structures */
#define PACK_STRUCT_BEGIN
#define PACK_STRUCT_STRUCT __attribute__((packed))
#define PACK_STRUCT_END
#define PACK_STRUCT_FIELD(x) x

/* Platform diagnostics */
#include <stdio.h>
#define LWIP_PLATFORM_DIAG(x) do { printf x; } while(0)
#define LWIP_PLATFORM_ASSERT(x) do { printf("Assertion \"%s\" failed at line %d in %s\n", \
                                     x, __LINE__, __FILE__); while(1); } while(0)

/* Random number generation */
#include <stdlib.h>
#define LWIP_RAND() ((u32_t)rand())

#endif /* LWIP_ARCH_CC_H */
