#ifndef LWIP_LWIPOPTS_H
#define LWIP_LWIPOPTS_H

/* --- NO_SYS mode: single-threaded, raw API only --- */
#define NO_SYS                  1
#define SYS_LIGHTWEIGHT_PROT    0
#define LWIP_SOCKET             0
#define LWIP_NETCONN            0
#define LWIP_NETIF_API          0

/* --- Memory configuration --- */
#define MEM_LIBC_MALLOC         1
#define MEMP_MEM_MALLOC         1
#define MEM_ALIGNMENT           8
#define MEM_SIZE                (512 * 1024)

/* --- pbuf pool --- */
#define PBUF_POOL_SIZE          256
#define PBUF_POOL_BUFSIZE       1600

/* --- TCP configuration for loopback use --- */
#define LWIP_TCP                1
#define TCP_MSS                 1360
#define TCP_WND                 (0xFFFF)    /* Must fit in u16_t without window scaling */
#define TCP_SND_BUF             (0xFFFF)
#define TCP_SND_QUEUELEN        ((4 * (TCP_SND_BUF) + (TCP_MSS - 1))/(TCP_MSS))
#define TCP_LISTEN_BACKLOG      1
#define LWIP_TCP_TIMESTAMPS     0
#define LWIP_TCP_SACK_OUT       0
#define LWIP_WND_SCALE          0
#define TCP_OVERSIZE            TCP_MSS
#define LWIP_TCP_KEEPALIVE      0

/* --- IPv4/IPv6 --- */
#define LWIP_IPV4               1
#define LWIP_IPV6               1
#define LWIP_ICMP               0
#define LWIP_ICMP6              1   /* Required by IPv6 stack internals */
#define LWIP_IPV6_MLD           1   /* Required by IPv6 netif */
#define LWIP_RAW                0
#define IPV6_FRAG_COPYHEADER    1   /* Required: sizeof(ip6_reass_helper) > IP6_FRAG_HLEN */

/* --- UDP (disabled - we only need TCP) --- */
#define LWIP_UDP                0
#define LWIP_DHCP               0
#define LWIP_AUTOIP             0

/* --- DNS (disabled) --- */
#define LWIP_DNS                0

/* --- ARP/Ethernet (disabled - raw IP input) --- */
#define LWIP_ARP                0
#define LWIP_ETHERNET           0

/* --- Disable stats for performance --- */
#define LWIP_STATS              0
#define LWIP_STATS_DISPLAY      0

/* --- Checksum: we compute checksums ourselves --- */
#define CHECKSUM_GEN_IP         1
#define CHECKSUM_GEN_TCP        1
#define CHECKSUM_CHECK_IP       1
#define CHECKSUM_CHECK_TCP      1

/* --- Loopback: no congestion control needed --- */
#define LWIP_EVENT_API          0
#define LWIP_CALLBACK_API       1

/* --- Debug (disabled for release) --- */
#define LWIP_DEBUG              0

/* --- Timers --- */
#define LWIP_TIMERS             1

/* --- Platform specifics for Windows --- */
#define LWIP_NO_STDINT_H        0
#define LWIP_NO_INTTYPES_H      0

/* --- We do not use netif link callbacks --- */
#define LWIP_NETIF_LINK_CALLBACK 0
#define LWIP_NETIF_STATUS_CALLBACK 0

/* --- Single netif, no routing --- */
#define LWIP_SINGLE_NETIF       1

/* --- Maximum number of TCP PCBs --- */
#define MEMP_NUM_TCP_PCB        256
#define MEMP_NUM_TCP_PCB_LISTEN 16
#define MEMP_NUM_TCP_SEG        512

/* --- Wildcard port matching for listen PCBs (custom) --- */
/* When local_port is 0 on a listen PCB, accept connections on any port */
#define LWIP_HOOK_FILENAME      "lwip_hooks.h"

#endif /* LWIP_LWIPOPTS_H */
