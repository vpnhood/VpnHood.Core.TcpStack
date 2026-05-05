/*
 * lwip_shim.c - Thin C wrapper around lwIP's raw TCP API for use from C# via P/Invoke.
 * Runs lwIP in NO_SYS=1 mode (single-threaded, callback-based).
 * All lwIP calls are funneled through a critical section to ensure thread safety
 * when the C# side calls from multiple threads.
 */

/* Include lwIP headers BEFORE any system headers to avoid htonl/htons conflicts */
#include "lwip/init.h"
#include "lwip/tcp.h"
#include "lwip/timeouts.h"
#include "lwip/netif.h"
#include "lwip/ip.h"
#include "lwip/pbuf.h"
#include "lwip/ip4.h"
#include "lwip/ip6.h"
#include "lwip/mem.h"

/* Now include our public header and system headers */
#include "lwip_shim.h"

#ifdef _WIN32
/* Prevent winsock from redefining htonl etc. */
#define _WINSOCKAPI_
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#else
#include <pthread.h>
#endif

#include <stdlib.h>
#include <string.h>

/* ---- Internal state ---- */
typedef struct {
    struct netif netif;

    /* Callbacks */
    lwip_output_fn   output_fn;
    void*            output_user;
    lwip_accept_fn   accept_fn;
    void*            accept_user;
    lwip_recv_fn     recv_fn;
    lwip_closed_fn   closed_fn;
    lwip_sent_fn     sent_fn;

    /* Active wildcard listener */
    struct tcp_pcb*  listen_pcb;

    /* Lock for thread safety */
#ifdef _WIN32
    CRITICAL_SECTION lock;
#else
    pthread_mutex_t  lock;
#endif
} lwip_shim_stack_t;

/* Per-connection context stored in tcp_pcb->callback_arg */
typedef struct {
    lwip_shim_stack_t* stack;
    void*              user_ctx; /* C# side context */
} conn_ctx_t;

/* ---- Lock helpers ---- */
static void stack_lock(lwip_shim_stack_t* s) {
#ifdef _WIN32
    EnterCriticalSection(&s->lock);
#else
    pthread_mutex_lock(&s->lock);
#endif
}

static void stack_unlock(lwip_shim_stack_t* s) {
#ifdef _WIN32
    LeaveCriticalSection(&s->lock);
#else
    pthread_mutex_unlock(&s->lock);
#endif
}

/* ---- Netif output callback ---- */
static err_t shim_netif_output4(struct netif* netif, struct pbuf* p, const ip4_addr_t* ipaddr) {
    (void)ipaddr;
    lwip_shim_stack_t* stack = (lwip_shim_stack_t*)netif->state;
    if (!stack->output_fn) return ERR_OK;

    /* Collect pbuf chain into contiguous buffer */
    uint16_t total = p->tot_len;
    uint8_t* buf = (uint8_t*)malloc(total);
    if (!buf) return ERR_MEM;

    pbuf_copy_partial(p, buf, total, 0);
    stack->output_fn(buf, total, stack->output_user);
    free(buf);
    return ERR_OK;
}

static err_t shim_netif_output6(struct netif* netif, struct pbuf* p, const ip6_addr_t* ipaddr) {
    (void)ipaddr;
    lwip_shim_stack_t* stack = (lwip_shim_stack_t*)netif->state;
    if (!stack->output_fn) return ERR_OK;

    uint16_t total = p->tot_len;
    uint8_t* buf = (uint8_t*)malloc(total);
    if (!buf) return ERR_MEM;

    pbuf_copy_partial(p, buf, total, 0);
    stack->output_fn(buf, total, stack->output_user);
    free(buf);
    return ERR_OK;
}

static err_t shim_netif_init(struct netif* netif) {
    netif->name[0] = 'v';
    netif->name[1] = 'h';
    netif->mtu = 1500;
    netif->flags = NETIF_FLAG_LINK_UP | NETIF_FLAG_UP;
    netif->output = shim_netif_output4;
    netif->output_ip6 = shim_netif_output6;
    return ERR_OK;
}

/* ---- TCP callbacks ---- */
static err_t shim_tcp_recv(void* arg, struct tcp_pcb* tpcb, struct pbuf* p, err_t err) {
    conn_ctx_t* ctx = (conn_ctx_t*)arg;
    if (!ctx) return ERR_ABRT;

    lwip_shim_stack_t* stack = ctx->stack;

    if (p == NULL || err != ERR_OK) {
        /* Connection closed by remote */
        if (stack->closed_fn)
            stack->closed_fn((lwip_conn_handle_t)tpcb, (int)err, ctx->user_ctx);
        /* Free conn context */
        free(ctx);
        tpcb->callback_arg = NULL;
        return ERR_OK;
    }

    if (stack->recv_fn) {
        /* Linearize if needed */
        uint16_t total = p->tot_len;
        uint8_t* buf = NULL;
        const uint8_t* data;

        if (p->next == NULL) {
            data = (const uint8_t*)p->payload;
        } else {
            buf = (uint8_t*)malloc(total);
            if (!buf) {
                pbuf_free(p);
                return ERR_MEM;
            }
            pbuf_copy_partial(p, buf, total, 0);
            data = buf;
        }

        int consumed = stack->recv_fn((lwip_conn_handle_t)tpcb, data, total, ctx->user_ctx);
        if (consumed > 0)
            tcp_recved(tpcb, (u16_t)consumed);

        if (buf) free(buf);
    }

    pbuf_free(p);
    return ERR_OK;
}

static err_t shim_tcp_sent(void* arg, struct tcp_pcb* tpcb, u16_t len) {
    conn_ctx_t* ctx = (conn_ctx_t*)arg;
    if (!ctx) return ERR_OK;

    lwip_shim_stack_t* stack = ctx->stack;
    if (stack->sent_fn)
        stack->sent_fn((lwip_conn_handle_t)tpcb, (int)len, ctx->user_ctx);

    return ERR_OK;
}

static void shim_tcp_err(void* arg, err_t err) {
    conn_ctx_t* ctx = (conn_ctx_t*)arg;
    if (!ctx) return;

    lwip_shim_stack_t* stack = ctx->stack;
    if (stack->closed_fn)
        stack->closed_fn(NULL, (int)err, ctx->user_ctx);

    free(ctx);
}

static err_t shim_tcp_accept(void* arg, struct tcp_pcb* newpcb, err_t err) {
    lwip_shim_stack_t* stack = (lwip_shim_stack_t*)arg;
    if (!stack || err != ERR_OK || !newpcb) return ERR_ABRT;

    /* Extract endpoints */
    uint32_t local_ip4 = 0, remote_ip4 = 0;
    const uint8_t* local_ip6 = NULL;
    const uint8_t* remote_ip6 = NULL;
    int is_ipv6 = 0;

    if (IP_IS_V6(&newpcb->local_ip)) {
        is_ipv6 = 1;
        local_ip6 = (const uint8_t*)ip_2_ip6(&newpcb->local_ip);
        remote_ip6 = (const uint8_t*)ip_2_ip6(&newpcb->remote_ip);
    } else {
        local_ip4 = ip4_addr_get_u32(ip_2_ip4(&newpcb->local_ip));
        remote_ip4 = ip4_addr_get_u32(ip_2_ip4(&newpcb->remote_ip));
    }

    conn_ctx_t* ctx = (conn_ctx_t*)calloc(1, sizeof(conn_ctx_t));
    if (!ctx) return ERR_MEM;
    ctx->stack = stack;

    void* user_ctx = NULL;
    if (stack->accept_fn) {
        user_ctx = stack->accept_fn(
            (lwip_conn_handle_t)newpcb,
            local_ip4, newpcb->local_port,
            remote_ip4, newpcb->remote_port,
            local_ip6, remote_ip6,
            is_ipv6,
            stack->accept_user);
    }
    ctx->user_ctx = user_ctx;

    tcp_arg(newpcb, ctx);
    tcp_recv(newpcb, shim_tcp_recv);
    tcp_sent(newpcb, shim_tcp_sent);
    tcp_err(newpcb, shim_tcp_err);

    /* Accept backlog */
    tcp_backlog_accepted(newpcb);

    return ERR_OK;
}

/* ---- Public API ---- */
LWIP_SHIM_EXPORT lwip_stack_handle_t lwip_shim_create(void) {
    lwip_shim_stack_t* stack = (lwip_shim_stack_t*)calloc(1, sizeof(lwip_shim_stack_t));
    if (!stack) return NULL;

#ifdef _WIN32
    InitializeCriticalSection(&stack->lock);
#else
    pthread_mutex_init(&stack->lock, NULL);
#endif

    stack_lock(stack);

    lwip_init();

    /* Set up a virtual netif that accepts all IP traffic */
    ip4_addr_t ip4addr, netmask, gw;
    IP4_ADDR(&ip4addr, 0, 0, 0, 0);
    IP4_ADDR(&netmask, 0, 0, 0, 0);
    IP4_ADDR(&gw, 0, 0, 0, 0);

    netif_add(&stack->netif, &ip4addr, &netmask, &gw, stack, shim_netif_init, ip_input);
    netif_set_default(&stack->netif);
    netif_set_up(&stack->netif);

    /* Enable IPv6 on the netif */
    ip6_addr_t ip6any;
    memset(&ip6any, 0, sizeof(ip6any));
    netif_ip6_addr_set(&stack->netif, 0, &ip6any);
    netif_ip6_addr_set_state(&stack->netif, 0, IP6_ADDR_VALID);

    stack_unlock(stack);
    return (lwip_stack_handle_t)stack;
}

LWIP_SHIM_EXPORT void lwip_shim_destroy(lwip_stack_handle_t handle) {
    lwip_shim_stack_t* stack = (lwip_shim_stack_t*)handle;
    if (!stack) return;

    stack_lock(stack);

    if (stack->listen_pcb) {
        tcp_close(stack->listen_pcb);
        stack->listen_pcb = NULL;
    }

    netif_remove(&stack->netif);

    stack_unlock(stack);

#ifdef _WIN32
    DeleteCriticalSection(&stack->lock);
#else
    pthread_mutex_destroy(&stack->lock);
#endif

    free(stack);
}

LWIP_SHIM_EXPORT void lwip_shim_set_output_callback(lwip_stack_handle_t handle, lwip_output_fn fn, void* user_data) {
    lwip_shim_stack_t* stack = (lwip_shim_stack_t*)handle;
    stack->output_fn = fn;
    stack->output_user = user_data;
}

LWIP_SHIM_EXPORT void lwip_shim_set_accept_callback(lwip_stack_handle_t handle, lwip_accept_fn fn, void* user_data) {
    lwip_shim_stack_t* stack = (lwip_shim_stack_t*)handle;
    stack->accept_fn = fn;
    stack->accept_user = user_data;
}

LWIP_SHIM_EXPORT void lwip_shim_set_recv_callback(lwip_stack_handle_t handle, lwip_recv_fn fn) {
    lwip_shim_stack_t* stack = (lwip_shim_stack_t*)handle;
    stack->recv_fn = fn;
}

LWIP_SHIM_EXPORT void lwip_shim_set_closed_callback(lwip_stack_handle_t handle, lwip_closed_fn fn) {
    lwip_shim_stack_t* stack = (lwip_shim_stack_t*)handle;
    stack->closed_fn = fn;
}

LWIP_SHIM_EXPORT void lwip_shim_set_sent_callback(lwip_stack_handle_t handle, lwip_sent_fn fn) {
    lwip_shim_stack_t* stack = (lwip_shim_stack_t*)handle;
    stack->sent_fn = fn;
}

LWIP_SHIM_EXPORT int lwip_shim_input(lwip_stack_handle_t handle, const uint8_t* data, int len) {
    lwip_shim_stack_t* stack = (lwip_shim_stack_t*)handle;
    if (!stack || !data || len <= 0) return -1;

    struct pbuf* p = pbuf_alloc(PBUF_RAW, (u16_t)len, PBUF_RAM);
    if (!p) return -1;

    memcpy(p->payload, data, len);

    stack_lock(stack);

    /* Determine IP version from first nibble */
    uint8_t version = (data[0] >> 4) & 0x0F;
    err_t err;

    if (version == 6) {
        /* For IPv6, set the netif's IPv6 address to match the packet's destination */
        /* IPv6 dest addr is at offset 24 in the IPv6 header */
        if (len >= 40) {
            ip6_addr_t dest6;
            memcpy(&dest6, data + 24, 16);
            ip6_addr_assign_zone(&dest6, IP6_UNICAST, &stack->netif);
            netif_ip6_addr_set(&stack->netif, 0, &dest6);
            netif_ip6_addr_set_state(&stack->netif, 0, IP6_ADDR_PREFERRED);
        }
        err = stack->netif.input(p, &stack->netif);
    } else {
        /* For IPv4, temporarily set netif IP to match the packet's destination.
         * This ensures lwIP's ip4_input_accept() passes the check.
         * IPv4 dest addr is at offset 16 in the IP header. */
        if (len >= 20) {
            ip4_addr_t dest4;
            memcpy(&dest4, data + 16, 4);
            netif_set_ipaddr(&stack->netif, &dest4);
        }
        err = stack->netif.input(p, &stack->netif);
    }

    stack_unlock(stack);

    /* ip_input takes ownership of the pbuf (frees it internally).
     * Do NOT call pbuf_free here even on error. */
    if (err != ERR_OK) {
        return -1;
    }
    return 0;
}

LWIP_SHIM_EXPORT lwip_listener_handle_t lwip_shim_listen_any(lwip_stack_handle_t handle) {
    lwip_shim_stack_t* stack = (lwip_shim_stack_t*)handle;
    if (!stack) return NULL;

    stack_lock(stack);

    if (stack->listen_pcb) {
        stack_unlock(stack);
        return (lwip_listener_handle_t)stack->listen_pcb;
    }

    struct tcp_pcb* pcb = tcp_new_ip_type(IPADDR_TYPE_ANY);
    if (!pcb) {
        stack_unlock(stack);
        return NULL;
    }

    /* Bind to any address, any port (0) to accept all */
    ip_addr_t any;
    ip_addr_set_any(0, &any);
    err_t err = tcp_bind(pcb, &any, 0);
    if (err != ERR_OK) {
        tcp_close(pcb);
        stack_unlock(stack);
        return NULL;
    }

    struct tcp_pcb* lpcb = tcp_listen_with_backlog(pcb, 255);
    if (!lpcb) {
        tcp_close(pcb);
        stack_unlock(stack);
        return NULL;
    }

    tcp_arg(lpcb, stack);
    tcp_accept(lpcb, shim_tcp_accept);
    /* Set local_port to 0 = wildcard (patched tcp_in accepts port 0 as any) */
    lpcb->local_port = 0;
    stack->listen_pcb = lpcb;

    stack_unlock(stack);
    return (lwip_listener_handle_t)lpcb;
}

LWIP_SHIM_EXPORT void lwip_shim_stop_listen(lwip_stack_handle_t handle, lwip_listener_handle_t listener) {
    lwip_shim_stack_t* stack = (lwip_shim_stack_t*)handle;
    if (!stack || !listener) return;

    stack_lock(stack);

    struct tcp_pcb* lpcb = (struct tcp_pcb*)listener;
    if (stack->listen_pcb == lpcb) {
        tcp_close(lpcb);
        stack->listen_pcb = NULL;
    }

    stack_unlock(stack);
}

LWIP_SHIM_EXPORT int lwip_shim_write(lwip_stack_handle_t handle, lwip_conn_handle_t conn, const uint8_t* data, int len) {
    lwip_shim_stack_t* stack = (lwip_shim_stack_t*)handle;
    if (!stack || !conn || !data || len <= 0) return -1;

    stack_lock(stack);

    struct tcp_pcb* pcb = (struct tcp_pcb*)conn;
    u16_t sndbuf = tcp_sndbuf(pcb);
    u16_t to_write = (u16_t)(len < sndbuf ? len : sndbuf);

    if (to_write == 0) {
        stack_unlock(stack);
        return 0;
    }

    err_t err = tcp_write(pcb, data, to_write, TCP_WRITE_FLAG_COPY);
    if (err != ERR_OK) {
        stack_unlock(stack);
        return (int)err;
    }

    tcp_output(pcb);
    stack_unlock(stack);
    return (int)to_write;
}

LWIP_SHIM_EXPORT int lwip_shim_sndbuf(lwip_stack_handle_t handle, lwip_conn_handle_t conn) {
    lwip_shim_stack_t* stack = (lwip_shim_stack_t*)handle;
    if (!stack || !conn) return 0;

    stack_lock(stack);
    int result = (int)tcp_sndbuf((struct tcp_pcb*)conn);
    stack_unlock(stack);
    return result;
}

LWIP_SHIM_EXPORT void lwip_shim_close(lwip_stack_handle_t handle, lwip_conn_handle_t conn) {
    lwip_shim_stack_t* stack = (lwip_shim_stack_t*)handle;
    if (!stack || !conn) return;

    stack_lock(stack);

    struct tcp_pcb* pcb = (struct tcp_pcb*)conn;
    conn_ctx_t* ctx = (conn_ctx_t*)pcb->callback_arg;

    tcp_arg(pcb, NULL);
    tcp_recv(pcb, NULL);
    tcp_sent(pcb, NULL);
    tcp_err(pcb, NULL);
    tcp_close(pcb);

    if (ctx) free(ctx);

    stack_unlock(stack);
}

LWIP_SHIM_EXPORT void lwip_shim_abort(lwip_stack_handle_t handle, lwip_conn_handle_t conn) {
    lwip_shim_stack_t* stack = (lwip_shim_stack_t*)handle;
    if (!stack || !conn) return;

    stack_lock(stack);

    struct tcp_pcb* pcb = (struct tcp_pcb*)conn;
    conn_ctx_t* ctx = (conn_ctx_t*)pcb->callback_arg;

    tcp_arg(pcb, NULL);
    tcp_recv(pcb, NULL);
    tcp_sent(pcb, NULL);
    tcp_err(pcb, NULL);
    tcp_abort(pcb);

    if (ctx) free(ctx);

    stack_unlock(stack);
}

LWIP_SHIM_EXPORT void lwip_shim_poll(lwip_stack_handle_t handle) {
    lwip_shim_stack_t* stack = (lwip_shim_stack_t*)handle;
    if (!stack) return;

    stack_lock(stack);
    sys_check_timeouts();
    stack_unlock(stack);
}

/* ---- lwIP sys_now() implementation for NO_SYS mode ---- */
#ifdef _WIN32
u32_t sys_now(void) {
    return (u32_t)GetTickCount();
}
#else
#include <time.h>
u32_t sys_now(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (u32_t)(ts.tv_sec * 1000 + ts.tv_nsec / 1000000);
}
#endif
