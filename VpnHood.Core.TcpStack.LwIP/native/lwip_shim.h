#ifndef LWIP_SHIM_H
#define LWIP_SHIM_H

#include <stdint.h>
#include <stddef.h>

#ifdef _WIN32
#define LWIP_SHIM_EXPORT __declspec(dllexport)
#else
#define LWIP_SHIM_EXPORT __attribute__((visibility("default")))
#endif

#ifdef __cplusplus
extern "C" {
#endif

/* Opaque handles */
typedef void* lwip_stack_handle_t;
typedef void* lwip_listener_handle_t;
typedef void* lwip_conn_handle_t;

/* Callback: outgoing IP packet produced by the stack.
 * The data pointer is valid only for the duration of the callback. */
typedef void (*lwip_output_fn)(const uint8_t* data, int len, void* user_data);

/* Callback: a new TCP connection has been accepted.
 * Returns a user-defined context pointer that will be passed to recv/closed callbacks. */
typedef void* (*lwip_accept_fn)(lwip_conn_handle_t conn,
                                uint32_t local_ip4, uint16_t local_port,
                                uint32_t remote_ip4, uint16_t remote_port,
                                const uint8_t* local_ip6, const uint8_t* remote_ip6,
                                int is_ipv6,
                                void* user_data);

/* Callback: data received on a connection.
 * Return the number of bytes consumed (acknowledged). */
typedef int (*lwip_recv_fn)(lwip_conn_handle_t conn, const uint8_t* data, int len, void* conn_ctx);

/* Callback: connection closed/error. */
typedef void (*lwip_closed_fn)(lwip_conn_handle_t conn, int err, void* conn_ctx);

/* Callback: sent data acknowledged by peer (flow control). */
typedef void (*lwip_sent_fn)(lwip_conn_handle_t conn, int len, void* conn_ctx);

/* ---- Stack lifecycle ---- */
LWIP_SHIM_EXPORT lwip_stack_handle_t lwip_shim_create(void);
LWIP_SHIM_EXPORT void lwip_shim_destroy(lwip_stack_handle_t stack);

/* ---- Callbacks ---- */
LWIP_SHIM_EXPORT void lwip_shim_set_output_callback(lwip_stack_handle_t stack, lwip_output_fn fn, void* user_data);
LWIP_SHIM_EXPORT void lwip_shim_set_accept_callback(lwip_stack_handle_t stack, lwip_accept_fn fn, void* user_data);
LWIP_SHIM_EXPORT void lwip_shim_set_recv_callback(lwip_stack_handle_t stack, lwip_recv_fn fn);
LWIP_SHIM_EXPORT void lwip_shim_set_closed_callback(lwip_stack_handle_t stack, lwip_closed_fn fn);
LWIP_SHIM_EXPORT void lwip_shim_set_sent_callback(lwip_stack_handle_t stack, lwip_sent_fn fn);

/* ---- Packet I/O ---- */
/* Feed a raw IP packet (IPv4 or IPv6) into the stack. Thread-safe (queued). */
LWIP_SHIM_EXPORT int lwip_shim_input(lwip_stack_handle_t stack, const uint8_t* data, int len);

/* ---- Listening ---- */
LWIP_SHIM_EXPORT lwip_listener_handle_t lwip_shim_listen_any(lwip_stack_handle_t stack);
LWIP_SHIM_EXPORT void lwip_shim_stop_listen(lwip_stack_handle_t stack, lwip_listener_handle_t listener);

/* ---- Connection operations ---- */
/* Write data to a connection. Returns bytes enqueued or negative error. */
LWIP_SHIM_EXPORT int lwip_shim_write(lwip_stack_handle_t stack, lwip_conn_handle_t conn, const uint8_t* data, int len);

/* Get the send buffer space available for a connection. */
LWIP_SHIM_EXPORT int lwip_shim_sndbuf(lwip_stack_handle_t stack, lwip_conn_handle_t conn);

/* Close a connection gracefully. */
LWIP_SHIM_EXPORT void lwip_shim_close(lwip_stack_handle_t stack, lwip_conn_handle_t conn);

/* Abort a connection (sends RST). */
LWIP_SHIM_EXPORT void lwip_shim_abort(lwip_stack_handle_t stack, lwip_conn_handle_t conn);

/* ---- Timer / poll ---- */
/* Must be called periodically (e.g., every 1-5ms) to drive lwIP timers. */
LWIP_SHIM_EXPORT void lwip_shim_poll(lwip_stack_handle_t stack);

#ifdef __cplusplus
}
#endif

#endif /* LWIP_SHIM_H */
