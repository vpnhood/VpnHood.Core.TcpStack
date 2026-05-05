cmake_minimum_required(VERSION 3.16)
project(lwip_shim C)

# Path to the lwIP source tree
set(LWIP_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../../../../_Test/lwip" CACHE PATH "Path to lwIP source")

# Resolve to absolute
get_filename_component(LWIP_DIR "${LWIP_DIR}" ABSOLUTE)

if(NOT EXISTS "${LWIP_DIR}/src/core/tcp.c")
    message(FATAL_ERROR "lwIP source not found at: ${LWIP_DIR}. Set LWIP_DIR to the lwIP root.")
endif()

# lwIP core sources (NO_SYS mode - no api/sockets needed)
set(LWIP_CORE_SRCS
    ${LWIP_DIR}/src/core/init.c
    ${LWIP_DIR}/src/core/def.c
    ${LWIP_DIR}/src/core/inet_chksum.c
    ${LWIP_DIR}/src/core/ip.c
    ${LWIP_DIR}/src/core/mem.c
    ${LWIP_DIR}/src/core/memp.c
    ${LWIP_DIR}/src/core/netif.c
    ${LWIP_DIR}/src/core/pbuf.c
    ${LWIP_DIR}/src/core/tcp.c
    ${CMAKE_CURRENT_SOURCE_DIR}/tcp_in_patched.c
    ${LWIP_DIR}/src/core/tcp_out.c
    ${LWIP_DIR}/src/core/timeouts.c
)

# IPv4 sources
set(LWIP_IPV4_SRCS
    ${LWIP_DIR}/src/core/ipv4/icmp.c
    ${LWIP_DIR}/src/core/ipv4/ip4.c
    ${LWIP_DIR}/src/core/ipv4/ip4_addr.c
    ${LWIP_DIR}/src/core/ipv4/ip4_frag.c
)

# IPv6 sources
set(LWIP_IPV6_SRCS
    ${LWIP_DIR}/src/core/ipv6/icmp6.c
    ${LWIP_DIR}/src/core/ipv6/ip6.c
    ${LWIP_DIR}/src/core/ipv6/ip6_addr.c
    ${LWIP_DIR}/src/core/ipv6/ip6_frag.c
    ${LWIP_DIR}/src/core/ipv6/nd6.c
    ${LWIP_DIR}/src/core/ipv6/mld6.c
    ${LWIP_DIR}/src/core/ipv6/inet6.c
)

# Our shim
set(SHIM_SRCS
    ${CMAKE_CURRENT_SOURCE_DIR}/lwip_shim.c
)

# Build as shared library (DLL on Windows)
add_library(lwip_shim SHARED
    ${LWIP_CORE_SRCS}
    ${LWIP_IPV4_SRCS}
    ${LWIP_IPV6_SRCS}
    ${SHIM_SRCS}
)

# Include directories
target_include_directories(lwip_shim PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}        # for lwipopts.h
    ${LWIP_DIR}/src/include            # lwIP headers
)

# Definitions
target_compile_definitions(lwip_shim PRIVATE
    LWIP_DEBUG=0
)

# Windows-specific settings
if(WIN32)
    target_compile_definitions(lwip_shim PRIVATE _CRT_SECURE_NO_WARNINGS)
endif()

# Output to a predictable location
set_target_properties(lwip_shim PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/../runtimes/win-x64/native"
    LIBRARY_OUTPUT_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/../runtimes/win-x64/native"
    ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/../runtimes/win-x64/native"
    OUTPUT_NAME "lwip_shim"
)

# Ensure release builds strip debug info
if(CMAKE_BUILD_TYPE STREQUAL "Release")
    if(MSVC)
        target_compile_options(lwip_shim PRIVATE /O2 /DNDEBUG)
    else()
        target_compile_options(lwip_shim PRIVATE -O2 -DNDEBUG)
    endif()
endif()
