cmake_minimum_required(VERSION 3.16)
project(lwip_shim C)

# Path to the lwIP source tree
set(LWIP_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../../../../_Test/lwip" CACHE PATH "Path to lwIP source")

# Determine output RID based on the target platform/ABI
if(CMAKE_SYSTEM_NAME STREQUAL "Android")
    if(ANDROID_ABI STREQUAL "arm64-v8a")
        set(RUNTIME_RID "android-arm64")
    elseif(ANDROID_ABI STREQUAL "armeabi-v7a")
        set(RUNTIME_RID "android-arm")
    elseif(ANDROID_ABI STREQUAL "x86_64")
        set(RUNTIME_RID "android-x64")
    elseif(ANDROID_ABI STREQUAL "x86")
        set(RUNTIME_RID "android-x86")
    else()
        message(FATAL_ERROR "Unsupported Android ABI: ${ANDROID_ABI}")
    endif()
elseif(WIN32)
    set(RUNTIME_RID "win-x64")
elseif(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    if(CMAKE_SYSTEM_PROCESSOR MATCHES "aarch64|arm64")
        set(RUNTIME_RID "linux-arm64")
    elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "arm")
        set(RUNTIME_RID "linux-arm")
    else()
        set(RUNTIME_RID "linux-x64")
    endif()
else()
    message(FATAL_ERROR "Unsupported platform: ${CMAKE_SYSTEM_NAME}")
endif()

message(STATUS "Target RID: ${RUNTIME_RID}")

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

# Platform-specific settings
if(WIN32)
    target_compile_definitions(lwip_shim PRIVATE _CRT_SECURE_NO_WARNINGS)
elseif(CMAKE_SYSTEM_NAME STREQUAL "Android")
    # Hide all symbols by default; only exported ones are visible
    target_compile_options(lwip_shim PRIVATE -fvisibility=hidden)
    # Android doesn't need a lib prefix for dlopen but CMake adds it automatically
endif()

# Output to a predictable RID-based location
set_target_properties(lwip_shim PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/../runtimes/${RUNTIME_RID}/native"
    LIBRARY_OUTPUT_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/../runtimes/${RUNTIME_RID}/native"
    ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/../runtimes/${RUNTIME_RID}/native"
    OUTPUT_NAME "lwip_shim"
)

# Release build optimizations
if(CMAKE_BUILD_TYPE STREQUAL "Release")
    if(MSVC)
        target_compile_options(lwip_shim PRIVATE /O2 /DNDEBUG)
    else()
        target_compile_options(lwip_shim PRIVATE -O2 -DNDEBUG -ffunction-sections -fdata-sections)
        if(NOT CMAKE_SYSTEM_NAME STREQUAL "Android")
            target_link_options(lwip_shim PRIVATE -Wl,--gc-sections)
        endif()
    endif()
endif()
