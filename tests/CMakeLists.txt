cmake_minimum_required(VERSION 3.16)
project(TinyKVServerTests LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include(FetchContent)
FetchContent_Declare(
    googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG v1.15.2
)
FetchContent_MakeAvailable(googletest)

enable_testing()

# 测试源码
file(GLOB_RECURSE TEST_SOURCES "unit/*.cpp" "integration/*.cpp")

# 生产源码 (排除 main.cpp)
set(PROD_SOURCES
    ${CMAKE_SOURCE_DIR}/../src/log/log.cpp
    ${CMAKE_SOURCE_DIR}/../src/config/config_loader.cpp
    ${CMAKE_SOURCE_DIR}/../src/config/yaml_parser.cpp
    ${CMAKE_SOURCE_DIR}/../src/storage/memory_store.cpp
    ${CMAKE_SOURCE_DIR}/../src/storage/wal.cpp
    ${CMAKE_SOURCE_DIR}/../src/storage/snapshot.cpp
    ${CMAKE_SOURCE_DIR}/../src/storage/persistent_memory_store.cpp
    ${CMAKE_SOURCE_DIR}/../src/protocol/kv_protocol.cpp
    ${CMAKE_SOURCE_DIR}/../src/cluster/consistent_hash.cpp
    ${CMAKE_SOURCE_DIR}/../src/cluster/router.cpp
    ${CMAKE_SOURCE_DIR}/../src/net/thread_pool.cpp
)

add_executable(tinykv_tests ${TEST_SOURCES} ${PROD_SOURCES})

target_include_directories(tinykv_tests PRIVATE
    ${CMAKE_SOURCE_DIR}/../
    ${CMAKE_SOURCE_DIR}/../src
    ${CMAKE_SOURCE_DIR}/../src/log
    ${CMAKE_SOURCE_DIR}/../src/config
    ${CMAKE_SOURCE_DIR}/../src/storage
    ${CMAKE_SOURCE_DIR}/../src/protocol
    ${CMAKE_SOURCE_DIR}/../src/cluster
    ${CMAKE_SOURCE_DIR}/../src/net
)

target_link_libraries(tinykv_tests
    gtest_main
    gmock_main
    pthread
)

include(GoogleTest)
gtest_discover_tests(tinykv_tests)
