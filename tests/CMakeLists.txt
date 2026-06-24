# SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
# SPDX-License-Identifier: Apache-2.0
#
# Maintainer: Andy Curtis <contactandyc@gmail.com>

# CMakeLists.txt for tests
cmake_minimum_required(VERSION 3.20)

project(a_raft_core_tests LANGUAGES C)

if(CMAKE_PREFIX_PATH)
  include_directories("${CMAKE_PREFIX_PATH}/include")
  link_directories("${CMAKE_PREFIX_PATH}/lib")
endif()

set(A_BUILD_VARIANT "debug" CACHE STRING
    "Variant to link via a_raft_core::a_raft_core (debug|memory|static|shared)")
set_property(CACHE A_BUILD_VARIANT PROPERTY STRINGS debug memory static shared)

option(A_ENABLE_COVERAGE "Enable code coverage instrumentation" OFF)

# ---- Determine compiler flags based on the variant ----
if(MSVC)
  set(_A_DEBUG_OPTS /Zi /Od)
  set(_A_RELEASE_OPTS /O2 /DNDEBUG)
else()
  set(_A_DEBUG_OPTS -O0 -g)
  set(_A_RELEASE_OPTS -O3 -DNDEBUG)
endif()

if(A_BUILD_VARIANT STREQUAL "debug" OR A_BUILD_VARIANT STREQUAL "memory")
  set(TEST_COMPILER_OPTS ${_A_DEBUG_OPTS})
else()
  set(TEST_COMPILER_OPTS ${_A_RELEASE_OPTS})
endif()

find_library(M_LIB m)

find_package(libuv CONFIG REQUIRED)
find_package(h2o_c_library CONFIG REQUIRED)
find_package(the_lz4_library CONFIG REQUIRED)

# Fallback for standalone test builds (when not included via add_subdirectory)
if(NOT TARGET a_raft_core::a_raft_core)
  find_package(a_raft_core CONFIG REQUIRED)
endif()

set(TEST_EXECUTABLES "")

# ---- Targets ----
# ==============================================================================
# test_raft_wal Target (Standard Test)
# ==============================================================================
add_executable(test_raft_wal
  src/test_raft_wal.c
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/raft_wal.c
)

target_include_directories(test_raft_wal BEFORE PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}/../src
  ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

list(APPEND TEST_EXECUTABLES test_raft_wal)

set_target_properties(test_raft_wal PROPERTIES
  C_STANDARD 23
  C_STANDARD_REQUIRED YES
  CXX_STANDARD 17
  CXX_STANDARD_REQUIRED YES
)

target_link_libraries(test_raft_wal PRIVATE libuv::uv)
target_link_libraries(test_raft_wal PRIVATE h2o_c_library::h2o_c_library)
target_link_libraries(test_raft_wal PRIVATE the_lz4_library::the_lz4_library)
target_link_libraries(test_raft_wal PRIVATE a_raft_core::a_raft_core)

if(M_LIB)
  target_link_libraries(test_raft_wal PRIVATE ${M_LIB})
endif()

if(MSVC)
  target_compile_options(test_raft_wal PRIVATE /W4 ${TEST_COMPILER_OPTS})
else()
  target_compile_options(test_raft_wal PRIVATE -Wall -Wextra -Wpedantic ${TEST_COMPILER_OPTS})
endif()

if(A_ENABLE_COVERAGE)
  if (CMAKE_C_COMPILER_ID MATCHES "Clang")
    target_compile_options(test_raft_wal PRIVATE -O0 -g -fprofile-instr-generate -fcoverage-mapping)
    target_link_options(test_raft_wal PRIVATE -fprofile-instr-generate -fcoverage-mapping)
  elseif (CMAKE_C_COMPILER_ID STREQUAL "GNU")
    target_compile_options(test_raft_wal PRIVATE -O0 -g --coverage)
    target_link_options(test_raft_wal PRIVATE --coverage)
  endif()
endif()

add_test(NAME test_raft_wal COMMAND $<TARGET_FILE:test_raft_wal>)

enable_testing()

# ---- Coverage aggregation ----
add_custom_target(coverage_report COMMENT "Generate coverage report")

if(A_ENABLE_COVERAGE AND CMAKE_C_COMPILER_ID MATCHES "Clang")
  include(ProcessorCount)
  ProcessorCount(NPROC)
  if(NPROC EQUAL 0)
    set(NPROC 4)
  endif()

  add_custom_command(TARGET coverage_report PRE_BUILD
    COMMAND ${CMAKE_CTEST_COMMAND} --output-on-failure -j ${NPROC}
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
    COMMENT "Running tests with coverage instrumentation"
  )

  find_program(LLVM_PROFDATA_EXECUTABLE llvm-profdata)
  find_program(LLVM_COV_EXECUTABLE llvm-cov)
  if(LLVM_PROFDATA_EXECUTABLE AND LLVM_COV_EXECUTABLE)
    add_custom_command(TARGET coverage_report POST_BUILD
      COMMAND ${LLVM_PROFDATA_EXECUTABLE} merge -sparse *.profraw -o default.profdata
      WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
      COMMENT "Merging coverage data"
    )

    # Normal compiled library path
    add_custom_command(TARGET coverage_report POST_BUILD
      COMMAND ${LLVM_COV_EXECUTABLE} show
              ../liba_raft_core_${A_BUILD_VARIANT}.a
              -instr-profile=default.profdata
              -format=html -output-dir=coverage_html
              --ignore-filename-regex='/usr/.*'
              --show-regions --show-line-counts-or-regions
      WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
      COMMENT "Generated LLVM coverage report → open coverage_html/index.html"
    )
  endif()
endif()
