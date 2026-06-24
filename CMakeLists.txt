# SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
# SPDX-License-Identifier: Apache-2.0
#
# Maintainer: Andy Curtis <contactandyc@gmail.com>

# CMakeLists.txt for libraries (regular or header-only)
cmake_minimum_required(VERSION 3.20)

project(a_raft_core
  VERSION 0.1.4
  LANGUAGES C
)

if(CMAKE_PREFIX_PATH)
  include_directories("${CMAKE_PREFIX_PATH}/include")
  link_directories("${CMAKE_PREFIX_PATH}/lib")
endif()
# ── Variant selection for the umbrella alias ──────────────────────────────────
set(A_BUILD_VARIANT "debug" CACHE STRING
    "Umbrella choice for in-tree linking (debug|memory|static|shared)")
set_property(CACHE A_BUILD_VARIANT PROPERTY STRINGS debug memory static shared)

option(A_ENABLE_COVERAGE "Enable code coverage instrumentation for this build" OFF)
if(A_ENABLE_COVERAGE)
  if(CMAKE_C_COMPILER_ID MATCHES "Clang")
    add_compile_options(-O0 -g -fprofile-instr-generate -fcoverage-mapping)
    add_link_options(-fprofile-instr-generate -fcoverage-mapping)
  elseif(CMAKE_C_COMPILER_ID STREQUAL "GNU")
    add_compile_options(-O0 -g --coverage)
    add_link_options(--coverage)
  endif()
endif()

option(A_BUILD_ENABLE_MEMORY_PROFILE "Define a macro on the 'memory' variant" ON)
set(A_BUILD_MEMORY_DEFINE "_AML_DEBUG_" CACHE STRING
    "Macro to define on the 'memory' variant when memory profiling is enabled")

if(MSVC)
  set(_A_DEBUG_OPTS /Zi /Od)
  set(_A_RELEASE_OPTS /O2 /DNDEBUG)
else()
  set(_A_DEBUG_OPTS -O0 -g)
  set(_A_RELEASE_OPTS -O3 -DNDEBUG)
endif()

include(GNUInstallDirs)

# ---- Dependencies (Standard CMake) ----
find_package(libuv CONFIG REQUIRED)
find_package(h2o_c_library CONFIG REQUIRED)
find_package(the_lz4_library CONFIG REQUIRED)

# ---- Dependencies (PkgConfig Shims) ----

# ── Main Targets (Compiled Libraries) ─────────────────────────────────────────

add_library(a_raft_core_debug STATIC
  src/raft_wal.c
)

target_include_directories(a_raft_core_debug PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)

set_target_properties(a_raft_core_debug PROPERTIES
  C_STANDARD 23
  C_STANDARD_REQUIRED YES
  CXX_STANDARD 17
  CXX_STANDARD_REQUIRED YES
  POSITION_INDEPENDENT_CODE ON
)

target_link_libraries(a_raft_core_debug PUBLIC
  libuv::uv
  h2o_c_library::h2o_c_library
  the_lz4_library::the_lz4_library
)

target_compile_options(a_raft_core_debug PRIVATE ${_A_DEBUG_OPTS})


install(TARGETS a_raft_core_debug EXPORT a_raft_coreTargets
  ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
  LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
  RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
  INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
add_library(a_raft_core_memory STATIC
  src/raft_wal.c
)

target_include_directories(a_raft_core_memory PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)

set_target_properties(a_raft_core_memory PROPERTIES
  C_STANDARD 23
  C_STANDARD_REQUIRED YES
  CXX_STANDARD 17
  CXX_STANDARD_REQUIRED YES
  POSITION_INDEPENDENT_CODE ON
)

target_link_libraries(a_raft_core_memory PUBLIC
  libuv::uv
  h2o_c_library::h2o_c_library
  the_lz4_library::the_lz4_library
)

target_compile_options(a_raft_core_memory PRIVATE ${_A_DEBUG_OPTS})

if(A_BUILD_ENABLE_MEMORY_PROFILE)
  target_compile_definitions(a_raft_core_memory PUBLIC ${A_BUILD_MEMORY_DEFINE})
endif()

install(TARGETS a_raft_core_memory EXPORT a_raft_coreTargets
  ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
  LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
  RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
  INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
add_library(a_raft_core_static STATIC
  src/raft_wal.c
)

target_include_directories(a_raft_core_static PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)

set_target_properties(a_raft_core_static PROPERTIES
  C_STANDARD 23
  C_STANDARD_REQUIRED YES
  CXX_STANDARD 17
  CXX_STANDARD_REQUIRED YES
  POSITION_INDEPENDENT_CODE ON
)

target_link_libraries(a_raft_core_static PUBLIC
  libuv::uv
  h2o_c_library::h2o_c_library
  the_lz4_library::the_lz4_library
)

target_compile_options(a_raft_core_static PRIVATE ${_A_RELEASE_OPTS})


install(TARGETS a_raft_core_static EXPORT a_raft_coreTargets
  ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
  LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
  RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
  INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
add_library(a_raft_core_shared SHARED
  src/raft_wal.c
)

target_include_directories(a_raft_core_shared PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)

set_target_properties(a_raft_core_shared PROPERTIES
  C_STANDARD 23
  C_STANDARD_REQUIRED YES
  CXX_STANDARD 17
  CXX_STANDARD_REQUIRED YES
  POSITION_INDEPENDENT_CODE ON
)

target_link_libraries(a_raft_core_shared PUBLIC
  libuv::uv
  h2o_c_library::h2o_c_library
  the_lz4_library::the_lz4_library
)

target_compile_options(a_raft_core_shared PRIVATE ${_A_RELEASE_OPTS})


install(TARGETS a_raft_core_shared EXPORT a_raft_coreTargets
  ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
  LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
  RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
  INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})

string(REPLACE "-" "_" _variant_us "${A_BUILD_VARIANT}")
set(_sel_tgt "a_raft_core_${_variant_us}")
if(TARGET "${_sel_tgt}")
  add_library(a_raft_core::a_raft_core ALIAS ${_sel_tgt})
endif()


install(DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/include/"
        DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}")

install(EXPORT a_raft_coreTargets
  FILE a_raft_coreTargets.cmake
  NAMESPACE a_raft_core::
  DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/a_raft_core)

# ---- Package config with umbrella target -------------------------------------
set(_CFG_IN "${CMAKE_CURRENT_BINARY_DIR}/config.cmake.in")
file(WRITE "${_CFG_IN}" [=[
@PACKAGE_INIT@
include(CMakeFindDependencyMacro)


if(NOT DEFINED A_BUILD_VARIANT OR A_BUILD_VARIANT STREQUAL "")
  set(A_BUILD_VARIANT "debug")
endif()
string(REPLACE "-" "_" _picked_us "${A_BUILD_VARIANT}")

set(_save_variant "${A_BUILD_VARIANT}")
set(_deps "@A_BUILD_DEPS@")
foreach(_dep IN LISTS _deps)
  if(_dep)
    find_dependency("${_dep}" REQUIRED)
  endif()
endforeach()
set(A_BUILD_VARIANT "${_save_variant}")

include("${CMAKE_CURRENT_LIST_DIR}/a_raft_coreTargets.cmake")

set(_ns "@A_BUILD_EXPORT_NAMESPACE@")
set(_pkg "@A_BUILD_TARGET_BASENAME@")

# ONLY run target modifications if the imported target doesn't exist yet!
if(NOT TARGET ${_ns}::${_pkg})
  add_library(${_ns}::${_pkg} INTERFACE IMPORTED)

  set(_pick ${_ns}::${_pkg}_${_picked_us})
  if(TARGET "${_pick}")
    target_link_libraries(${_ns}::${_pkg} INTERFACE "${_pick}")
  endif()
endif()
]=])

set(A_BUILD_TARGET_BASENAME "a_raft_core")
set(A_BUILD_EXPORT_NAMESPACE "a_raft_core")
set(A_BUILD_DEPS "libuv;h2o_c_library;the_lz4_library")

include(CMakePackageConfigHelpers)
configure_package_config_file(
  "${_CFG_IN}"
  "${CMAKE_CURRENT_BINARY_DIR}/a_raft_coreConfig.cmake"
  INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/a_raft_core
)
write_basic_package_version_file(
  "${CMAKE_CURRENT_BINARY_DIR}/a_raft_coreConfigVersion.cmake"
  VERSION ${PROJECT_VERSION}
  COMPATIBILITY SameMajorVersion
)
install(FILES
  "${CMAKE_CURRENT_BINARY_DIR}/a_raft_coreConfig.cmake"
  "${CMAKE_CURRENT_BINARY_DIR}/a_raft_coreConfigVersion.cmake"
  DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/a_raft_core
)
# Extra project-specific targets


if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/tests")
  enable_testing()
  add_subdirectory("tests")
endif()

# Dynamically mount all subprojects (apps, examples, tests, etc.)
# # add_subdirectory(tests)
# 
