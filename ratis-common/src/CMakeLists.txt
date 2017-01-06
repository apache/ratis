#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

#
# CMake configuration.
#

cmake_minimum_required(VERSION 2.6 FATAL_ERROR)

list(APPEND CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/..)
include(RatisCommon)

# Source and test locations.
set(SRC main/native/src/org/apache/ratis)
set(TST main/native/src/test/org/apache/ratis)

#
# Main configuration.
#

# The caller must specify where the generated headers have been placed.
if(NOT GENERATED_JAVAH)
    message(FATAL_ERROR "You must set the CMake variable GENERATED_JAVAH")
endif()

# Configure JNI.
include(RatisJNI)

# Build hardware CRC32 acceleration, if supported on the platform.
if(CMAKE_SYSTEM_PROCESSOR MATCHES "^i.86$" OR CMAKE_SYSTEM_PROCESSOR STREQUAL "x86_64" OR CMAKE_SYSTEM_PROCESSOR STREQUAL "amd64")
  set(BULK_CRC_ARCH_SOURCE_FIlE "${SRC}/util/bulk_crc32_x86.c")
elseif(CMAKE_SYSTEM_PROCESSOR STREQUAL "aarch64")
  set(BULK_CRC_ARCH_SOURCE_FIlE "${SRC}/util/bulk_crc32_aarch64.c")
else()
  message("No HW CRC acceleration for ${CMAKE_SYSTEM_PROCESSOR}, falling back to SW")
endif()

# Check for platform-specific functions and libraries.
include(CheckFunctionExists)
include(CheckLibraryExists)
check_function_exists(sync_file_range HAVE_SYNC_FILE_RANGE)
check_function_exists(posix_fadvise HAVE_POSIX_FADVISE)
check_library_exists(dl dlopen "" NEED_LINK_DL)

# Configure the build.
include_directories(
    ${GENERATED_JAVAH}
    main/native/src
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/src
    ${CMAKE_BINARY_DIR}
    ${JNI_INCLUDE_DIRS}
    ${SRC}/util
)
configure_file(${CMAKE_SOURCE_DIR}/config.h.cmake ${CMAKE_BINARY_DIR}/config.h)

set(CMAKE_BUILD_WITH_INSTALL_RPATH TRUE)
ratis_add_dual_library(ratis
    main/native/src/exception.c
    ${SRC}/io/nativeio/NativeIO.c
    ${SRC}/io/nativeio/errno_enum.c
    ${SRC}/io/nativeio/file_descriptor.c
    ${SRC}/util/NativeCodeLoader.c
    ${SRC}/util/NativeCrc32.c
    ${SRC}/util/bulk_crc32.c
    ${BULK_CRC_ARCH_SOURCE_FIlE}
)
if(NEED_LINK_DL)
   set(LIB_DL dl)
endif()

ratis_target_link_dual_libraries(ratis ${LIB_DL} ${JAVA_JVM_LIBRARY})
set(LIBRATIS_VERSION "1.0.0")
set_target_properties(ratis PROPERTIES SOVERSION ${LIBRATIS_VERSION})
ratis_dual_output_directory(ratis target/usr/local/lib)

# By embedding '$ORIGIN' into the RPATH of libratis.so, dlopen will look in
# the directory containing libratis.so. However, $ORIGIN is not supported by
# all operating systems.
if(${CMAKE_SYSTEM_NAME} MATCHES "Linux|SunOS")
    set(RPATH "\$ORIGIN/")
    if(EXTRA_LIBRATIS_RPATH)
        set(RPATH "${RPATH}:${EXTRA_LIBRATIS_RPATH}/")
    endif()
    set_target_properties(ratis PROPERTIES INSTALL_RPATH "${RPATH}")
endif()

# Build the CRC32 test executable.
add_executable(test_bulk_crc32
    ${SRC}/util/bulk_crc32.c
    ${BULK_CRC_ARCH_SOURCE_FIlE}
    ${TST}/util/test_bulk_crc32.c
)
