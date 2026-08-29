# Copyright 2026 Yusheng Peng
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

cmake_minimum_required(VERSION 3.16)
project(mdds)

if(NOT CMAKE_CXX_STANDARD)
  set(CMAKE_CXX_STANDARD 17)
  set(CMAKE_CXX_STANDARD_REQUIRED ON)
endif()

if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
  add_compile_options(-Wall -Wextra -Wpedantic)
endif()

# DSoftBus backend: enabled automatically for the OHOS cross build, or
# explicitly with -DMDDS_WITH_DSOFTBUS=ON.
option(MDDS_WITH_DSOFTBUS "Build the DSoftBus socket transport backend" ${OHOS_CROSS_BUILD})

# ament/colcon build (ROS 2 workspace) vs standalone host build for unit tests.
find_package(ament_cmake_ros QUIET)

set(MDDS_SOURCES
  src/discovery.cpp
  src/frame.cpp
  src/fragment.cpp
  src/participant.cpp
  src/qos.cpp
  src/transport_udp.cpp
)

if(ament_cmake_ros_FOUND)
  add_library(${PROJECT_NAME} SHARED ${MDDS_SOURCES})
  target_include_directories(${PROJECT_NAME} PUBLIC
    "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>"
    "$<INSTALL_INTERFACE:include/${PROJECT_NAME}>"
  )
else()
  # Standalone host build (unit tests only): static lib, no ament.
  add_library(${PROJECT_NAME} STATIC ${MDDS_SOURCES})
  target_include_directories(${PROJECT_NAME} PUBLIC
    "${CMAKE_CURRENT_SOURCE_DIR}/include"
  )
  if(WIN32)
    target_link_libraries(${PROJECT_NAME} PUBLIC ws2_32)
  endif()
endif()

if(MDDS_WITH_DSOFTBUS)
  target_sources(${PROJECT_NAME} PRIVATE src/transport_dsoftbus.cpp)
  target_compile_definitions(${PROJECT_NAME} PUBLIC MDDS_WITH_DSOFTBUS=1)
  # Vendored DSoftBus SDK headers + board-pulled client/token libraries.
  # The same libraries exist on the device under /system/lib64/platformsdk.
  target_include_directories(${PROJECT_NAME} PRIVATE
    "${CMAKE_CURRENT_SOURCE_DIR}/third_party/dsoftbus/include"
  )
  target_link_directories(${PROJECT_NAME} PRIVATE
    "${CMAKE_CURRENT_SOURCE_DIR}/third_party/dsoftbus/lib"
  )
  target_link_libraries(${PROJECT_NAME} PRIVATE
    -l:libsoftbus_client.z.so
    -l:libnativetoken_shared.z.so
    -l:libtokensetproc_shared.z.so
  )
  target_link_options(${PROJECT_NAME} PRIVATE
    -Wl,--allow-shlib-undefined
    -Wl,-rpath,/system/lib64/platformsdk
  )
endif()

if(BUILD_TESTING)
  set(MDDS_TESTS
    test_frame
    test_fragment
    test_transport_udp
    test_participant
  )
  if(ament_cmake_ros_FOUND)
    find_package(ament_cmake_gtest REQUIRED)
    foreach(test_name IN LISTS MDDS_TESTS)
      ament_add_gtest(${test_name} test/${test_name}.cpp)
      if(TARGET ${test_name})
        target_link_libraries(${test_name} ${PROJECT_NAME})
      endif()
    endforeach()
  else()
    find_package(GTest QUIET)
    if(GTest_FOUND)
      enable_testing()
      foreach(test_name IN LISTS MDDS_TESTS)
        add_executable(${test_name} test/${test_name}.cpp)
        target_link_libraries(${test_name} ${PROJECT_NAME} GTest::gtest GTest::gtest_main)
        add_test(NAME ${test_name} COMMAND ${test_name})
      endforeach()
    else()
      message(STATUS "GTest not found; host unit tests disabled")
    endif()
  endif()
endif()

if(ament_cmake_ros_FOUND)
  ament_export_targets(export_${PROJECT_NAME})
  install(
    DIRECTORY include/
    DESTINATION include/${PROJECT_NAME}
  )
  install(
    TARGETS ${PROJECT_NAME}
    EXPORT export_${PROJECT_NAME}
    ARCHIVE DESTINATION lib
    LIBRARY DESTINATION lib
    RUNTIME DESTINATION bin
  )
  ament_package()
endif()
