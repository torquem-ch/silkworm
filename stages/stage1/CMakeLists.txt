#[[
   Copyright 2020-2021 The Silkworm Authors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
]]

if(MSVC)
  add_link_options(/STACK:0x1000000)
elseif (${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
  add_link_options(-Wl,-stack_size -Wl,0x1000000)
else()
  add_link_options(-Wl,-z,stack-size=10000000)
endif()

if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++20 -fcoroutines")
endif()

find_package(CLI11 CONFIG REQUIRED)

find_package(gRPC CONFIG REQUIRED)

file(GLOB_RECURSE STAGE1_SRC CONFIGURE_DEPENDS "*.cpp" "*.cc" "*.hpp" "*.c" "*.h")
list(FILTER STAGE1_SRC EXCLUDE REGEX "main\.cpp$|_test\.cpp$")

add_library(silkworm_stage1 ${STAGE1_SRC}
                           "${CMAKE_SOURCE_DIR}/interfaces/sentry.grpc.pb.cc"
                           "${CMAKE_SOURCE_DIR}/interfaces/sentry.pb.cc"
                           "${CMAKE_SOURCE_DIR}/interfaces/types.pb.cc")

add_dependencies(silkworm_stage1 generate_sentry_grpc)

set_source_files_properties("${CMAKE_SOURCE_DIR}/interfaces/sentry.grpc.pb.cc" PROPERTIES GENERATED TRUE)
set_source_files_properties("${CMAKE_SOURCE_DIR}/interfaces/sentry.pb.cc" PROPERTIES GENERATED TRUE)
set_source_files_properties("${CMAKE_SOURCE_DIR}/interfaces/types.pb.cc" PROPERTIES GENERATED TRUE)

# see https://samthursfield.wordpress.com/2015/11/21/cmake-dependencies-between-targets-and-files-and-custom-commands/

target_link_libraries(silkworm_stage1 PUBLIC silkworm_db
                                             silkworm_tg_api
                                             CLI11::CLI11
                                             gRPC::grpc++
                                             protobuf::libprotobuf)

target_include_directories(silkworm_stage1 PUBLIC ${CMAKE_SOURCE_DIR}
                                                  "${CMAKE_SOURCE_DIR}/interfaces"
                                                  messages
                                                  rpc)


# test
find_package(Catch2 CONFIG REQUIRED)

file(GLOB_RECURSE STAGE1_TESTS CONFIGURE_DEPENDS "*_test.cpp")

add_executable(stage1_test "${CMAKE_SOURCE_DIR}/cmd/unit_test.cpp"
                            ${STAGE1_TESTS})
target_link_libraries(stage1_test silkworm_stage1 Catch2::Catch2)

if(MSVC)
  target_compile_options(stage1_test PRIVATE /EHsc)
else()
  target_compile_options(stage1_test PRIVATE -fno-exceptions)
endif()