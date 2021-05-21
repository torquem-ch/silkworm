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

find_package(Catch2 CONFIG REQUIRED)

if(MSVC)
  add_link_options(/STACK:0x1000000)
elseif (${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
  add_link_options(-Wl,-stack_size -Wl,0x1000000)
else()
  add_link_options(-Wl,-z,stack-size=10000000)
endif()

file(GLOB_RECURSE SILKWORM_CORE_TESTS CONFIGURE_DEPENDS "${CMAKE_SOURCE_DIR}/core/silkworm/*_test.cpp")
add_executable(core_test unit_test.cpp ${SILKWORM_CORE_TESTS})
target_link_libraries(core_test silkworm_core Catch2::Catch2)
if(MSVC)
  target_compile_options(core_test PRIVATE /EHsc)
else()
  target_compile_options(core_test PRIVATE -fno-exceptions)
endif()

if(NOT SILKWORM_CORE_ONLY)

  find_package(absl CONFIG REQUIRED)

  file(GLOB_RECURSE SILKWORM_DB_TESTS CONFIGURE_DEPENDS "${CMAKE_SOURCE_DIR}/db/silkworm/*_test.cpp")
  add_executable(db_test unit_test.cpp ${SILKWORM_DB_TESTS})
  target_link_libraries(db_test silkworm_db Catch2::Catch2)

  add_executable(check_changes check_changes.cpp)
  target_link_libraries(check_changes PRIVATE silkworm_db CLI11::CLI11 absl::time)

  add_executable(scan_txs scan_txs.cpp)
  target_link_libraries(scan_txs PRIVATE silkworm_db CLI11::CLI11 absl::time)

  find_package(CLI11 CONFIG REQUIRED)

  add_executable(check_senders check_senders.cpp)
  target_link_libraries(check_senders PRIVATE silkworm_db CLI11::CLI11)

  add_executable(check_pow check_pow.cpp)
  target_link_libraries(check_pow PRIVATE silkworm_db CLI11::CLI11)

  add_executable(dbtool dbtool.cpp)
  target_link_libraries(dbtool PRIVATE silkworm_db CLI11::CLI11)

  add_executable(execute execute.cpp)
  target_link_libraries(execute PRIVATE silkworm_db silkworm_tg_api CLI11::CLI11)

  add_executable(blockhashes blockhashes.cpp)
  target_link_libraries(blockhashes PRIVATE silkworm_db CLI11::CLI11)
  
  add_executable(hashstate hashstate.cpp)
  target_link_libraries(hashstate PRIVATE silkworm_db CLI11::CLI11)

  add_executable(check_hashstate check_hashstate.cpp)
  target_link_libraries(check_hashstate PRIVATE silkworm_db CLI11::CLI11)

  add_executable(tx_lookup tx_lookup.cpp)
  target_link_libraries(tx_lookup PRIVATE silkworm_db CLI11::CLI11)

  add_executable(check_tx_lookup check_tx_lookup.cpp)
  target_link_libraries(check_tx_lookup PRIVATE silkworm_db CLI11::CLI11)

  add_executable(history_index history_index.cpp)
  target_link_libraries(history_index PRIVATE silkworm_db CLI11::CLI11)

  add_executable(initialize_with_genesis initialize_with_genesis.cpp)
  target_link_libraries(initialize_with_genesis PRIVATE silkworm_db CLI11::CLI11)

  add_executable(check_blockhashes check_blockhashes.cpp)
  target_link_libraries(check_blockhashes PRIVATE silkworm_db CLI11::CLI11)

  add_executable(extract_headers extract_headers.cpp)
  target_link_libraries(extract_headers PRIVATE silkworm_db CLI11::CLI11)

  add_executable(log_index log_index.cpp)
  target_link_libraries(log_index PRIVATE silkworm_db cborcpp CLI11::CLI11)

  find_package(gRPC CONFIG REQUIRED)
  add_executable(download_headers download_headers.cpp)
  target_link_libraries(download_headers PRIVATE silkworm_db silkworm_tg_api silkworm_stage1 CLI11::CLI11)
  target_include_directories(download_headers PRIVATE ${CMAKE_SOURCE_DIR})

  # Ethereum Consensus Tests
  find_package(nlohmann_json CONFIG REQUIRED)
  add_executable(consensus consensus.cpp)
  target_compile_definitions(consensus PRIVATE SILKWORM_CONSENSUS_TEST_DIR="${CMAKE_SOURCE_DIR}/tests")
  target_link_libraries(consensus PRIVATE silkworm_core nlohmann_json::nlohmann_json evmc::loader CLI11::CLI11)

  # Benchmarks
  find_package(benchmark CONFIG REQUIRED)
  add_executable(benchmark_precompile benchmark_precompile.cpp)
  target_link_libraries(benchmark_precompile silkworm_core benchmark::benchmark)

endif(NOT SILKWORM_CORE_ONLY)
