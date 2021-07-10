/*
   Copyright 2021 The Silkworm Authors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

#ifndef SILKWORM_DB_MDBX_HPP_
#define SILKWORM_DB_MDBX_HPP_

#include <stdint.h>

#include <filesystem>
#include <string>

#include <silkworm/common/base.hpp>
#include <silkworm/common/util.hpp>
#include <silkworm/db/util.hpp>

#include "../libmdbx/mdbx.h++"

namespace silkworm::db {

constexpr std::string_view kDbDataFileName{"mdbx.dat"};
constexpr std::string_view kDbLockFileName{"mdbx.lck"};

struct EnvConfig {
    std::string path{};
    bool create{false};         // Whether or not db file must be created
    bool readonly{false};       // Whether or not db should be opened in RO mode
    bool exclusive{false};      // Whether or not this process has exclusive access
    bool inmemory{false};       // Whether or not this db is in memory
    bool shared{false};         // Whether or not this process opens a db already opened by another process
    uint32_t max_tables{128};   // Default max number of named tables
    uint32_t max_readers{100};  // Default max number of readers
};

struct MapConfig {
    const char* name{nullptr};                                        // Name of the table (is key in MAIN_DBI)
    const ::mdbx::key_mode key_mode{::mdbx::key_mode::usual};         // Key collation order
    const ::mdbx::value_mode value_mode{::mdbx::value_mode::single};  // Data Storage Mode
};

::mdbx::env_managed open_env(const EnvConfig& config);
::mdbx::map_handle open_map(::mdbx::txn& tx, const MapConfig& config);
::mdbx::cursor_managed open_cursor(::mdbx::txn& tx, const MapConfig& config);

static inline std::filesystem::path get_datafile_path(std::filesystem::path& base_path) noexcept {
    return std::filesystem::path(base_path / std::filesystem::path(kDbDataFileName));
}

static inline std::filesystem::path get_lockfile_path(std::filesystem::path& base_path) noexcept {
    return std::filesystem::path(base_path / std::filesystem::path(kDbLockFileName));
}

}  // namespace silkworm::db

#endif  // !SILKWORM_DB_MDBX_HPP_
