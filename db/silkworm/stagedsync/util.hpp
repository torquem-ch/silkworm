/*
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
*/
#include <silkworm/common/base.hpp>
#ifndef SILKWORM_STAGEDSYNC_UTIL_HPP_
#define SILKWORM_STAGEDSYNC_UTIL_HPP_

/*
Part of the compatibility layer with the Erigon DB format;
see its package dbutils.
*/

namespace silkworm::stagedsync {

enum class [[nodiscard]] StageResult {
    kStageSuccess,
    kStageBadBlockHash,
    kStageBadChainSequence,
    kStageInvalidRange,
    kStageAborted
};

void check_stagedsync_error(StageResult code);

// Convert changesets key and value pair to plain state format
std::pair<Bytes, Bytes> convert_to_db_format(const ByteView& key, const ByteView& value);

}  // namespace silkworm::db

#endif  // SILKWORM_DB_UTIL_HPP_
