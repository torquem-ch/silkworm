/*
   Copyright 2020 The Silkworm Authors

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

#include "analysis_cache.hpp"

#include <memory>
#include <utility>

#include "analysis.hpp"

namespace silkworm {

AnalysisCache& AnalysisCache::instance() noexcept {
    static AnalysisCache x{};
    return x;
}

std::shared_ptr<evmone::code_analysis> AnalysisCache::get(const evmc::bytes32& key, evmc_revision revision) noexcept {
    std::lock_guard lock(mutex_);

    if (revision_ == revision && cache_.exists(key)) {
        return cache_.get(key);
    } else {
        return nullptr;
    }
}

void AnalysisCache::put(const evmc::bytes32& key, const std::shared_ptr<evmone::code_analysis>& analysis,
                        evmc_revision revision) noexcept {
    std::lock_guard lock(mutex_);

    if (revision_ != revision) {
        // multiple revisions are not supported
        cache_.clear();
    }
    revision_ = revision;

    cache_.put(key, analysis);
}

}  // namespace silkworm
