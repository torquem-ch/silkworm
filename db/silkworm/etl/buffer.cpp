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

#include "buffer.hpp"

namespace silkworm::etl {

Buffer::Buffer(size_t optimal_size): optimal_size_(optimal_size) {
    entries_.reserve(kInitialBufferCap);

}

void Buffer::put(Entry& entry) {
    size_ += entry.size();
    entries_.push_back(std::move(entry));
}

void Buffer::sort() {
    std::sort(entries_.begin(), entries_.end(),
              [](const Entry& a, const Entry& b) { 
                auto diff{a.key.compare(b.key)};
                if (diff == 0) {
                    return a.value.compare(b.value) < 0;
                }
                return diff < 0; 
                });
}

size_t Buffer::size() const noexcept { return size_; }

std::vector<Entry>& Buffer::get_entries() { return entries_; }

void Buffer::clear() {
    std::vector<Entry>().swap(entries_);
    size_ = 0;
}

bool Buffer::overflows() const noexcept { return size_ >= optimal_size_; }
}  // namespace silkworm::etl
