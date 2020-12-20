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

#include "collector.hpp"

#include <queue>

namespace silkworm::etl {

void Collector::flush_buffer() {
    if (buffer_.size()) {
        buffer_.sort();
        data_providers_.emplace_back((int)data_providers_.size());
        data_providers_.back().write_buffer_to_disk(buffer_.get_entries());
        buffer_.reset();
    }
}

void Collector::collect(silkworm::ByteView key, silkworm::ByteView value) {
    buffer_.put(key, value);
    if (buffer_.check_flush_size()) {
        flush_buffer();
    }
}

void Collector::load(silkworm::lmdb::Table* table, Load load) {
    if (!data_providers_.size()) {
        buffer_.sort();
        for (const auto& entry : buffer_.get_entries()) {
            auto pairs{load(entry.key, entry.value)};
            for (const auto& pair : pairs) {
                table->put(pair.key, pair.value);
            }
        }
        buffer_.reset();
        return;
    }

    flush_buffer();

    // Define a priority queue based on smallest available key
    auto key_comparer = [](Entry left, Entry right) { return left.key > right.key; };
    std::priority_queue<Entry, std::vector<Entry>, decltype(key_comparer)> queue(key_comparer);

    // Read one "record" from each data_provider and let the queue
    // sort them. On top of the queue the smallest key
    for (auto& data_provider : data_providers_) {
        auto item{data_provider.read_entry()};
        if (item.key.size()) {
            queue.push(item);
        }
    }

    // Process the queue from smallest to largest key
    while (queue.size()) {
        auto& current{queue.top()};  // Pick smallest key by reference

        // Process linked pairs
        for (const auto& pair : load(current.key, current.value)) {
            table->put(pair.key, pair.value);
        }

        // From the provider which has served the current key
        // read next "record"
        auto next{data_providers_.at(current.i).read_entry()};

        // At this point `current` has been processed.
        // We can remove it from the queue
        queue.pop();

        // Add next item to the queue only if it has
        // meaningful data
        if (next.key.size()) {
            queue.push(next);
        } else {
            data_providers_.at(next.i).reset();
        }
    }
}

std::vector<Entry> default_load(silkworm::ByteView key, silkworm::ByteView value) {
    return std::vector<Entry>({{key, value, 0}});
}

}  // namespace silkworm::etl
