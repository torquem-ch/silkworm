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

#ifndef SILKWORM_SINGLETON_HPP
#define SILKWORM_SINGLETON_HPP

#include <memory>

namespace owning {
template <class T>
class Singleton {
    static inline std::unique_ptr<T> instance_;
  public:
    static void instance(std::unique_ptr<T> instance) {instance_ = instance;}
    static T& instance() {return *instance_;}
};
}

namespace non_owning {
template <class T>
class Singleton {
    static inline T* instance_;
  public:
    static void instance(T* instance) {instance_ = instance;}
    static T& instance() {return *instance_;}
};
}
#endif  // SILKWORM_SINGLETON_HPP
