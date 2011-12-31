/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "commons.h"
#include "TotalOrderPartitioner.h"

namespace NativeTask {

const char * TotalOrderPartitioner::PARTITION_FILE_NAME = "_nativepartition.lst";

struct Leaf {
  uint32_t pos;
};

struct Range {
  uint32_t lower;
  uint32_t upper;
};

struct Inner {
  uint32_t offset[256];
};

struct TrieNode {
  uint8_t type;
  union Data {
    Inner inner;
    Leaf leaf;
    Leaf range;
  };
};

void TotalOrderPartitioner::configure(Config & config) {

}

uint32_t TotalOrderPartitioner::getPartition(const char * key,
                                             uint32_t & keyLen,
                                             uint32_t numPartition) {
  return SearchTrie(_partitions, _trie);
}

uint32_t TotalOrderPartitioner::SearchTrie(const vector<string> & partitions, string & trie) {
  return 0;
}

void TotalOrderPartitioner::MakeTrie(const vector<string> & partitions, string & trie, uint32_t maxLevel) {

}

} // namespace NativeTask

