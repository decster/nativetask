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
#include "WritableUtils.h"
#include "FileSystem.h"
#include "TotalOrderPartitioner.h"

namespace NativeTask {

const char * TotalOrderPartitioner::PARTITION_FILE_NAME = "_nativepartition.lst";

// index (lower=0xffffffff, upper == offset)
// range (lower <= upper)
struct Range {
  uint32_t lower;
  uint32_t upper;
};

struct TrieNode {
  Range offset[256];
};

inline TrieNode * GetNode(string & trie, uint32_t offset) {
  return (TrieNode*) (trie.data() + offset);
}

static uint32_t MakeTrieInner(string & trie, vector<string> & splits,
                              uint32_t lower, uint32_t upper, string & prefix,
                              uint32_t maxDepth) {
  uint32_t depth = prefix.length();
  uint32_t ret = trie.length();
  trie.append(sizeof(TrieNode), '\0');
  TrieNode * node = GetNode(trie, ret);
  string trial = prefix;
  trial.append(1, '\0');
  // append an extra byte on to the prefix
  int currentBound = lower;
  for (int ch = 0; ch < 255; ++ch) {
    trial[depth] = (char)(ch + 1);
    lower = currentBound;
    while (currentBound < upper) {
      if (fmemcmp(splits[currentBound].c_str(), trial.c_str(),
                  splits[currentBound].length(), trial.length()) >= 0) {
        break;
      }
      currentBound += 1;
    }
    trial[depth] = ch;
    if (lower == currentBound || trial.length() >= maxDepth) {
      node->offset[ch].lower = lower;
      node->offset[ch].upper = currentBound;
    }
    else {
      node->offset[ch].lower = 0xffffffff;
      node->offset[ch].upper = MakeTrieInner(trie, splits, lower, currentBound,
                                             trial, maxDepth);
    }
  }
  // pick up the rest
  trial[depth] = 127;
  if (currentBound == upper || trial.length() >= maxDepth) {
    node->offset[255].lower = currentBound;
    node->offset[255].upper = upper;
  }
  else {
    node->offset[255].lower = 0xffffffff;
    node->offset[255].upper = MakeTrieInner(trie, splits, currentBound, upper,
                                            trial, maxDepth);
  }
  return ret;
}

class BinaryStringCompare {
public:
  bool operator()(const string & lhs, const string & rhs) {
    size_t minlen = std::min(lhs.length(), rhs.length());
    int ret = fmemcmp(lhs.data(), rhs.data(), minlen);
    return ret < 0 || (ret == 0 && (lhs.length() < rhs.length()));
  }
};

static uint32_t SearchTrieInner(vector<string> & splits, string & trie,
                                uint32_t nodeOffset, uint32_t level,
                                const char * key, uint32_t keyLen) {
  if (splits.size()==0) {
    return 0;
  }
  TrieNode * node = GetNode(trie, nodeOffset);
  Range & range = node->offset[(uint8_t)(level<keyLen ? key[level] : 0)];
  if (range.lower == 0xffffffff) {
    return SearchTrieInner(splits, trie, range.upper, level+1, key, keyLen);
  }
  else {
    uint32_t retmax = splits.size();
    if (range.lower >= retmax) {
      return retmax;
    }
    if (range.lower == range.upper) {
      string & p = splits[range.lower];
      if (fmemcmp(key, p.data(), keyLen, p.length())>=0) {
        return range.lower + 1;
      } else {
        return range.lower;
      }
    } else {
      // binary search
      uint32_t len = range.upper - range.lower;
      uint32_t current = range.lower;
      while (true) {
        size_t half = len >> 1;
        size_t middle = current + half;
        int64_t ret = fmemcmp(key, splits[middle].data(), keyLen, splits[middle].length());
        if (ret < 0) {
          len = half;
          if (len==0) {
            return current;
          }
        }
        else if (ret==0) {
          return middle + 1;
        }
        else {
          len = len - half - 1;
          current = middle + 1;
          if (len == 0) {
            return current;
          }
        }
      }
    }
  }
  THROW_EXCEPTION(IOException, "Logic errir, can not get here");
}

void TotalOrderPartitioner::configure(Config & config) {
  string path = config.get("total.order.partitioner.path", PARTITION_FILE_NAME);
  uint32_t maxDepth = config.getInt("total.order.partitioner.max.trie.depth", 2);
  InputStream * is = FileSystem::getRaw().open(path);
  _splits.clear();
  LoadPartitionFile(_splits, is);
  delete is;
  uint32_t numPartition = config.getInt("mapred.reduce.tasks", 1);
  if (_splits.size() + 1 != numPartition) {
    THROW_EXCEPTION(IOException, "splits in partition file does not match mapred.reduce.tasks");
  }
  MakeTrie(_splits, _trie, maxDepth);
}

uint32_t TotalOrderPartitioner::getPartition(const char * key,
                                             uint32_t & keyLen,
                                             uint32_t numPartition) {
  return SearchTrieInner(_splits, _trie, 0, 0, key, keyLen);
}

uint32_t TotalOrderPartitioner::SearchTrie(vector<string> & splits,
                                           string & trie, const char * key,
                                           uint32_t keyLen) {
  return SearchTrieInner(splits, trie, 0, 0, key, keyLen);
}

void TotalOrderPartitioner::LoadPartitionFile(vector<string> & splits, InputStream * is) {
  uint32_t partition = WritableUtils::ReadInt(is);
  for (uint32_t i = 1; i < partition; i++) {
    string s = WritableUtils::ReadText(is);
    splits.push_back(s);
  }
}

void TotalOrderPartitioner::MakeTrie(vector<string> & splits, string & trie,
                                     uint32_t maxDepth) {
  trie.clear();
  if (splits.size() == 0) {
    return;
  }
  size_t size = 1;
  for (uint32_t i = 1; i < maxDepth; i++) {
    size *= 256;
  }
  trie.reserve(sizeof(TrieNode) * size);
  string prefix = "";
  MakeTrieInner(trie, splits, 0, splits.size(), prefix, maxDepth);
}

void TotalOrderPartitioner::PrintTrie(vector<string> & splits, string & trie, uint32_t pos, uint32_t indent) {
  TrieNode * node = GetNode(trie, pos);
  string indexStr = string(indent * 2, ' ');
  for (int i = 0; i < 256; i++) {
    Range & range = node->offset[i];
    if (range.lower == 0xffffffff) {
      LOG("%sInnerNode[%6u]", indexStr.c_str(), range.upper);
      PrintTrie(splits, trie, range.upper, indent + 2);
    }
    else {
      if (range.lower == range.upper) {
        LOG("%sLeaf[%4d][%s]", indexStr.c_str(), range.lower, splits[range.lower].c_str());
      }
      else {
        LOG("%sLeaf[%4d][%s]-[%5d][%s]",
            indexStr.c_str(),
            range.lower, splits[range.lower].c_str(),
            range.upper, range.upper==splits.size() ? "<NULL>":splits[range.upper].c_str());
      }
    }
  }
}

} // namespace NativeTask

