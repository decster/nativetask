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

#include "test_commons.h"
#include "lib/TotalOrderPartitioner.h"

static string GetKey(uint64_t key, uint64_t range) {
  if (range>=1000000) {
    return StringUtil::Format("%07d", key);
  } else if (range>=100000) {
    return StringUtil::Format("%06d", key);
  } else if (range>=10000) {
    return StringUtil::Format("%05d", key);
  } else if (range>=1000) {
    return StringUtil::Format("%04d", key);
  } else if (range>=100) {
    return StringUtil::Format("%03d", key);
  } else if (range>=10) {
    return StringUtil::Format("%02d", key);
  } else if (range>=1) {
    return StringUtil::Format("%d", key);
  }
}

static void MakeSplits(vector<string> & splits, int64_t partition, int64_t record) {
  for (int64_t i = 1; i < partition; i++) {
    splits.push_back(GetKey(i*record, partition*record));
  }
}

static void TestTrieSearch(int64_t partition, int64_t record) {
  vector<string> splits;
  MakeSplits(splits, partition, record);
  string trie;
  TotalOrderPartitioner::MakeTrie(splits, trie, 2);
  for (int64_t i=0 ; i < partition*record; i++) {
    string key = GetKey(i, partition*record);
    uint32_t part = TotalOrderPartitioner::SearchTrie(splits, trie, key.c_str(), key.length());
    ASSERT_EQ(i/record, part);
  }
}

TEST(TotalOrderPartitioner, Trie) {
  int64_t partition = TestConfig.getInt("input.partition", 100);
  int64_t record = TestConfig.getInt("record.per.partition", 100);
  TestTrieSearch(partition, record);
}

static void TestRandomTrieSearch(int64_t seed, int64_t size, int64_t partition,
                                 uint32_t maxlevel) {
  Random rand(seed);
  string range;
  for (int i='A';i<'Z';i++) {
    range.append(1, (char)i);
  }
  set<string> all;
  while (all.size()<size) {
    all.insert(rand.nextBytes(rand.next_int32(10), range));
  }
  vector<string> allarray;
  std::copy(all.begin(), all.end(), std::back_inserter(allarray));
  all.clear();
  vector<string> splits;
  for (int64_t i=1;i<partition;i++) {
    splits.push_back(allarray[size*i/partition]);
  }
//  LOG("Splits:")
//  for (size_t i=0;i<splits.size();i++) {
//    LOG("%lu [%s]", i, splits[i].c_str());
//  }
  string trie;
  TotalOrderPartitioner::MakeTrie(splits, trie, maxlevel);
//  TotalOrderPartitioner::PrintTrie(splits, trie);
  Timer timer;
  for (size_t i=0;i<allarray.size();i++) {
    string & key = allarray[i];
    uint32_t p = TotalOrderPartitioner::SearchTrie(splits, trie, key.c_str(), key.length());
    if (p==0) {
      int64_t ret = fmemcmp(key.data(), splits[0].data(), key.length(), splits[0].length());
      ASSERT_TRUE(ret<0);
    } else if (p<partition-1) {
      string & lower = splits[p-1];
      string & upper = splits[p];
//      LOG("[%s]<=[%s]<[%s]", lower.c_str(), key.c_str(), upper.c_str());
      int64_t ret1 = fmemcmp(key.data(), lower.data(), key.length(), lower.length());
      ASSERT_TRUE(ret1>=0);
      int64_t ret2 = fmemcmp(key.data(), upper.data(), key.length(), upper.length());
      ASSERT_TRUE(ret2<0);
    } else if (p==partition-1) {
      int64_t ret = fmemcmp(key.data(), splits[p-1].data(), key.length(), splits[p-1].length());
      ASSERT_TRUE(ret>=0);
    } else {
      FAIL();
    }
  }
  LOG("Size: %lld Partition: %lld Level: %u Space: %lu Time: %.3lf",
      size, partition, maxlevel, trie.size(), (timer.now()-timer.last())/1000000000.0);
}

TEST(TotalOrderPartitioner, TrieRandomSearch) {
  int64_t seed = TestConfig.getInt("input.seed", -1);
  int64_t size = TestConfig.getInt("input.size", 100000);
  for (uint32_t level = 1; level < 5; level++) {
    for (int64_t partition = 10; partition <= 1000; partition *= 10) {
      TestRandomTrieSearch(seed, size, partition, level);
    }
  }
}

