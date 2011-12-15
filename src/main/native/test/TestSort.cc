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

#include "Streams.h"
#include "Buffers.h"
#include "lib/TeraSort.h"
#include "DualPivotQuickSort.h"
#include "test_commons.h"

string gBuffer;

inline const char * get_position(uint32_t offset) {
  return gBuffer.data() + offset;
}

/**
 * c qsort compare function
 */
static int compare_offset(const void * plh, const void * prh) {
  InplaceBuffer * lhb = (InplaceBuffer*) get_position(*(uint32_t*) plh);
  InplaceBuffer * rhb = (InplaceBuffer*) get_position(*(uint32_t*) prh);
  uint32_t minlen = std::min(lhb->length, rhb->length);
  int ret = fmemcmp(lhb->content, rhb->content, minlen);
  if (ret) {
    return ret;
  }
  return lhb->length - rhb->length;
}

/**
 * dualpivot sort compare function
 */
static inline int compare_offset_int(int32_t lhs, int32_t rhs) {
  InplaceBuffer * lhb = (InplaceBuffer*) get_position(lhs);
  InplaceBuffer * rhb = (InplaceBuffer*) get_position(rhs);
  uint32_t minlen = std::min(lhb->length, rhb->length);
  int ret = fmemcmp(lhb->content, rhb->content, minlen);
  if (ret) {
    return ret;
  }
  return lhb->length - rhb->length;
}

/**
 * dualpivot sort compare function
 */
class CompareOffset {
public:
  int operator()(uint32_t lhs, uint32_t rhs) {
    InplaceBuffer * lhb = (InplaceBuffer*) get_position(lhs);
    InplaceBuffer * rhb = (InplaceBuffer*) get_position(rhs);
    uint32_t minlen = std::min(lhb->length, rhb->length);
    int ret = fmemcmp(lhb->content, rhb->content, minlen);
    if (ret) {
      return ret;
    }
    return lhb->length - rhb->length;
  }
};


/**
 * quicksort compare function
 */
class OffsetLessThan {
public:
  bool operator()(uint32_t lhs, uint32_t rhs) {
    InplaceBuffer * lhb = (InplaceBuffer*) get_position(lhs);
    InplaceBuffer * rhb = (InplaceBuffer*) get_position(rhs);
    uint32_t minlen = std::min(lhb->length, rhb->length);
    int ret = fmemcmp(lhb->content, rhb->content, minlen);
    return ret < 0 || (ret == 0 && (lhb->length < rhb->length));
  }
};

void makeInput(string & dest, vector<uint32_t> & offsets, uint64_t length) {
  TeraGen tera = TeraGen(length/100,1,0);
  dest.reserve(length+1024);
  string k,v;
  while (tera.next(k, v)) {
    offsets.push_back(dest.length());
    uint32_t tempLen = k.length();
    dest.append((const char *)&tempLen, 4);
    dest.append(k.data(), k.length());
    tempLen = v.length();
    dest.append((const char *)&tempLen, 4);
    dest.append(v.data(), v.length());
  }
}

void makeInputWord(string & dest, vector<uint32_t> & offsets, uint64_t length) {
  Random r;
  dest.reserve(length+1024);
  string k,v;
  string range = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
  while (true) {
    k = r.nextBytes(16, range);
    v = r.nextBytes(8, range);
    offsets.push_back(dest.length());
    uint32_t tempLen = k.length();
    dest.append((const char *)&tempLen, 4);
    dest.append(k.data(), k.length());
    tempLen = v.length();
    dest.append((const char *)&tempLen, 4);
    dest.append(v.data(), v.length());
    if (dest.length() > length) {
      return;
    }
  }
}


void printOffsets(string & dest, vector<uint32_t> & offsets) {
  for (size_t i=0;i<offsets.size();i++) {
    InplaceBuffer * buf = (InplaceBuffer*)get_position(offsets[i]);
    printf("%s ", buf->str().c_str());
  }
  printf("\n");
}

TEST(Perf, sort) {
  vector<uint32_t> offsets;
  makeInput(gBuffer, offsets, 50000000);
  vector<uint32_t> offsetstemp1 = offsets;
  Timer timer;
  qsort(&offsetstemp1[0], offsetstemp1.size(), sizeof(uint32_t), compare_offset);
  LOG("%s", timer.getInterval("qsort").c_str());
  vector<uint32_t> offsetstemp2 = offsets;
  timer.reset();
  std::sort(offsetstemp2.begin(), offsetstemp2.end(), OffsetLessThan());
  LOG("%s", timer.getInterval("std::sort").c_str());
  vector<uint32_t> offsetstemp3 = offsets;
  timer.reset();
  DualPivotQuicksort(offsetstemp3, CompareOffset());
  LOG("%s", timer.getInterval("DualPivotQuicksort").c_str());

//  ASSERT_EQ(offsetstemp1, offsetstemp2);
//  ASSERT_EQ(offsetstemp2, offsetstemp3);
}
