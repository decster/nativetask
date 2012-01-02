/*
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

#include "mempool.h"
#include "util/DualPivotQuickSort.h"

namespace NativeTask {

bool MemoryBlockPool::_inited = false;

uint32_t MemoryBlockPool::_min_block_size = 32 * 1024;

char * MemoryBlockPool::_base = NULL;

uint32_t MemoryBlockPool::_capacity = 0;

uint32_t MemoryBlockPool::_used = 0;

std::vector<MemoryBlock> MemoryBlockPool::_blocks = std::vector<MemoryBlock>();

bool MemoryBlockPool::init(uint32_t capacity, uint32_t min_block_size)
    throw (OutOfMemoryException) {
  _min_block_size = min_block_size;
  LOG("Native MemoryBlockPool: min_block_size %uK, capacity %uM",
      _min_block_size/1024,
      capacity/1024/1024);
  release();
  // 3GB at most
  assert(capacity<=(3<<30));
  capacity = GetCeil(capacity, _min_block_size);
  // add 8 bytes buffer tail to support simple_memcpy
  _base = (char*) malloc(capacity+sizeof(uint64_t));
  if (NULL == _base) {
    THROW_EXCEPTION(OutOfMemoryException, "Not enough memory to init MemoryBlockPool");
  }
  _capacity = capacity;
  _used = 0;
  _inited = true;
  return true;
}

void MemoryBlockPool::release() {
  if (NULL != _base) {
    free(_base);
  }
  _base = NULL;
  _capacity = 0;
  _used = 0;
  _blocks.clear();
  _inited = false;
}

void MemoryBlockPool::sort(std::vector<uint32_t> & kvpairs_offsets, SortType type) {
  switch (type) {
  case CQSORT:
    qsort(&kvpairs_offsets[0], kvpairs_offsets.size(), sizeof(uint32_t),
          compare_offset);
    break;
  case CPPSORT:
    std::sort(kvpairs_offsets.begin(), kvpairs_offsets.end(), OffsetLessThan());
    break;
  case DUALPIVOTSORT:
    DualPivotQuicksort(kvpairs_offsets, CompareOffset());
    break;
  default:
    THROW_EXCEPTION(UnsupportException, "SortType not support");
  }
}

void MemoryBlockPool::dump(FILE *out)
{
  uint32_t totalused=0;
  for (size_t i = 0; i < _blocks.size(); i++) {
    //fprintf(out, "%s\n", _blocks[i].str().c_str());
    totalused += _blocks[i].used();
  }
  fprintf(out,
      "Base: %p Capacity: %u Blocks: %lu Used: %u/%u %.3lf\n",
      _base,
      _capacity,
      _blocks.size(),
      totalused,
      _used,
      totalused / (double) _used);
}

} // namespace NativeTask

