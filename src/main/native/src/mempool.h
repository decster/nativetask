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

#ifndef MEMORYPOOL_H_
#define MEMORYPOOL_H_

#include "commons.h"
#include "Buffers.h"
#include "MapOutputSpec.h"

namespace NativeTask {

/**
 * A block of memory used to store small(relatively) Buffers,
 * increase cpu/cache affinity when perform sort, compression, spill
 */
class MemoryBlock {
  friend class MemoryBlockPool;
private:
  uint32_t _size;
  uint32_t _used;
  char * _pos;

public:
  MemoryBlock(uint32_t size, char * pos) :
      _size(size),
      _used(0),
      _pos(pos) {
  }

  const uint32_t size() const {
    return _size;
  }

  char * start() const {
    return _pos;
  }

  const uint32_t used() const {
    return _used;
  }

  inline char * position() const {
    return _pos + _used;
  }

  inline const uint32_t rest() const {
    return _size - _used;
  }

  char * put(void * obj, uint32_t size) {
    assert(_used+size<=_size);
    char * ret = _pos + _used;
    memcpy(ret, obj, size);
    _used += size;
    return ret;
  }

  std::string str() {
    char buff[1024];
    snprintf(buff, 1024, "%016llx: %u/%u", (unsigned long long) _pos, _used, _size);
    return std::string(buff);
  }

};


inline uint32_t GetCeil(uint32_t v, uint32_t unit) {
  return ((v + unit - 1) / unit) * unit;
}

const uint32_t DEFAULT_MIN_BLOCK_SIZE = 16 * 1024;
const uint32_t DEFAULT_MAX_BLOCK_SIZE = 1 * 1024 * 1024;
const uint32_t NULL_BLOCK_INDEX = 0xffffffffU;

/**
 * Class for allocating and manage MemoryBlocks
 */
class MemoryBlockPool {
private:
  static bool _inited;
  static uint32_t _min_block_size;
  static char * _base;
  static uint32_t _capacity;
  static uint32_t _used;
  static std::vector<MemoryBlock> _blocks;

public:
  static bool init(uint32_t capacity,
      uint32_t min_block_size = DEFAULT_MIN_BLOCK_SIZE)
      throw (OutOfMemoryException);

  static void release();

  static void clear() {
    _blocks.clear();
    _used = 0;
  }

  static bool inited() {
    return _inited;
  }

  static MemoryBlock & get_block(uint32_t idx) {
    assert(_blocks.size() > idx);
    return _blocks[idx];
  }

  static char * get_position(uint32_t offset) {
    return _base + offset;
  }

  static uint32_t get_offset(void * pos) {
    assert((char*)pos >= _base);
    return (uint32_t) ((char*) pos - _base);
  }

  static char * alloc_block(uint32_t & current_block_idx, uint32_t size) {
    uint32_t newsize = GetCeil(size+8, _min_block_size);
    assert(newsize%_min_block_size==0);
    if (size > _capacity) {
      THROW_EXCEPTION(OutOfMemoryException, "size larger than io.sort.mb");
    }
    if (_used + size > _capacity) {
      return NULL;
    }
    if (_used + newsize > _capacity) {
      _blocks.push_back(MemoryBlock(_capacity - _used, _base + _used));
      _used = _capacity;
    }
    else {
      _blocks.push_back(MemoryBlock(newsize-8, _base + _used));
      _used += newsize;
    }
    current_block_idx = _blocks.size() - 1;
    MemoryBlock & blk = _blocks[current_block_idx];
    char * ret = blk.position();
    blk._used += size;
    return ret;
  }

  static char * allocate_buffer(uint32_t & current_block_idx, uint32_t size) {
    if (likely(current_block_idx != NULL_BLOCK_INDEX)) {
      MemoryBlock & cur_blk = get_block(current_block_idx);
      if (likely(size <= cur_blk.rest())) {
        char * ret = cur_blk.position();
        cur_blk._used += size;
        return ret;
      }
    }
    return alloc_block(current_block_idx, size);
  }

  static void dump(FILE * out);

  // TODO: extend last block


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
   * DualPivotQuickSort compare function
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
   * cpp std::sort compare function
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

  static void sort(std::vector<uint32_t> & kvpairs_offsets, SortType type);

};

} // namespace NativeTask


#endif /* MEMORYPOOL_H_ */
