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

#ifndef MERGE_H_
#define MERGE_H_

#include "Buffers.h"
#include "MapOutputCollector.h"
#include "IFile.h"

namespace Hadoop {

/**
 * merger
 */
class MergeEntry {
public:
  // these 3 fields should be filled after next() is called
  const char *   _key;
  uint32_t _key_len;
  uint32_t _value_len;
public:
  MergeEntry() :
      _key_len(0),
      _key(NULL) {
  }

  virtual ~MergeEntry() {
  }

  inline bool operator<(const MergeEntry & rhs) const {
    uint32_t minlen = std::min(_key_len, rhs._key_len);
    int ret = fmemcmp(_key, rhs._key, minlen);
    return ret < 0 || (ret == 0 && (_key_len < rhs._key_len));
  }

  /**
   * move to next partition
   * 0 on success
   * 1 on no more
   */
  virtual int nextPartition() = 0;

  /**
   * move to next key/value
   * 0 on success
   * 1 on no more
   */
  virtual int next() = 0;

  /**
   * read value
   */
  virtual const char * getValue() = 0;
};

/**
 * Merge entry for in-memory partition bucket
 */
class MemoryMergeEntry: public MergeEntry {
protected:
  char *               _value;
  MapOutputCollector * _moc;
  PartitionBucket *    _pb;
  int64_t              _cur_partition;
  int64_t              _cur_index;
public:
  MemoryMergeEntry(MapOutputCollector * moc) :
      _moc(moc),
      _pb(NULL),
      _cur_partition(-1ULL),
      _cur_index(-1ULL) {
  }

  virtual ~MemoryMergeEntry() {
  }

  /**
   * move to next partition
   * 0 on success
   * 1 on no more
   */
  virtual int nextPartition() {
    ++_cur_partition;
    if (_cur_partition < _moc->num_partition()) {
      _pb = _moc->bucket(_cur_partition);
      _cur_index = -1ULL;
      return 0;
    }
    return 1;
  }

  /**
   * move to next key/value
   * 0 on success
   * 1 on no more
   */
  virtual int next() {
    ++_cur_index;
    if ((NULL != _pb) && (_cur_index < _pb->recored_count())) {
      uint32_t offset = _pb->recored_offset(_cur_index);
      InplaceBuffer * kb = (InplaceBuffer*)(MemoryBlockPool::get_position(offset));
      _key_len = kb->length;
      _key = kb->content;
      InplaceBuffer & vb = kb->next();
      _value_len = vb.length;
      _value = vb.content;
      assert(_value != NULL);
      return 0;
    }
    // detect error early
    _key_len = 0xffffffff;
    _value_len = 0xffffffff;
    _key = NULL;
    _value = NULL;
    return 1;
  }

  /**
   * read value
   * NOTICE: no big value problem, cause value is already in memory
   */
  virtual const char * getValue() {
    return _value;
  }
};


/**
 * Merge entry for intermediate file
 */
class IFileMergeEntry : public MergeEntry {
protected:
  IFileReader * _reader;
  bool new_partition;
public:
  /**
   * @param reader: managed by InterFileMergeEntry
   */
  IFileMergeEntry(IFileReader * reader):_reader(reader) {
    new_partition = false;
  }

  virtual ~IFileMergeEntry() {
  }

  /**
   * move to next partition
   * 0 on success
   * 1 on no more
   */
  virtual int nextPartition() {
    return _reader->nextPartition();
  }

  /**
   * move to next key/value
   * 0 on success
   * 1 on no more
   */
  virtual int next() {
    _key = _reader->nextKey(_key_len);
    if (unlikely(NULL == _key)) {
      // detect error early
      _key_len = 0xffffffffU;
      _value_len = 0xffffffffU;
      return 1;
    }
    _value_len = _reader->valueLen();
    return 0;
  }

  /**
   * read value
   */
  virtual const char * getValue() {
    const char * ret = _reader->value(_value_len);
    assert(ret != NULL);
    return ret;
  }
};



/**
 * heap used by merge
 */
template<typename T>
void adjust_heap(T* first, int rt, int heap_len, bool (*Comp)(T, T)) {
  while (rt * 2 <= heap_len) // not leaf
  {
    int left = (rt << 1); // left child
    int right = (rt << 1) + 1; // right child
    int smallest = rt;
    if (Comp(*(first + left - 1), *(first + smallest - 1))) {
      smallest = left;
    }
    if (right <= heap_len && Comp(*(first + right - 1), *(first + smallest - 1))) {
      smallest = right;
    }
    if (smallest != rt) {
      std::swap(*(first + smallest - 1), *(first + rt - 1));
      rt = smallest;
    }
    else {
      break;
    }
  }
}

template<typename T>
void make_heap(T* begin, T* end, bool (*Comp)(T, T)) {
  int heap_len = end - begin;
  if (heap_len >= 0) {
    for (int i = heap_len / 2; i >= 1; i--) {
      adjust_heap(begin, i, heap_len, Comp);
    }
  }
}

/**
 * just for test
 */
template<typename T>
void check_heap(T* begin, T* end, bool (*Comp)(T, T)) {
  int heap_len = end - begin;
  if (heap_len >= 0) {
    for (int i = heap_len / 2; i >= 1; i--) {
      int left = i << 1;
      int right = left + 1;
      if (Comp(*(begin+left-1), *(begin+i-1))) {
        assert(false);
      }
      if (right<=heap_len) {
        if (Comp(*(begin+right-1), *(begin+i-1))) {
          assert(false);
        }
      }
    }
  }
}

template<typename T>
void push_heap(T* begin, T* end, bool (*Comp)(T, T)) {
  int now = end - begin;
  while (now > 1) {
    int parent = (now >> 1);
    if (Comp(*(begin + now - 1), *(begin + parent - 1))) {
      std::swap(*(begin + now - 1), *(begin + parent - 1));
      now = parent;
    }
    else {
      break;
    }
  }
}

template<typename T>
void pop_heap(T* begin, T* end, bool (*Comp)(T, T)) {
  *begin = *(end - 1);
  // adjust [begin, end - 1) to heap
  adjust_heap(begin, 1, end - begin - 1, Comp);
}


/**
 * Merger
 */
typedef MergeEntry * MergeEntryPtr;

inline bool MergeEntryCompare(const MergeEntryPtr lhs, const MergeEntryPtr rhs) {
  return *lhs < *rhs;
}

class Merger {
private:
  std::vector<MergeEntryPtr> _entries;
  std::vector<MergeEntryPtr> _heap;
  IFileWriter * _writer;
public:
  Merger(IFileWriter * writer) :
      _writer(writer) {
  }
  ~Merger();

  void addMergeEntry(MergeEntryPtr pme);

  /**
   * @return 0 if success, have next partition
   *         1 if failed, no more
   */
  int startPartition();

  /**
   * finish one partition
   */
  void endPartition();

  void initHeap();

  void merge();
};

} // namespace Hadoop

#endif /* MERGE_H_ */
