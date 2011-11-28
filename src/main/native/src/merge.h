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

#include "MapOutputCollector.h"
#include "fileutils.h"

namespace Hadoop {

/**
 * merger
 */
class MergeEntry {
public:
  // these 3 fields showed be filled when next() is called
  uint32_t _key_len;
  uint32_t _value_len;
  const char *   _key;
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
  virtual int next_partition() = 0;

  /**
   * move to next key/value
   * 0 on success
   * 1 on no more
   */
  virtual int next() = 0;

  /**
   * read value
   * NOTICE: no big value problem, cause value is already in memory
   */
  virtual const char * get_value() = 0;
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
  virtual int next_partition() {
    ++_cur_partition;
    if (_cur_partition < _moc->num_partition()) {
      _pb = _moc->bucket(_cur_partition);
      _pb->sort(_moc->get_sort_type());
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
  virtual const char * get_value() {
    return _value;
  }
};


/**
 * Merge entry for intermediate file
 */
class InterFileMergeEntry : public MergeEntry {
protected:
  IntermediateFileReader * _reader;
  bool new_partition;
public:
  /**
   * @param reader: managed by InterFileMergeEntry
   */
  InterFileMergeEntry(IntermediateFileReader * reader):_reader(reader) {
    new_partition = false;
  }

  InterFileMergeEntry(SpillRangeInfo * sr, uint32_t buffsize) {
    _reader = new IntermediateFileReader(sr, buffsize);
  }

  virtual ~InterFileMergeEntry() {
    delete _reader;
  }

  /**
   * move to next partition
   * 0 on success
   * 1 on no more
   */
  virtual int next_partition() {
    return _reader->next_partition();
  }

  /**
   * move to next key/value
   * 0 on success
   * 1 on no more
   */
  virtual int next() {
    _key = _reader->next_key(_key_len);
    if (unlikely(NULL == _key)) {
      // detect error early
      _key_len = 0xffffffffU;
      _value_len = 0xffffffffU;
      return 1;
    }
    _value_len = _reader->current_value_len();
    return 0;
  }

  /**
   * read value
   * TODO: handle big value
   */
  virtual const char * get_value() {
    assert(_value_len!=0xffffffffU);
    const char * ret = _reader->value();
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
  MapOutputWriter * _writer;
public:
  Merger(MapOutputWriter * writer) :
      _writer(writer) {
  }
  ~Merger() {
    _heap.clear();
    for (size_t i = 0 ; i < _entries.size() ; i++) {
      delete _entries[i];
    }
    _entries.clear();
  }

  void add_merge_entry(MergeEntryPtr pme) {
    _entries.push_back(pme);
  }

  /**
   * 0 if success, have next partition
   * 1 if failed, no more
   */
  int next_partition() {
    int ret = -1;
    for (size_t i = 0 ; i < _entries.size() ; i++) {
      int r = _entries[i]->next_partition();
      if (ret == -1) {
        ret = r;
      } else if (r != ret) {
        THROW_EXCEPTION(IOException, "MergeEntry partition number not equal");
      }
    }
    if (0==ret) { // do have new partition
      _writer->start_partition();
    }
    return ret;
  }

  /**
   * finish one partition
   */
  void end_partition() {
    _writer->end_partition();
  }

  void init_heap() {
    _heap.clear();
    for (size_t i = 0 ; i < _entries.size() ; i++) {
      MergeEntryPtr pme = _entries[i];
      if (0==pme->next()) {
        _heap.push_back(pme);
      }
    }
    make_heap(&(_heap[0]), &(_heap[0])+_heap.size(), MergeEntryCompare);
  }

  void merge() {
    // only use TIMED in test, clock() is enough here
    clock_t start_time = clock();
    uint64_t total_record = 0;
    _heap.reserve(_entries.size());
    MergeEntryPtr * base = &(_heap[0]);
    while (0==next_partition()) {
      init_heap();
      size_t cur_heap_size = _heap.size();
      while (cur_heap_size>0) {
        MergeEntryPtr top = base[0];
        _writer->write_key_part(top->_key, top->_key_len, top->_value_len);
        // TODO: handle big value
        _writer->write_value_part(top->get_value(), top->_value_len);
        total_record++;
        if (0 == top->next()) { // have more, adjust heap
          if (cur_heap_size == 1) {
            continue;
          } else if (cur_heap_size == 2) {
            if (*(base[1]) < *(base[0])) {
              std::swap(base[0], base[1]);
            }
          } else {
            adjust_heap(base, 1, cur_heap_size, MergeEntryCompare);
          }
        } else { // no more, pop heap
          if (cur_heap_size>2) {
            pop_heap(base, base+cur_heap_size, MergeEntryCompare);
          }
          else if (cur_heap_size==2) {
            base[0] = base[1];
          }
          cur_heap_size--;
        }
      }
      end_partition();
    }
    clock_t end_time = clock();
    uint64_t output_size;
    uint64_t real_output_size;
    _writer->get_statistics(output_size, real_output_size);
    LOG("Merge %lu segments: record %llu, avg %.3lf, size %llu, real %llu, time(cpu) %.3lf",
        _entries.size(),
        total_record,
        (double)output_size/total_record,
        output_size,
        real_output_size,
        (end_time-start_time)/(double)CLOCKS_PER_SEC);
  }
};

} // namespace Hadoop

#endif /* MERGE_H_ */
