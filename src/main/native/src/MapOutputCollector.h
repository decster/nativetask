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

#ifndef MAPOUTPUTBUFFER_H_
#define MAPOUTPUTBUFFER_H_

#include "commons.h"
#include "mempool.h"
#include "Timer.h"
#include "Buffers.h"
#include "MapOutputSpec.h"
#include "IFile.h"
#include "PartitionIndex.h"

namespace NativeTask {

/**
 * Memory Key-Value buffer pair with direct address content, so can be
 * easily copied or dumped to file
 */
struct KVBuffer {
  InplaceBuffer key;

  InplaceBuffer & get_key() {
    return key;
  }

  InplaceBuffer & get_value() {
    return key.next();
  }

  KVBuffer & next() {
    InplaceBuffer & value = get_value();
    return *(KVBuffer*) (value.content + value.length);
  }

  uint32_t memory() {
    return key.memory() + get_value().memory();
  }

  std::string str() {
    return get_key().str() + "\t" + get_value().str();
  }

  void copy_to(char * dest) {
    memcpy(dest, this, memory());
  }
};

/**
 * Buffer for a single partition
 */
class PartitionBucket {
private:
  bool     _sorted;
  uint32_t _partition;
  uint32_t _current_block_idx;
  std::vector<uint32_t> _kv_offsets;
  std::vector<uint32_t> _blk_ids;
public:
  PartitionBucket(uint32_t partition) :
    _sorted(false),
    _partition(partition),
    _current_block_idx(NULL_BLOCK_INDEX) {
  }

  void clear() {
    _sorted = false;
    _current_block_idx = NULL_BLOCK_INDEX;
    _kv_offsets.clear();
    _blk_ids.clear();
  }


  const uint32_t recored_count() const {
    return (uint32_t)_kv_offsets.size();
  }

  const uint32_t recored_offset(uint32_t record_idx) {
    return _kv_offsets[record_idx];
  }

  const uint32_t blk_count() const {
    return (uint32_t)_blk_ids.size();
  }

  const uint32_t current_block_idx() const {
    return _current_block_idx;
  }

  /**
   * @throws OutOfMemoryException if total_length > io.sort.mb
   */
  char * get_buffer_to_put(uint32_t total_length) {
    uint32_t old_block_idx = _current_block_idx;
    char * ret = MemoryBlockPool::allocate_buffer(_current_block_idx,
                                                  total_length);
    if (likely(NULL!=ret)) {
      _kv_offsets.push_back(MemoryBlockPool::get_offset(ret));
      if (unlikely(old_block_idx != _current_block_idx)) {
        _blk_ids.push_back(_current_block_idx);
      }
    }
    return ret;
  }

  char * get_buffer_to_put(uint32_t keylen, uint32_t valuelen) {
    return get_buffer_to_put(keylen + valuelen + sizeof(uint32_t) * 2);
  }

  void sort(SortType type);

  uint64_t estimate_spill_size(OutputFileType output_type, KeyValueType ktype,
                               KeyValueType vtype);

  void spill(IFileWriter & writer, uint64_t & keyGroupCount, ObjectCreatorFunc combinerCreator, Config & config)
      throw (IOException, UnsupportException);

  void dump(int fd, uint64_t offset, uint32_t & crc);

protected:
  class Iterator : public KVIterator {
  protected:
    PartitionBucket & pb;
    size_t index;
  public:
    Iterator(PartitionBucket & pb):pb(pb),index(0){}
    virtual ~Iterator(){}
    virtual bool next(Buffer & key, Buffer & value);
  };

  class KeyGroupIterator : public KeyGroup {
  protected:
    PartitionBucket & pb;
    InplaceBuffer * currentKey;
    size_t index;
    size_t size;
    bool hasNext;
  public:
    KeyGroupIterator(PartitionBucket & pb);
    virtual ~KeyGroupIterator(){}
    bool nextKey();
    virtual const char * getKey(uint32_t & len);
    virtual const char * nextValue(uint32_t & len);
  };
};

/**
 * MapOutputCollector
 */
class MapOutputCollector {
private:
  Config * _config;
  PartitionBucket ** _buckets;
  uint32_t _num_partition;
  std::vector<PartitionIndex *> _spills;
  MapOutputSpec _mapOutputSpec;
  bool _sortFirst;
  Timer _collectTimer;
private:
  void init_memory(uint32_t memory_capacity);

  void reset();

  void delete_temp_spill_files();

public:
  MapOutputCollector(uint32_t num_partition);

  ~MapOutputCollector();

  void configure(Config & config);

  MapOutputSpec & getMapOutputSpec() {
    return _mapOutputSpec;
  }

  uint32_t num_partition() const {
    return _num_partition;
  }

  PartitionBucket * bucket(uint32_t partition) {
    assert(partition<_num_partition);
    return _buckets[partition];
  }

  /**
   * estimate current spill file size
   */
  uint64_t estimate_spill_size(OutputFileType output_type, KeyValueType ktype,
      KeyValueType vtype);

  /**
   * sort all partitions, just used for testing sort only
   */
  void sort_all(SortType);

  /**
   * spill a range of partition buckets, prepare for future
   * Parallel sort & spill, TODO: parallel sort & spill
   */
  void spill_range(uint32_t start_partition,
                   uint32_t num_partition,
                   RecordOrderType orderType,
                   SortType sortType,
                   IFileWriter & writer,
                   uint64_t & blockCount,
                   uint64_t & recordCount,
                   uint64_t & sortTime,
                   uint64_t & keyGroupCount,
                   ObjectCreatorFunc combinerCreator);

  /**
   * normal spill use options in _config
   * @param filepaths: spill file path
   */
  void mid_spill(std::vector<std::string> & filepaths,
                 const std::string & idx_file_path,
                 MapOutputSpec & spec,
                 ObjectCreatorFunc combinerCreator);

  /**
   * final merge and/or spill use options in _config, and
   * previous spilled file & in-memory data
   */
  void final_merge_and_spill(std::vector<std::string> & filepaths,
      const std::string & indexpath, MapOutputSpec & spec,
      ObjectCreatorFunc combinerCreator);

  /**
   * collect one k/v pair
   * @throws OutOfMemoryException if length > io.sort.mb
   */
  char * get_buffer_to_put(uint32_t length, uint32_t partition) {
    assert(partition<_num_partition);
    PartitionBucket * pb = _buckets[partition];
    if (unlikely(NULL==pb)) {
      pb = new PartitionBucket(partition);
      _buckets[partition] = pb;
    }
    return pb->get_buffer_to_put(length);
  }

  /**
   * collect one k/v pair
   * @return 0 success; 1 buffer full, need spill
   */
  int put(const void * key, uint32_t keylen, const void * value,
      uint32_t vallen, uint32_t partition) {
    uint32_t total_length = keylen + vallen + sizeof(uint32_t) * 2;
    char * buff = get_buffer_to_put(total_length, partition);
    if (NULL == buff) {
      // need spill
      return 1;
    }
    InplaceBuffer * pkey = (InplaceBuffer*) buff;
    pkey->length = keylen;
    if (keylen>0) {
      simple_memcpy(pkey->content, key, keylen);
    }
    InplaceBuffer & val = pkey->next();
    val.length = vallen;
    if (vallen>0) {
      simple_memcpy(val.content, value, vallen);
    }
    return 0;
  }

};


}; //namespace NativeTask

#endif /* MAPOUTPUTBUFFER_H_ */









