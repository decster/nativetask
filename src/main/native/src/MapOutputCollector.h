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
#include "iobuffer.h"
#include "mempool.h"

namespace Hadoop {

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

  void sort(SortType type) {
    if ((!_sorted) && (_kv_offsets.size()>1)) {
      if (type == CQSORT) {
        MemoryBlockPool::sort_c(_kv_offsets);
      }
      else {
        MemoryBlockPool::sort_cpp(_kv_offsets);
      }
    }
    _sorted = true;
  }

  uint64_t estimate_spill_size(OutputFileType output_type, KeyValueType ktype,
      KeyValueType vtype);

  uint64_t spill(int fd, uint64_t & copmressed_size,
      OutputFileType output_file_type, RecordOrderType order_type,
      CompressionType compression_type, KeyValueType k_type, KeyValueType v_type,
      uint32_t & checksum, char * output_buffer, uint32_t buffer_size)
      throw (IOException, UnsupportException);

  void dump(int fd, uint64_t offset, uint32_t & crc) {
    FILE * out = fdopen(fd, "w");
    fprintf(out, "Partition %d total %lu kv pairs, sorted: %s\n", _partition,
            _kv_offsets.size(), _sorted?"true":"false");
    for (size_t i = 0; i < _kv_offsets.size(); i++) {
      KVBuffer * kv = (KVBuffer*) MemoryBlockPool::get_position(_kv_offsets[i]);
      std::string info = kv->str();
      fwrite(info.c_str(), 1, info.length(), out);
      fputc('\n', out);
    }
    fputc('\n', out);
  }
};


/**
 * Store spill file segment information
 */
struct SpillSegmentInfo {
  // uncompressed stream end position
  uint64_t end_position;
  // compressed stream end position
  uint64_t real_end_position;
};

class SpillRangeInfo {
public:
  uint32_t start;
  uint32_t length;
  std::string filepath;
  SpillSegmentInfo * segments;
  SpillRangeInfo(uint32_t start, uint32_t len, std::string & filepath,
      SpillSegmentInfo * segments) :
    start(start), length(len), filepath(filepath), segments(segments) {
  }
  ~SpillRangeInfo() {
    delete [] segments;
  }

  void delete_file() {
    if (filepath.length()>0) {
      struct stat st;
      if (0 == stat(filepath.c_str(),&st)) {
        remove(filepath.c_str());
      }
    }
  }
};

class SpillInfo {
  uint32_t _num_partition;
public:
  std::vector<SpillRangeInfo*> ranges;
  SpillInfo(uint32_t num_partition) :
    _num_partition(num_partition) {
  }

  ~SpillInfo() {
    for (size_t i = 0; i < ranges.size(); i++) {
      delete ranges[i];
    }
    ranges.clear();
  }

  void delete_files() {
    for (size_t i = 0; i < ranges.size(); i++) {
      ranges[i]->delete_file();
    }
  }

  void add(SpillRangeInfo * sri) {
    ranges.push_back(sri);
  }

  void write_ifile_idx(const std::string & filepath);
};

/**
 * MapOutputCollector
 */
class MapOutputCollector {
private:
  PartitionBucket ** _buckets;
  uint32_t _num_partition;
  RecordOrderType _order_type;
  SortType _sort_type;
  CompressionType _compress_type;
  KeyValueType _key_type;
  KeyValueType _value_type;
  OutputFileType _output_type;
  std::vector<SpillInfo *> _spills;

private:
  void init_memory(uint32_t memory_capacity);

  void reset();

  void delete_temp_spill_files();

public:
  MapOutputCollector(uint32_t num_partition);

  ~MapOutputCollector();

  void configure(Config & config);

  uint32_t num_partition() const {
    return _num_partition;
  }

  PartitionBucket * bucket(uint32_t partition) {
    assert(partition<_num_partition);
    return _buckets[partition];
  }

  SortType get_sort_type() const {
    return _sort_type;
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
  SpillSegmentInfo * spill_range(uint32_t start_partition,
      uint32_t num_partition, const char * filepath, RecordOrderType order_type,
      CompressionType compression_type, KeyValueType k_type, KeyValueType v_type,
      OutputFileType output_type) throw (IOException, UnsupportException);

  /**
   * normal spill use options in _config
   * @param filepaths: spill file path
   */
  int mid_spill(std::vector<std::string> & filepaths);

  /**
   * final merge and/or spill use options in _config, and
   * previous spilled file & in-memory data
   */
  int final_merge_and_spill(std::vector<std::string> & filepaths,
      std::string & indexpath);

  /**
   * spill content to local file
   */
  SpillSegmentInfo * spill(const char * filepath, RecordOrderType order_type,
      CompressionType compression_type, KeyValueType key_value_type,
      OutputFileType output_type) throw (IOException, UnsupportException) {
    return spill_range(0, _num_partition, filepath, order_type,
        compression_type, _key_type, _value_type, output_type);
  }

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


}; //namespace Hadoop

#endif /* MAPOUTPUTBUFFER_H_ */









