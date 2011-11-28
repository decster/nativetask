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

#include "NativeObjectFactory.h"
#include "MapOutputCollector.h"
#include "fileutils.h"
#include "merge.h"

namespace Hadoop {

/**
 * KVBuffer writer
 * TODO: remove this & use IFile/IntermediateFile writer
 */
static uint64_t write_kvbuffers(const std::vector<uint32_t> & _kv_offsets,
    bool with_end_mark, int fd, char * output_buffer, uint32_t buffer_size,
    uint64_t & real_offset, uint32_t & checksum,
    OutputFileType output_file_type, KeyValueType k_type, KeyValueType v_type,
    CompressionType compression_type) {
  uint64_t total_size = 0;
  AppendBuffer append_buffer(output_buffer, buffer_size, fd, &checksum,
      &real_offset, compression_type);
  if (output_file_type == INTERMEDIATE) {
    for (size_t i = 0; i<_kv_offsets.size(); i++) {
      KVBuffer * pkvbuffer = (KVBuffer*)MemoryBlockPool::get_position(_kv_offsets[i]);
      uint32_t size = pkvbuffer->memory();
      append_buffer.append(pkvbuffer, size);
      total_size += size;
    }
  }
  else if (output_file_type == IFILE) {
    // IFile Format
    // IFile          [[Entry][...][[EOF_MARKER][EOF_MARKER]]][checksum]
    // Entry          [VInt keylen][VInt valuelen][SerializedData][SerializedData]
    // SerializedData [Text|BytesWritable]
    // Text           [VInt length][Raw data]
    // BytesWritable  [Int length][Raw data]
    for (size_t i = 0; i<_kv_offsets.size() ; i++) {
      KVBuffer * pkvbuffer = (KVBuffer*)MemoryBlockPool::get_position(_kv_offsets[i]);
      InplaceBuffer & bkey = pkvbuffer->get_key();
      InplaceBuffer & bvalue = pkvbuffer->get_value();
      // append KeyLength ValueLength KeyBytesLength
      char * oldmark = append_buffer.reserve_space(MaxVUInt32Length*3);
      char * mark = oldmark;
      if (k_type == TextType) {
        WriteVUInt32(mark, GetVUInt32Len(bkey.length)+bkey.length);
      } else {
        WriteVUInt32(mark, sizeof(uint32_t)+bkey.length);
      }
      if (v_type == TextType) {
        WriteVUInt32(mark, GetVUInt32Len(bvalue.length)+bvalue.length);
      } else if (v_type == BytesType ) {
        WriteVUInt32(mark, sizeof(uint32_t)+bvalue.length);
      } else {
        // !TODO: handle
        WriteVUInt32(mark, bvalue.length);
      }
      if (k_type == TextType) {
        WriteVUInt32(mark, bkey.length);
      } else {
        WriteUInt32(mark, bkey.length);
        mark += sizeof(uint32_t);
      }
      append_buffer.reserve_space_used_to(mark);
      total_size += (mark-oldmark);
      // append Key
      append_buffer.append(bkey.content, bkey.length);
      total_size += bkey.length;
      // append ValueBytesLength
      if (v_type == TextType) {
        char * oldmark2 = append_buffer.reserve_space(MaxVUInt32Length);
        char * mark2 = oldmark2;
        WriteVUInt32(mark2, bvalue.length);
        append_buffer.reserve_space_used_to(mark2);
        total_size += (mark2-oldmark2);
      } else if (v_type == BytesType) {
        append_buffer.append(bswap(bvalue.length));
        total_size += sizeof(uint32_t);
      } else {
        // !TODO: handle
      }
      // append Value
      append_buffer.append(bvalue.content, bvalue.length);
      total_size += bvalue.length;
    }
    if (with_end_mark) {
      // append end of ifile marker
      append_buffer.append(&IFileEndMarker, sizeof(IFileEndMarker));
      total_size += sizeof(IFileEndMarker);
    }
  } else {
    THROW_EXCEPTION(UnsupportException, "spill OutputFileType not supported");
  }
  // dump the rest
  append_buffer.flush();
  return total_size;
}

/////////////////////////////////////////////////////////////////
// PartitionBucket
/////////////////////////////////////////////////////////////////

uint64_t PartitionBucket::estimate_spill_size(OutputFileType output_type,
    KeyValueType ktype, KeyValueType vtype) {
  int64_t ret = 0;
  for (size_t i = 0; i<_blk_ids.size() ; i++) {
    MemoryBlock & blk = MemoryBlockPool::get_block(_blk_ids[i]);
    ret += blk.used();
  }
  if (output_type == INTERMEDIATE) {
    return ret+sizeof(uint32_t);
  }
  int64_t average_kv_size =
      (ret - (_kv_offsets.size() * sizeof(uint32_t) * 2)) / (_kv_offsets.size()*2);
  int64_t vhead_len = GetVUInt32Len(average_kv_size);
  // TODO: fix it
  if (ktype == TextType) {
    ret += _kv_offsets.size() * 2 * (2*vhead_len - sizeof(uint32_t));
  }
  else if (ktype == BytesType) {
    ret += _kv_offsets.size() * 2* vhead_len;
  }
  else {
    ret += _kv_offsets.size() * 2 * vhead_len;
  }
  return ret+2+sizeof(uint32_t);
}


uint64_t PartitionBucket::spill(int fd, uint64_t & real_offset,
    OutputFileType output_file_type, RecordOrderType order_type,
    CompressionType compression_type, KeyValueType k_type, KeyValueType v_type,
    uint32_t & checksum, char * output_buffer, uint32_t buffer_size)
    throw (IOException, UnsupportException) {
  uint64_t total_size = 0;
  if (order_type == NOSORT && output_file_type == INTERMEDIATE) {
    // special case
    if (compression_type == PLAIN) {
      AppendBuffer append_buffer(output_buffer, buffer_size, fd, &checksum, &real_offset);
      for (size_t i = 0; i<_blk_ids.size() ; i++) {
        MemoryBlock & blk = MemoryBlockPool::get_block(_blk_ids[i]);
        append_buffer.flush(blk.start(), blk.used());
        total_size += blk.used();
      }
    }
    else {
      // TODO: impl
      THROW_EXCEPTION(UnsupportException, "Compression output not supported");
    }
    return total_size;
  }
  if (order_type == FULLSORT) {
    if (!_sorted) {
      sort(CPPSORT);
    }
  }
  else if (order_type == GROUPBY) {
    THROW_EXCEPTION(UnsupportException, "spill RecordOrderType GROUPBY not supported");
  }
  // TODO: use IFile or IntermediateFile writer
  return write_kvbuffers(_kv_offsets, true, fd,
      output_buffer, buffer_size, real_offset, checksum, output_file_type,
      k_type, v_type, compression_type);
}

/////////////////////////////////////////////////////////////////
// SpillInfo
/////////////////////////////////////////////////////////////////
void SpillInfo::write_ifile_idx(const std::string & filepath) {
  // TODO: use AppendBuffer
  size_t buffsize = sizeof(uint64_t)*3*_num_partition;
  uint8_t * buffer = new uint8_t[buffsize];
  memset(buffer, 0, buffsize);
  uint64_t current_base = 0;
  uint64_t * current_dest = (uint64_t*)buffer;
  for (size_t i = 0; i < ranges.size(); i++) {
    SpillRangeInfo * range = ranges[i];
    for (size_t j=0; j<range->length;j++) {
      SpillSegmentInfo * segment = &(range->segments[j]);
      if (j==0) {
        current_dest[0] = bswap64(current_base);
        current_dest[1] = bswap64(segment->end_position);
        current_dest[2] = bswap64(segment->real_end_position);
      }
      else {
        current_dest[0] = bswap64(current_base
            + range->segments[j - 1].real_end_position);
        current_dest[1] = bswap64(segment->end_position
            - range->segments[j - 1].end_position);
        current_dest[2] = bswap64(segment->real_end_position
            - range->segments[j - 1].real_end_position);
      }
      current_dest+=3;
    }
    current_base += range->segments[range->length - 1].real_end_position;
  }
  if (current_dest-(uint64_t*)buffer != _num_partition*3) {
    THROW_EXCEPTION(IOException, "write ifile index inconsistent");
  }
  uint32_t chsum = checksum_init();
  checksum_update(chsum, buffer, sizeof(uint64_t)*3*_num_partition);
  chsum = checksum_val(chsum);
  FILE * fout = fopen(filepath.c_str(),"wb");
  if (1 != fwrite(buffer,buffsize, 1,fout)) {
    THROW_EXCEPTION(IOException, "write ifile content failed");
  }
#ifdef SPILLRECORD_CHECKSUM_UINT
  chsum = bswap(chsum);
  if (1 != fwrite(&chsum, sizeof(uint32_t), 1, fout)) {
    THROW_EXCEPTION(IOException, "write ifile checksum failed");
  }
#else
  uint64_t wtchsum = bswap64((uint64_t)chsum);
  if (1 != fwrite(&wtchsum, sizeof(uint64_t), 1, fout)) {
    THROW_EXCEPTION(IOException, "write ifile checksum failed");
  }
#endif
  fclose(fout);
  delete [] buffer;
}


/////////////////////////////////////////////////////////////////
// MapOutputCollector
/////////////////////////////////////////////////////////////////

MapOutputCollector::MapOutputCollector(uint32_t num_partition) {
  _num_partition = num_partition;
  _buckets = new PartitionBucket*[num_partition];
  memset(_buckets, 0, sizeof(PartitionBucket*) * num_partition);
}

MapOutputCollector::~MapOutputCollector() {
  if (_buckets) {
    for (uint32_t i = 0; i < _num_partition; i++) {
      delete _buckets[i];
    }
  }
  delete[] _buckets;
  for (size_t i = 0; i < _spills.size(); i++) {
    delete _spills[i];
  }
  _spills.clear();
  MemoryBlockPool::release();
}

void MapOutputCollector::delete_temp_spill_files() {
  for (size_t i = 0; i < _spills.size(); i++) {
    _spills[i]->delete_files();
  }
}

void MapOutputCollector::init_memory(uint32_t memory_capacity) {
  if (!MemoryBlockPool::inited()) {
    // At least DEFAULT_MIN_BLOCK_SIZE
    // TODO: at most  DEFUALT_MAX_BLOCK_SIZE
    // and make every bucket have approximately 4 blocks
    uint32_t s = memory_capacity / _num_partition / 4;
    s = GetCeil(s, DEFAULT_MIN_BLOCK_SIZE);
    s = std::max(s, DEFAULT_MIN_BLOCK_SIZE);
    MemoryBlockPool::init(memory_capacity, s);
  }
}

void MapOutputCollector::reset() {
  for (uint32_t i = 0; i < _num_partition; i++) {
    if (NULL != _buckets[i]) {
      _buckets[i]->clear();
    }
  }
  MemoryBlockPool::clear();
}

void MapOutputCollector::configure(Config & config) {
  _order_type = (RecordOrderType) config.get_type_enum(
      "native.record.order.type", FULLSORT);
  _sort_type = (SortType) config.get_type_enum(
      "native.sort.type", CPPSORT);
  _compress_type = (CompressionType) config.get_type_enum(
      "native.compression.type", PLAIN);
  _output_type = (OutputFileType) config.get_type_enum(
      "native.mapoutput.file.type", IFILE);

  const char * key_class = config.get("mapred.mapoutput.key.class");
  if (NULL == key_class) {
    key_class = config.get("mapred.output.key.class");
  }
  if (NULL == key_class) {
    THROW_EXCEPTION(IOException, "mapred.mapoutput.key.class not set");
  }
  _key_type = JavaClassToKeyValueType(key_class);
  if (UnknownType == _key_type) {
    THROW_EXCEPTION(UnsupportException, "key class type not support");
  }
  const char * value_class = config.get("mapred.mapoutput.value.class");
  if (NULL == value_class) {
    value_class = config.get("mapred.output.value.class");
  }
  if (NULL == value_class) {
    THROW_EXCEPTION(IOException, "mapred.mapoutput.value.class not set");
  }
  _value_type = JavaClassToKeyValueType(value_class);

  init_memory(config.get_uint32("io.sort.mb", 300) * 1024 * 1024);
}

/**
 * sort all partitions
 */
void MapOutputCollector::sort_all(SortType sort_type) {
  // do sort
  for (uint32_t i = 0; i < _num_partition; i++) {
    PartitionBucket * pb = _buckets[i];
    if ((NULL != pb) && (pb->current_block_idx() != NULL_BLOCK_INDEX)) {
      pb->sort(sort_type);
    }
  }
}

/**
 * Spill buffer to file
 * @return Array of spill segments information
 */
SpillSegmentInfo * MapOutputCollector::spill_range(uint32_t start_partition,
    uint32_t num_partition, const char * filepath, RecordOrderType order_type,
    CompressionType compression_type, KeyValueType k_type, KeyValueType v_type,
    OutputFileType output_type) throw (IOException, UnsupportException) {
  // only use TIMED in test, clock() is enough here
  clock_t start_time = clock();
  if (order_type == GROUPBY) {
    THROW_EXCEPTION(UnsupportException, "spill option not supported");
  }
  uint32_t output_buffer_size = NativeObjectFactory::GetConfig().get_uint32(
      "mapred.native.spill.buffer.kb", 32)*1024;
  if (_compress_type != PLAIN) {
    output_buffer_size = NativeObjectFactory::GetConfig().get_uint32(
        "mapred.native.compression.buffer.kb", output_buffer_size / 1024)
        * 1024;
  }
  SpillSegmentInfo * ret = new SpillSegmentInfo[_num_partition];
  // add 8 bytes buffer tail to support simple_memcpy
  char * output_buffer = new char[output_buffer_size+sizeof(uint64_t)];
  FILE * fout = fopen(filepath, "wb");
  int fdout = fileno(fout);
  uint64_t cur_offset = 0;
  uint64_t real_offset = 0;
  uint32_t end_partition = start_partition + num_partition;
  uint64_t record_count = 0;
  uint64_t block_count = 0;
  for (uint32_t i = start_partition; i < end_partition; i++) {
    PartitionBucket * pb = _buckets[i];
    if (pb == NULL) {
      pb = new PartitionBucket(i);
      _buckets[i] = pb;
    }
    // ifile or intermediate file not empty
    uint32_t chsum = checksum_init();
    cur_offset += pb->spill(fdout, real_offset, output_type, order_type,
        compression_type, k_type, v_type, chsum, output_buffer, output_buffer_size);
    // TODO: move checksum write to appender
    chsum = checksum_val(chsum);
    chsum = bswap(chsum);
    WRITE(fdout, &chsum, sizeof(uint32_t));
    real_offset += sizeof(uint32_t);
    record_count += pb->recored_count();
    block_count += pb->blk_count();
    ret[i].end_position = cur_offset;
    ret[i].real_end_position = real_offset;
  }
  delete [] output_buffer;
  fclose(fout);

  // show statistics
  clock_t end_time = clock();
  LOG("Spill %lu range [%u,%u) record: %llu, avg: %.3lf, block: %llu, size %llu, real: %llu, time(cpu): %.3lf",
      _spills.size(), start_partition, end_partition,
      record_count, (double)cur_offset/record_count,
      block_count, cur_offset, real_offset,
      (end_time-start_time)/(double)CLOCKS_PER_SEC);
  return ret;
}


/**
 * normal spill in-memory data
 * @param filepaths: spill file path
 */
int MapOutputCollector::mid_spill(std::vector<std::string> & filepaths) {
  if (filepaths.size()==1) {
    // mid_spill don't use compression, and output as intermediate file
    SpillSegmentInfo * seginfo = spill_range(0, _num_partition,
        filepaths[0].c_str(), _order_type, PLAIN, _key_type, _value_type,
        INTERMEDIATE);
    SpillRangeInfo * sri = new SpillRangeInfo(0, _num_partition, filepaths[0], seginfo);
    SpillInfo * si = new SpillInfo(_num_partition);
    si->add(sri);
    _spills.push_back(si);
    reset();
    return 0;
  } else if (0==filepaths.size()) {
    THROW_EXCEPTION(IOException, "Spill file path empty");
  }
  THROW_EXCEPTION(UnsupportException, "Parallel spill not supported");
}

/**
 * final merge and/or spill, use previous spilled
 * file & in-memory data
 */
int MapOutputCollector::final_merge_and_spill(
    std::vector<std::string> & filepaths, std::string & idx_file_path) {
  std::string map_outoutput = filepaths[0];
  if (_spills.size()==0) {
    SpillSegmentInfo * seginfo = spill_range(0, _num_partition,
        map_outoutput.c_str(), _order_type, _compress_type, _key_type, _value_type,
        _output_type);
    SpillRangeInfo * sri = new SpillRangeInfo(0, _num_partition, map_outoutput, seginfo);
    if (idx_file_path.length()>0) {
      SpillInfo * si = new SpillInfo(_num_partition);
      si->add(sri);
      si->write_ifile_idx(idx_file_path);
      delete si;
    }
    else {
      delete sri;
    }
    reset();
    return 0;
  }
  else {
    // TODO: impl multi-pass merge & compression
    FILE * fout = fopen(map_outoutput.c_str(),"wb");
    if (NULL == fout) {
      THROW_EXCEPTION(IOException, "create final map output file failed");
    }
    int foutfd = fileno(fout);
    uint32_t buffsize = NativeObjectFactory::GetConfig().get_uint32(
        "mapred.native.spill.buffer.kb", 32) * 1024;
    if (_compress_type != PLAIN) {
      buffsize = NativeObjectFactory::GetConfig().get_uint32(
          "mapred.native.compression.buffer.kb", buffsize/1024) * 1024;
    }
    int merge_range = 0;
    char * buff = new char[buffsize+sizeof(uint64_t)];
    MapOutputWriter * mow = NULL;
    if (INTERMEDIATE == _output_type) {
      mow = new IntermediateFileWriter(foutfd, buff, buffsize, _compress_type);
    }
    else if (IFILE == _output_type) {
      mow = new IFileWriter(foutfd, buff, buffsize,
                _key_type, _value_type, _compress_type);
    }
    else {
      THROW_EXCEPTION(UnsupportException, "Output file type not supported");
    }
    Merger * merger = new Merger(mow);
    for (size_t i = 0 ; i < _spills.size() ; i++) {
      SpillInfo * spill = _spills[i];
      MergeEntryPtr pme = new InterFileMergeEntry(spill->ranges[merge_range],
          buffsize);
      merger->add_merge_entry(pme);
    }
    merger->add_merge_entry(new MemoryMergeEntry(this));
    merger->merge();
    delete merger;
    fclose(fout);
    // write index
    SpillRangeInfo * spill_range = mow->get_spill_range_info(0);
    SpillInfo * spill_info = new SpillInfo(_num_partition);
    spill_info->add(spill_range);
    spill_info->write_ifile_idx(idx_file_path);
    delete spill_info;
    delete mow;
    delete [] buff;
    delete_temp_spill_files();
    reset();
    return 0;
  }
}

uint64_t MapOutputCollector::estimate_spill_size(OutputFileType output_type,
    KeyValueType ktype, KeyValueType vtype) {
  uint64_t ret = 0;
  if (_buckets) {
    for (uint32_t i = 0; i < _num_partition; i++) {
      ret += _buckets[i]->estimate_spill_size(output_type, ktype, vtype);
    }
  }
  return ret;
}


}; // namespace Hadoop





