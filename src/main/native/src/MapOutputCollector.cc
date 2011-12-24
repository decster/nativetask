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

#include "Timer.h"
#include "FileSystem.h"
#include "NativeObjectFactory.h"
#include "MapOutputCollector.h"
#include "Merge.h"

namespace Hadoop {

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
  int64_t vhead_len = WritableUtils::GetVLongSize(average_kv_size);
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

bool PartitionBucket::Iterator::next(Buffer & key, Buffer & value) {
  if (index<pb._kv_offsets.size()) {
    KVBuffer * pkvbuffer = (KVBuffer*)MemoryBlockPool::get_position(pb._kv_offsets[index]);
    InplaceBuffer & bkey = pkvbuffer->get_key();
    InplaceBuffer & bvalue = pkvbuffer->get_value();
    key.reset(bkey.content, bkey.length);
    value.reset(bvalue.content, bvalue.length);
    ++index;
    return true;
  }
  return false;
}

bool PartitionBucket::KeyGroupIterator::nextKey() {
  if (currentKeyFinished==false) {
    uint32_t temp;
    while (true) {
      const char * ret = nextValue(temp);
      if (ret==NULL) {
        return false;
      }
      if (currentKeyFinished) {
        key = currentKey->content;
        keyLen = currentKey->length;
        return true;
      }
    }
  } else if (key==NULL) {
    if (pb._kv_offsets.size()==0) {
      return false;
    }
    KVBuffer * pkvbuffer = (KVBuffer*)MemoryBlockPool::get_position(pb._kv_offsets[index]);
    InplaceBuffer & currentKey = pkvbuffer->get_key();
    key = currentKey.content;
    keyLen = currentKey.length;
    currentKeyFinished = false;
    return true;
  }
  key = currentKey->content;
  keyLen = currentKey->length;
  return true;
}

const char * PartitionBucket::KeyGroupIterator::getKey(uint32_t & len) {
  len = keyLen;
  return key;
}

const char * PartitionBucket::KeyGroupIterator::nextValue(uint32_t & len) {
  if (unlikely(index>=(ssize_t)pb._kv_offsets.size())) {
    return NULL;
  }
  KVBuffer * pkvbuffer = (KVBuffer*)MemoryBlockPool::get_position(pb._kv_offsets[index]);
  InplaceBuffer & bkey = pkvbuffer->get_key();
  InplaceBuffer & bvalue = pkvbuffer->get_value();
  if ((bkey.length != currentKey->length) ||
      (fmemeq(currentKey->content, bkey.content, bkey.length) != 0)) {
    currentKey = &bkey;
    currentKeyFinished = true;
    index++;
    return NULL;
  }
  index++;
  len = bvalue.length;
  return bvalue.content;
}

void PartitionBucket::spill(IFileWriter & writer, ObjectCreatorFunc combinerCreator, Config & config)
    throw (IOException, UnsupportException) {
  if (combinerCreator == NULL) {
    for (size_t i = 0; i<_kv_offsets.size() ; i++) {
      KVBuffer * pkvbuffer = (KVBuffer*)MemoryBlockPool::get_position(_kv_offsets[i]);
      InplaceBuffer & bkey = pkvbuffer->get_key();
      InplaceBuffer & bvalue = pkvbuffer->get_value();
      writer.write(bkey.content, bkey.length, bvalue.content, bvalue.length);
    }
  } else {
    NativeObject * combiner = combinerCreator();
    if (combiner == NULL) {
      THROW_EXCEPTION_EX(UnsupportException, "Create combiner failed");
    }
    switch (combiner->type()) {
    case MapperType:
      {
        Mapper * mapper = (Mapper*)combiner;
        mapper->setCollector(&writer);
        mapper->configure(config);
        for (size_t i = 0; i<_kv_offsets.size() ; i++) {
          KVBuffer * pkvbuffer = (KVBuffer*)MemoryBlockPool::get_position(_kv_offsets[i]);
          InplaceBuffer & bkey = pkvbuffer->get_key();
          InplaceBuffer & bvalue = pkvbuffer->get_value();
          mapper->map(bkey.content, bkey.length, bvalue.content, bvalue.length);
        }
        mapper->close();
        delete mapper;
      }
      break;
    case ReducerType:
      {
        Reducer * reducer = (Reducer*)combiner;
        reducer->setCollector(&writer);
        reducer->configure(config);
        KeyGroupIterator kg = KeyGroupIterator(*this);
        while (kg.nextKey()) {
          reducer->reduce(kg);
        }
        reducer->close();
        delete reducer;
      }
      break;
    case FolderType:
      {
        delete combiner;
        THROW_EXCEPTION(UnsupportException, "Folder not support in combiner");
      }
    default:
      delete combiner;
      THROW_EXCEPTION(UnsupportException, "Combiner type not support");
    }
  }
}

void PartitionBucket::sort(SortType type) {
  if ((!_sorted) && (_kv_offsets.size()>1)) {
    MemoryBlockPool::sort(_kv_offsets, type);
  }
  _sorted = true;
}

void PartitionBucket::dump(int fd, uint64_t offset, uint32_t & crc) {
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


/////////////////////////////////////////////////////////////////
// MapOutputCollector
/////////////////////////////////////////////////////////////////

MapOutputCollector::MapOutputCollector(uint32_t num_partition) :
  _config(NULL),
  _buckets(NULL),
  _sortFirst(false) {
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
    _spills[i]->deleteFiles();
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
  _config = &config;
  _sortFirst = config.getBool("native.spill.sort.first", false);
  MapOutputSpec::getSpecFromConfig(config, _mapOutputSpec);
  init_memory(config.getInt("io.sort.mb", 300) * 1024 * 1024);
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
void MapOutputCollector::spill_range(uint32_t start_partition,
                                     uint32_t num_partition,
                                     RecordOrderType orderType,
                                     SortType sortType,
                                     IFileWriter & writer,
                                     uint64_t & blockCount,
                                     uint64_t & recordCount,
                                     ObjectCreatorFunc combinerCreator) {
  if (orderType == GROUPBY) {
    THROW_EXCEPTION(UnsupportException, "GROUPBY not supported");
  }
  IndexEntry * ret = new IndexEntry[_num_partition];
  Timer timer;
  if (_sortFirst && orderType==FULLSORT) {
    Timer timer;
    for (uint32_t i = 0; i < num_partition; i++) {
      PartitionBucket * pb = _buckets[start_partition+i];
      if ((NULL != pb) && (pb->current_block_idx() != NULL_BLOCK_INDEX)) {
        pb->sort(sortType);
      }
    }
    LOG("%s", timer.getInterval("Sort all buckets").c_str());
  }
  for (uint32_t i = 0; i < num_partition; i++) {
    writer.startPartition();
    PartitionBucket * pb = _buckets[start_partition+i];
    if (pb != NULL) {
      if (orderType == FULLSORT) {
        pb->sort(sortType);
      }
      pb->spill(writer, combinerCreator, *_config);
      recordCount += pb->recored_count();
      blockCount += pb->blk_count();
    }
    writer.endPartition();
  }
}


void MapOutputCollector::mid_spill(std::vector<std::string> & filepaths,
                                   const std::string & idx_file_path,
                                   MapOutputSpec & spec,
                                   ObjectCreatorFunc combinerCreator) {
  if (filepaths.size() == 1) {
    uint64_t blockCount = 0;
    uint64_t recordCount = 0;
    OutputStream * fout = FileSystem::getRaw().create(filepaths[0], true);
    IFileWriter * writer = new IFileWriter(fout, spec.checksumType,
                                             spec.keyType, spec.valueType,
                                             spec.codec);
    Timer timer;
    spill_range(0, _num_partition, spec.orderType, spec.sortType, *writer,
                blockCount, recordCount, combinerCreator);
    IndexRange * info = writer->getIndex(0);
    info->filepath = filepaths[0];
    double interval = (timer.now() - timer.last()) / 1000000000.0;
    LOG("Spill %lu range [%u,%u) record: %llu, avg: %.3lf, block: %llu, size %llu, real: %llu, time: %.3lf",
        _spills.size(), 0, _num_partition,
        recordCount, (double)info->getEndPosition()/recordCount,
        blockCount, info->getEndPosition(), info->getRealEndPosition(),
        interval);
    PartitionIndex * si = new PartitionIndex(_num_partition);
    si->add(info);
    if (idx_file_path.length()>0) {
      si->writeIFile(idx_file_path);
      delete si;
    } else {
      _spills.push_back(si);
    }
    delete writer;
    delete fout;
    reset();
  } else if (filepaths.size() == 0) {
    THROW_EXCEPTION(IOException, "Spill file path empty");
  } else {
    THROW_EXCEPTION(UnsupportException, "Parallel spill not support");
  }
}

/**
 * final merge and/or spill, use previous spilled
 * file & in-memory data
 */
void MapOutputCollector::final_merge_and_spill(std::vector<std::string> & filepaths,
                                               const std::string & idx_file_path,
                                               MapOutputSpec & spec) {
  if (_spills.size()==0) {
    mid_spill(filepaths, idx_file_path, spec);
    return;
  }
  OutputStream * fout = FileSystem::getRaw().create(filepaths[0], true);
  IFileWriter * writer = new IFileWriter(fout, spec.checksumType,
                                           spec.keyType, spec.valueType,
                                           spec.codec);
  Merger * merger = new Merger(writer);
  InputStream ** inputStreams = new InputStream*[_spills.size()];
  IFileReader ** readers = new IFileReader*[_spills.size()];
  for (size_t i = 0 ; i < _spills.size() ; i++) {
    PartitionIndex * spill = _spills[i];
    inputStreams[i] = FileSystem::getRaw().open(spill->ranges[0]->filepath);
    readers[i] = new IFileReader(inputStreams[i], spec.checksumType,
                                  spec.keyType, spec.valueType,
                                  spill->ranges[0], spec.codec);
    MergeEntryPtr pme = new IFileMergeEntry(readers[i]);
    merger->addMergeEntry(pme);
  }
  if (spec.orderType==GROUPBY) {
    THROW_EXCEPTION(UnsupportException, "GROUPBY not support");
  } else if (spec.orderType==FULLSORT) {
    Timer timer;
    sort_all(spec.sortType);
    LOG("Sort in-memory partition [%u,%u) time: %.3lf", 0, _num_partition, (timer.now()-timer.last())/1000000000.0);
  }
  merger->addMergeEntry(new MemoryMergeEntry(this));
  merger->merge();
  delete merger;
  for (size_t i=0;i<_spills.size();i++) {
    delete readers[i];
    delete inputStreams[i];
  }
  delete [] readers;
  delete [] inputStreams;
  delete fout;
  // write index
  IndexRange * spill_range = writer->getIndex(0);
  PartitionIndex * spill_info = new PartitionIndex(_num_partition);
  spill_info->add(spill_range);
  spill_info->writeIFile(idx_file_path);
  delete spill_info;
  delete_temp_spill_files();
  reset();
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





