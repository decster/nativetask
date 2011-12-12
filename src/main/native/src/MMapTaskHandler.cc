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

#include "commons.h"
#include "MMapTaskHandler.h"
#include "NativeObjectFactory.h"
#include "MapOutputCollector.h"

namespace Hadoop {

MMapTaskHandler::MMapTaskHandler() :
    _moc(NULL),
    _mapper(NULL),
    _partitioner(NULL) {
}

MMapTaskHandler::~MMapTaskHandler() {
  reset();
}

void MMapTaskHandler::reset() {
  delete _mapper;
  _mapper = NULL;
  delete _moc;
  _moc = NULL;
  delete _partitioner;
  _partitioner = NULL;
}

void MMapTaskHandler::setup() {
  Config & config = NativeObjectFactory::GetConfig();

  // partitioner
  const char * partitionerClass = config.get("native.partitioner.class");
  if (NULL != partitionerClass) {
    _partitioner
        = (Partitioner *) NativeObjectFactory::CreateObject(partitionerClass);
  }
  else {
    _partitioner
        = (Partitioner *) NativeObjectFactory::CreateDefaultObject(PartitionerType);
  }
  if (NULL == _partitioner) {
    THROW_EXCEPTION(UnsupportException, "Partitioner not found");
  }
  _partitioner->configure(config);

  // collector
  _numPartition = config.getInt("mapred.reduce.tasks", 1);
  if (_numPartition > 0) {
    LOG("Native Mapper with MapOutputCollector");
    _moc = new MapOutputCollector(_numPartition);
    _moc->configure(config);
  }
  else {
    LOG("Native Mapper with java direct output collector");
  }

  // mapper
  const char * mapperClass = config.get("native.mapper.class");
  if (NULL != mapperClass) {
    _mapper = (Mapper *) NativeObjectFactory::CreateObject(mapperClass);
  }
  else {
    _mapper = (Mapper *) NativeObjectFactory::CreateDefaultObject(MapperType);
  }
  if (NULL == _mapper) {
    THROW_EXCEPTION(UnsupportException, "Mapper not found");
  }
  _mapper->configure(config);
  _mapper->setCollector(this);
}

void MMapTaskHandler::collect(const void * key, uint32_t keyLen,
    const void * value, uint32_t valueLen, int partition) {
  if (NULL == _moc) {
    THROW_EXCEPTION(UnsupportException, "Collect with partition not support");
  }
  int result =_moc->put(key, keyLen, value, valueLen, partition);
  if (result==0) {
    return;
  }
  string spillpath = this->sendCommand("GetSpillPath");
  if (hasJavaException()) {
    return;
  }
  if (spillpath.length() == 0) {
    THROW_EXCEPTION(IOException, "Illegal(empty) spill files path");
  }
  vector<string> pathes;
  StringUtil::Split(spillpath, ";", pathes);
  _moc->mid_spill(pathes,"", _moc->getMapOutputSpec());
  result =_moc->put(key, keyLen, value, valueLen, partition);
  if (0 != result) {
    // should not get here, cause _moc will throw Exceptions
    THROW_EXCEPTION(OutOfMemoryException, "key/value pair larger than io.sort.mb");
  }
}

void MMapTaskHandler::collect(const void * key, uint32_t keyLen,
                     const void * value, uint32_t valueLen) {
  if (NULL == _moc) {
    // TODO: use record writer
    return;
  }
  uint32_t partition = _partitioner->getPartition((const char *) key, keyLen,
      _numPartition);
  collect(key, keyLen, value, valueLen, partition);
}

void MMapTaskHandler::close() {
  _mapper->close();
  if (NULL == _moc) {
    // TODO: close record writer
    return;
  }
  string outputpath = this->sendCommand("GetOutputPath");
  if (hasJavaException()) {
    return;
  }
  string indexpath = this->sendCommand("GetOutputIndexPath");
  if (hasJavaException()) {
    return;
  }
  if ((outputpath.length() == 0) || (indexpath.length() == 0)) {
    THROW_EXCEPTION(IOException, "Illegal(empty) map output file/index path");
  }
  vector<string> pathes;
  StringUtil::Split(outputpath, ";", pathes);
  _moc->final_merge_and_spill(pathes, indexpath, _moc->getMapOutputSpec());
}

} // namespace Hadoop


