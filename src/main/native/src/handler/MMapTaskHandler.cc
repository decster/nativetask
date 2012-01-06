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
#include "util/StringUtil.h"
#include "TaskCounters.h"
#include "MMapTaskHandler.h"
#include "NativeObjectFactory.h"
#include "MapOutputCollector.h"

namespace NativeTask {

MMapTaskHandler::MMapTaskHandler() :
    _numPartition(1),
    _config(NULL),
    _reader(NULL),
    _mapper(NULL),
    _partitioner(NULL),
    _combinerCreator(NULL),
    _moc(NULL),
    _writer(NULL),
    _mapInputRecords(NULL),
    _mapInputBytes(NULL),
    _mapOutputRecords(NULL),
    _mapOutputBytes(NULL) {
}

MMapTaskHandler::~MMapTaskHandler() {
  reset();
}

void MMapTaskHandler::reset() {
  delete _reader;
  _reader = NULL;
  delete _mapper;
  _mapper = NULL;
  _combinerCreator = NULL;
  delete _partitioner;
  _partitioner = NULL;
  delete _moc;
  _moc = NULL;
  delete _writer;
  _writer = NULL;
}

void MMapTaskHandler::initCounters() {
  _mapInputRecords = NativeObjectFactory::GetCounter(
      TaskCounters::TASK_COUNTER_GROUP,
      TaskCounters::MAP_INPUT_RECORDS);
  _mapInputBytes = NativeObjectFactory::GetCounter(
      TaskCounters::TASK_COUNTER_GROUP,
      TaskCounters::MAP_INPUT_BYTES);
  _mapOutputRecords = NativeObjectFactory::GetCounter(
      TaskCounters::TASK_COUNTER_GROUP,
      TaskCounters::MAP_OUTPUT_RECORDS);
  _mapOutputBytes = NativeObjectFactory::GetCounter(
      TaskCounters::TASK_COUNTER_GROUP,
      TaskCounters::MAP_OUTPUT_BYTES);
}

void MMapTaskHandler::configure(Config & config) {
  initCounters();

  _config = &config;
  _numPartition = config.getInt("mapred.reduce.tasks", 1);

  const char * readerClass = config.get("native.recordreader.class");
  if (NULL == readerClass) {
    THROW_EXCEPTION(IOException, "native.recordreader.class not set");
  }
  _reader = (RecordReader*) NativeObjectFactory::CreateObject(readerClass);
  if (NULL == _reader) {
    THROW_EXCEPTION_EX(UnsupportException, "%s not found", readerClass);
  }
  _reader->configure(config);

  if (_numPartition > 0) {
    // collector
    _moc = new MapOutputCollector(_numPartition);
    _moc->configure(config);

    // combiner
    const char * combinerClass = config.get("native.combiner.class");
    if (NULL != combinerClass) {
      _combinerCreator = NativeObjectFactory::GetObjectCreator(combinerClass);
      if (NULL == _combinerCreator) {
        THROW_EXCEPTION_EX(UnsupportException, "Combiner not found: %s", combinerClass);
      }
    }

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
      THROW_EXCEPTION(IOException, "Partitioner not found");
    }
    _partitioner->configure(config);

    LOG("Native Mapper with MapOutputCollector, RecordReader: %s Combiner: %s Partitioner: %s",
        readerClass?readerClass:"Java RecordReader",
        combinerClass?combinerClass:"null",
        partitionerClass?partitionerClass:"default");
  }
  else {
    const char * writerClass = config.get("native.recordwriter.class");
    if (NULL == writerClass) {
      THROW_EXCEPTION(IOException, "RecordWriter not found");
    }
    _writer = (RecordWriter*) NativeObjectFactory::CreateObject(writerClass);
    _writer->configure(config);
    LOG("Native Mapper with RecordReader: %s RecordWriter: %s", readerClass?readerClass:"Java RecordReader", writerClass);
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
  _mapper->setCollector(this);
  _mapper->configure(config);
}

void MMapTaskHandler::collect(const void * key, uint32_t keyLen,
    const void * value, uint32_t valueLen, int partition) {
  _mapOutputRecords->increase();
  _mapOutputBytes->increase(keyLen + valueLen);
  if (NULL != _moc) {
    int result =_moc->put(key, keyLen, value, valueLen, partition);
    if (result==0) {
      return;
    }
    string spillpath = this->sendCommand("GetSpillPath");
    if (spillpath.length() == 0) {
      THROW_EXCEPTION(IOException, "Illegal(empty) spill files path");
    }
    vector<string> pathes;
    StringUtil::Split(spillpath, ";", pathes);
    _moc->mid_spill(pathes,"", _moc->getMapOutputSpec(), _combinerCreator);
    result =_moc->put(key, keyLen, value, valueLen, partition);
    if (0 != result) {
      // should not get here, cause _moc will throw Exceptions
      THROW_EXCEPTION(OutOfMemoryException, "key/value pair larger than io.sort.mb");
    }
  } else {
    THROW_EXCEPTION(UnsupportException, "Collect with partition not support");
  }
}

void MMapTaskHandler::collect(const void * key, uint32_t keyLen,
                     const void * value, uint32_t valueLen) {
  if (NULL != _moc) {
    uint32_t partition = _partitioner->getPartition((const char *) key, keyLen,
        _numPartition);
    collect(key, keyLen, value, valueLen, partition);
  } else {
    _mapOutputRecords->increase();
    _mapOutputBytes->increase(keyLen + valueLen);
    _writer->collect(key, keyLen, value, valueLen);
  }
}

string MMapTaskHandler::command(const string & cmd) {
  if (cmd != "run") {
    THROW_EXCEPTION_EX(UnsupportException, "command not support [%s]", cmd.c_str());
  }
  if (_reader==NULL || _mapper==NULL) {
    THROW_EXCEPTION(IOException, "MMapTaskHandler not setup yet");
  }
  NativeObjectFactory::SetTaskProgressSource(_reader);
  Buffer key;
  Buffer value;
  while (_reader->next(key, value)) {
    _mapInputRecords->increase();
    _mapInputBytes->increase(key.length() + value.length());
    _mapper->map(key.data(), key.length(), value.data(), value.length());
  }
  close();
  NativeObjectFactory::SetTaskProgressSource(NULL);
  return string();
}

void MMapTaskHandler::close() {
  _mapper->close();
  if (NULL != _moc) {
    string outputpath = this->sendCommand("GetOutputPath");
    string indexpath = this->sendCommand("GetOutputIndexPath");
    if ((outputpath.length() == 0) || (indexpath.length() == 0)) {
      THROW_EXCEPTION(IOException, "Illegal(empty) map output file/index path");
    }
    vector<string> pathes;
    StringUtil::Split(outputpath, ";", pathes);
    _moc->final_merge_and_spill(pathes, indexpath, _moc->getMapOutputSpec(), _combinerCreator);
  } else {
    _writer->close();
  }
}

} // namespace NativeTask


