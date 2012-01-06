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
#include "NativeObjectFactory.h"
#include "RReducerHandler.h"

namespace NativeTask {

RReducerHandler::RReducerHandler() :
  _mapper(NULL),
  _reducer(NULL),
  _folder(NULL),
  _writer(NULL),
  _current(NULL),
  _remain(0),
  _kvlength(0),
  _klength(0),
  _vlength(0),
  _keyGroupIterState(NEW_KEY),
  _nextKeyValuePair(NULL),
  _KVBuffer(NULL),
  _KVBufferCapacity(0) {
}

RReducerHandler::~RReducerHandler() {
  reset();
}

void RReducerHandler::reset() {
  delete _mapper;
  _mapper = NULL;
  delete _reducer;
  _reducer = NULL;
  delete _folder;
  _folder = NULL;
  delete _collector;
  _collector = NULL;
  delete _writer;
  _writer = NULL;
  delete _KVBuffer;
  _KVBuffer = NULL;
}

void RReducerHandler::initCounters() {
   _reduceInputRecords = NativeObjectFactory::GetCounter(
       TaskCounters::TASK_COUNTER_GROUP,
       TaskCounters::REDUCE_INPUT_RECORDS);
   _reduceOutputRecords = NativeObjectFactory::GetCounter(
       TaskCounters::TASK_COUNTER_GROUP,
       TaskCounters::REDUCE_OUTPUT_RECORDS);
   _reduceInputGroups = NativeObjectFactory::GetCounter(
       TaskCounters::TASK_COUNTER_GROUP,
       TaskCounters::REDUCE_INPUT_GROUPS);
}

class TrackingCollector : public Collector {
protected:
  Collector * _collector;
  Counter * _counter;
public:
  TrackingCollector(Collector * collector, Counter * counter) :
    _collector(collector),
    _counter(counter) {
  }

  virtual void collect(const void * key, uint32_t keyLen,
                       const void * value, uint32_t valueLen) {
    _counter->increase();
    _collector->collect(key, keyLen, value, valueLen);
  }
};

void RReducerHandler::configure(Config & config) {
  initCounters();

  // writer
  const char * writerClass = config.get("native.recordwriter.class");
  if (NULL != writerClass) {
    _writer = (RecordWriter*) NativeObjectFactory::CreateObject(writerClass);
    if (NULL == _writer) {
      THROW_EXCEPTION_EX(IOException, "native.recordwriter.class %s not found", writerClass);
    }
    _writer->configure(config);
  }

  _collector = new TrackingCollector(_writer != NULL
                                         ? (Collector*) _writer
                                         : (Collector*) this,
                                     _reduceOutputRecords);
  // reducer
  const char * reducerClass = config.get("native.reducer.class");
  if (NULL != reducerClass) {
    NativeObject * obj = NativeObjectFactory::CreateObject(reducerClass);
    if (NULL == obj) {
      THROW_EXCEPTION_EX(IOException, "native.reducer.class %s not found", reducerClass);
    }
    _reducerType = obj->type();
    switch (_reducerType) {
    case ReducerType:
      _reducer = (Reducer*)obj;
      _reducer->setCollector(_collector);
      break;
    case MapperType:
      _mapper = (Mapper*)obj;
      _mapper->setCollector(_collector);
      break;
    case FolderType:
      _folder = (Folder*)obj;
      _folder->setCollector(_collector);
      break;
    default:
        THROW_EXCEPTION(UnsupportException, "Reducer type not supported");
    }
  }
  else {
    // default use IdenticalMapper
    _reducerType = MapperType;
    _mapper = (Mapper *) NativeObjectFactory::CreateDefaultObject(
        MapperType);
    _mapper->setCollector(_collector);
  }
  if (NULL == _reducer && _mapper == NULL && _folder == NULL) {
    THROW_EXCEPTION(UnsupportException, "Reducer class not found");
  }

  switch (_reducerType) {
  case ReducerType:
    LOG("Native Reducer with Reducer API, RecordWriter: %s", writerClass?writerClass:"Java RecordWriter");
    _reducer->configure(config);
    break;
  case MapperType:
    LOG("Native Reducer with Mapper API, RecordWriter: %s", writerClass?writerClass:"Java RecordWriter");
    _mapper->configure(config);
    break;
  case FolderType:
    LOG("Native Reducer with Folder API, RecordWriter: %s", writerClass?writerClass:"Java RecordWriter");
    _folder->configure(config);
    // TODO: implement
    THROW_EXCEPTION(UnsupportException, "Folder API not supported");
    break;
  default:
    // should not be here
    THROW_EXCEPTION(UnsupportException, "Reducer type not supported");
  }
}

std::string RReducerHandler::command(const std::string & cmd) {
  if (cmd == "run") {
    run();
    return string();
  }
  THROW_EXCEPTION(UnsupportException, "Command not supported by RReducerHandler");
}

void RReducerHandler::run() {
  switch (_reducerType) {
  case ReducerType:
    {
      while (nextKey()) {
        _reduceInputGroups->increase();
        _reducer->reduce(*this);
      }
      _reducer->close();
    }
    break;
  case MapperType:
    {
      char * kvpair;
      while(NULL != (kvpair = nextKeyValuePair())) {
        _mapper->map(kvpair, _klength, kvpair + _klength, _vlength);
      }
      _mapper->close();
    }
    break;
  case FolderType:
    // TODO: implement
    THROW_EXCEPTION(UnsupportException, "Folder API not supported");
    break;
  default:
    THROW_EXCEPTION(UnsupportException, "Reducer type not supported");
  }
  if (_writer != NULL) {
    _writer->close();
  } else {
    finish();
  }
}


void RReducerHandler::collect(const void * key, uint32_t keyLen,
    const void * value, uint32_t valueLen) {
  putInt(keyLen);
  put((char *)key, keyLen);
  putInt(valueLen);
  put((char *)value, valueLen);
}

int32_t RReducerHandler::refill() {
  string ret = sendCommand("r");
  int32_t retvalue = *((const int32_t*)ret.data());
  _current = _ib.buff;
  _remain = retvalue;
  return retvalue;
}

char * RReducerHandler::nextKeyValuePair() {
  if (unlikely(_remain==0)) {
    if (refill() <= 0) {
      return NULL;
    }
  }
  if (_remain<8) {
    THROW_EXCEPTION(IOException, "not enough meta to read kv pair");
  }
  _reduceInputRecords->increase();
  _klength = ((uint32_t*)_current)[0];
  _vlength = ((uint32_t*)_current)[1];
  _kvlength = _klength + _vlength;
  _current += 8;
  _remain -= 8;
  char * keyPos;
  if (_kvlength <= _remain) {
    _nextKeyValuePair = _current;
    _current += _kvlength;
    _remain -= _kvlength;
    return _nextKeyValuePair;
  } else {
    if (_KVBufferCapacity<_kvlength) {
      delete _KVBuffer;
      _KVBuffer = new char[(_kvlength+2)/2*3];
      _KVBufferCapacity = (_kvlength+2)/2*3;
    }
    uint32_t need = _kvlength;
    while (need>0) {
      if (need <=_remain) {
        memcpy(_KVBuffer+_kvlength-need,_current,need);
        _current += need;
        _remain -= need;
        break;
      } else {
        memcpy(_KVBuffer+_kvlength-need,_current,_remain);
        need -= _remain;
        if (refill() <=0) {
          THROW_EXCEPTION_EX(IOException, "refill reach EOF, kvlength: %u, still need: %u", _kvlength, need);
        }
      }
    }
    _nextKeyValuePair = _KVBuffer;
    return _nextKeyValuePair;
  }
}

bool RReducerHandler::nextKey() {
  uint32_t temp;
  while (_keyGroupIterState==SAME_KEY) {
    nextValue(temp);
  }
  if (_keyGroupIterState ==  NEW_KEY) {
    if (unlikely(_nextKeyValuePair == NULL)) {
      if (NULL == nextKeyValuePair()) {
        _keyGroupIterState = NO_MORE;
        return false;
      }
    }
    _currentGroupKey.assign(_nextKeyValuePair, _klength);
    return true;
  }
  return false;
}

const char * RReducerHandler::getKey(uint32_t & len) {
  len = (uint32_t)_currentGroupKey.length();
  return _currentGroupKey.c_str();
}

const char * RReducerHandler::nextValue(uint32_t & len) {
  char * pos;
  switch (_keyGroupIterState) {
  case SAME_KEY: {
    pos = nextKeyValuePair();
    if (pos != NULL) {
      if (_klength == _currentGroupKey.length()) {
        if (fmemeq(pos, _currentGroupKey.c_str(), _klength)) {
          len = _vlength;
          return pos + _klength;
        }
      }
      _keyGroupIterState = NEW_KEY;
      return NULL;
    }
    _keyGroupIterState = NO_MORE;
    return NULL;
  }
  case NEW_KEY: {
    len = _vlength;
    _keyGroupIterState = SAME_KEY;
    return _nextKeyValuePair + _klength;
  }
  case NO_MORE:
    return false;
  }
}


} // namespace NativeTask

