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
#include "NativeObjectFactory.h"
#include "RReducerHandler.h"

namespace Hadoop {

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
  _keyGroupIterState(INIT),
  _inplaceKVBuffer(NULL),
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
  delete _writer;
  _writer = NULL;
  delete _KVBuffer;
  _KVBuffer = NULL;
}

void RReducerHandler::configure(Config & config) {
  // writer
  const char * writerClass = config.get("native.recordwriter.class");
  if (NULL != writerClass) {
    _writer = (RecordWriter*) NativeObjectFactory::CreateObject(writerClass);
    if (NULL == _writer) {
      THROW_EXCEPTION_EX(IOException, "native.recordwriter.class %s not found", writerClass);
    }
    _writer->configure(config);
  }

  Collector * collector = _writer != NULL
      ? (Collector*) _writer
      : (Collector*) this;

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
      _reducer->setCollector(collector);
      break;
    case MapperType:
      _mapper = (Mapper*)obj;
      _mapper->setCollector(collector);
      break;
    case FolderType:
      _folder = (Folder*)obj;
      _folder->setCollector(collector);
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
    _mapper->setCollector(collector);
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
        _reducer->reduce(*this);
      }
      _reducer->close();
    }
    break;
  case MapperType:
    {
      char * kvpair;
      while(NULL != (kvpair = readKVPair())) {
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
    const void * value, uint32_t valueLen, int partition) {
  THROW_EXCEPTION(UnsupportException, "Collect with partition not support in reducer");
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
  if (_hasJavaException) {
    THROW_EXCEPTION(JavaException, "java side throw an exception when call refill");
  }
  int32_t retvalue = *((const int32_t*)ret.data());
  _current = _ib.buff;
  _remain = retvalue;
  return retvalue;
}

char * RReducerHandler::readKVPair() {
  if (_remain==0) {
    if (refill() <= 0) {
      return NULL;
    }
  }
  if (_remain<8) {
    THROW_EXCEPTION(IOException, "not enough meta to read kv pair");
  }
  _klength = ((uint32_t*)_current)[0];
  _vlength = ((uint32_t*)_current)[1];
  _kvlength = _klength + _vlength;
  _current += 8;
  _remain -= 8;
  char * keyPos;
  _inplaceKVBuffer = NULL;
  if (_kvlength <= _remain) {
    _inplaceKVBuffer = _current;
    _current += _kvlength;
    _remain -= _kvlength;
    return _inplaceKVBuffer;
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
    return _KVBuffer;
  }
}

bool RReducerHandler::nextKey() {
  while (true) {
    char * pos;
    switch (_keyGroupIterState) {
    case INIT:
      pos = readKVPair();
      if (pos == NULL){
        _keyGroupIterState = NO_MORE;
        return false;
      }
      _keyGroupIterState = NEW_KEY;
      _currentGroupKey.assign(pos, _klength);
      return true;
    case NEW_KEY:
      return true;
    case SAME_KEY:
      pos = readKVPair();
      if (pos == NULL) {
        _keyGroupIterState = NO_MORE;
        return false;
      }
      if (_klength == _currentGroupKey.length()) {
        if (fmemeq(pos, _currentGroupKey.c_str(), _klength)) {
          continue;
        }
      }
      _currentGroupKey.assign(pos, _klength);
      _keyGroupIterState = NEW_KEY;
      return true;
    case NO_MORE:
      return false;
    }
  }
}

const char * RReducerHandler::getKey(uint32_t & len) {
  len = (uint32_t)_currentGroupKey.length();
  return _currentGroupKey.c_str();
}

const char * RReducerHandler::nextValue(uint32_t & len) {
  char * pos;
  switch (_keyGroupIterState) {
  case NEW_KEY:
    len = _vlength;
    _keyGroupIterState = SAME_KEY;
    return (_inplaceKVBuffer != NULL ? _inplaceKVBuffer : _KVBuffer) + _klength;
  case SAME_KEY:
    pos = readKVPair();
    if (pos != NULL) {
      if (_klength == _currentGroupKey.length()) {
        if (fmemeq(pos, _currentGroupKey.c_str(), _klength)) {
          len = _vlength;
          return pos + _klength;
        }
      }
      _currentGroupKey.assign(pos, _klength);
      _keyGroupIterState = NEW_KEY;
      return NULL;
    }
    _keyGroupIterState = NO_MORE;
    return NULL;
  case NO_MORE:
    return false;
  }
}


} // namespace Hadoop

