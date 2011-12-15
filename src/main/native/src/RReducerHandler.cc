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
  _dest(NULL),
  _current(NULL),
  _remain(0),
  _kvlength(0),
  _klength(0),
  _vlength(0),
  _reducerThreadError(false),
  _inputBufferFull(false),
  _needFlushOutput(false),
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
  delete _dest;
  _dest = NULL;
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
    // TODO: configure in reducerThread?
    _reducer->configure(config);
    break;
  case MapperType:
    _mapper->configure(config);
    break;
  case FolderType:
    _folder->configure(config);
    // TODO: implement
    THROW_EXCEPTION(UnsupportException, "Folder API not supported");
    break;
  default:
    // should not be here
    THROW_EXCEPTION(UnsupportException, "Reducer type not supported");
  }

  if (_reducer != NULL) {
    startReducerThread();
  }
}

void RReducerHandler::finish() {
  if (_reducer != NULL) {
    joinReducerThread();
    if (_reducerThreadError) {
      THROW_EXCEPTION_EX(IOException, "Reducer thread throw exception: %s",
                         _errorMessage.c_str());
    }
  } else {
    if (_mapper!=NULL) {
      _mapper->close();
    }
    if (_folder!=NULL) {
      // TODO: _folder finals
    }
    if (_writer!=NULL) {
      _writer->close();
    }
  }
  BatchHandler::finish();
}

std::string RReducerHandler::command(const std::string & cmd) {
  THROW_EXCEPTION(UnsupportException, "Not supported by RReducerHandler");
}

void RReducerHandler::handleInput(char * buff, uint32_t length) {
  switch (_reducerType) {
  case ReducerType:
    asynHandleInput(buff, length);
    break;
  case MapperType:
    syncHandleInput(buff, length);
    break;
  case FolderType:
    THROW_EXCEPTION(UnsupportException, "Folder not supported");
    break;
  }
}

void RReducerHandler::syncHandleInput(char * buff, uint32_t length) {
  if (unlikely(_remain > 0)) {
    uint32_t cp = _remain < length ? _remain : length;
    memcpy(_dest+_kvlength-_remain, buff, cp);
    length -= cp;
    buff += cp;
    _remain -= cp;
    if (0 == _remain) {
      _mapper->map(_dest, _klength, _dest+_klength, _vlength);
      delete _dest;
      _dest = NULL;
    }
  }
  while (length > 0) {
    if (unlikely(length<2*sizeof(uint32_t))) {
      THROW_EXCEPTION(IOException, "k/v length information incomplete");
    }
    uint32_t klength = ((uint32_t*)buff)[0];
    uint32_t vlength = ((uint32_t*)buff)[1];
    buff += 2*sizeof(uint32_t);
    length -= 2*sizeof(uint32_t);
    uint32_t kvlength = klength+vlength;
    // TODO: optimize length==0
    if (kvlength <= length) {
      _mapper->map(buff, klength, buff+klength, vlength);
      buff += kvlength;
      length -= kvlength;
    } else {
      _dest = new char[kvlength];
      _klength = klength;
      _vlength = vlength;
      _kvlength = kvlength;
      memcpy(_dest, buff, length);
      _remain = kvlength-length;
      return;
    }
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

void * ReducerThreadFunction(void * pData) {
  ((RReducerHandler*)pData)->runReducer();
  return NULL;
}

void RReducerHandler::startReducerThread() {
  pthread_mutex_init(&_mutex, NULL);
  pthread_cond_init(&_mainThreadCond, NULL);
  pthread_cond_init(&_reducerThreadCond, NULL);
  //LOG("++ (M) start reducer thread");
  int ret = pthread_create(&_reducerThread, NULL,
      ReducerThreadFunction, this);
  if (ret != 0) {
    THROW_EXCEPTION(IOException, "Create reducer thread failed");
  }
}

void RReducerHandler::joinReducerThread() {
  //LOG("++ (M) join reducer thread");
  int ret =pthread_join((pthread_t)_reducerThread, NULL);
  if (ret!=0) {
    THROW_EXCEPTION(IOException, "Reducer thread join failed");
  }
  pthread_mutex_destroy(&_mutex);
  pthread_cond_destroy(&_mainThreadCond);
  pthread_cond_destroy(&_reducerThreadCond);
}

void RReducerHandler::runReducer() {
  //LOG("+++ (R) runReducer");
  try {
    waitRead();
    while (nextKey()) {
      _reducer->reduce(*this);
    }
    _reducer->close();
    if (_writer != NULL) {
      _writer->close();
    }
  } catch (std::exception e) {
    _reducerThreadError = true;
    _errorMessage = e.what();
  }
  //LOG("+++ (R) runReducer finished");
}

void RReducerHandler::flushOutput(uint32_t length) {
  if (_reducerType==ReducerType) {
    _obPosition = length;
    _needFlushOutput = true;
    //LOG("++ (R)signal need flush Output");
    pthread_cond_signal(&_mainThreadCond);
    pthread_mutex_lock(&_mutex);
    while (_needFlushOutput) {
      //LOG("++ (R)wait flush Output finish");
      pthread_cond_wait(&_reducerThreadCond, &_mutex);
    }
    //LOG("++ (R)wait flush Output finish succeed");
    pthread_mutex_unlock(&_mutex);
  } else {
    BatchHandler::flushOutput(length);
  }
}

void RReducerHandler::asynHandleInput(char * buff, uint32_t length) {
  // TODO: notice reduce thread that new input data arrived
  pthread_mutex_lock(&_mutex);
  //LOG("++ (M) signal buffer full");
  _ibPosition = length;
  _inputBufferFull = true;
  pthread_cond_signal(&_reducerThreadCond);
  while (true) {
    //LOG("++ (M) wait buffer empty or flush output");
    pthread_cond_wait(&_mainThreadCond, &_mutex);
    if (_inputBufferFull==false) {
      break;
    } else if (_needFlushOutput) {
      BatchHandler::flushOutput(_obPosition);
      //LOG("++ (M) signal flush output finished");
      _needFlushOutput = false;
      pthread_cond_signal(&_reducerThreadCond);
    }
  }
  //LOG("++ (M) handle input finished");
  pthread_mutex_unlock(&_mutex);
}

void RReducerHandler::waitRead() {
  //LOG("++ (R)signal buffer empty")
  _inputBufferFull = false;
  pthread_cond_signal(&_mainThreadCond);
  pthread_mutex_lock(&_mutex);
  while (_inputBufferFull==false) {
    //LOG("+++ (R)wait buffer full");
    pthread_cond_wait(&_reducerThreadCond, &_mutex);
  }
  //LOG("+++ (R)wait buffer full succeed");
  pthread_mutex_unlock(&_mutex);
  _current = _ib.buff;
  _remain = _ibPosition;
}

char * RReducerHandler::readKVPair() {
  if (_remain==0) {
    waitRead();
  }
  if (_remain<8) {
    THROW_EXCEPTION(IOException, "not enough meta to read kv pair");
  }
  _klength = ((uint32_t*)_current)[0];
  if (_klength==EndOfInput) {
    _keyGroupIterState = NO_MORE;
    return false;
  }
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
        waitRead();
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

