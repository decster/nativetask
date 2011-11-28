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

#ifndef RREDUCERHANDLER_H_
#define RREDUCERHANDLER_H_

#include <pthread.h>
#include "NativeTask.h"
#include "BatchHandler.h"

namespace Hadoop {

class RReducerHandler :
    public BatchHandler,
    public Collector,
    public KeyGroup {
  friend void * ReducerThreadFunction(void *);
  static const uint32_t EndOfInput = (uint32_t)-1;
  enum KeyGroupIterState {
    INIT,
    NEW_KEY,
    SAME_KEY,
    NO_MORE,
  };
protected:
  NativeObjectType _reducerType;
  // passive mapper (hash aggregation) style reducer
  Mapper  * _mapper;
  // active style reducer
  Reducer * _reducer;
  // passive folder style reducer
  Folder  * _folder;
  // state info KV pairs
  char * _dest;
  char * _current;
  uint32_t _remain;
  uint32_t _kvlength;
  uint32_t _klength;
  uint32_t _vlength;
  // thread for run active _reducer
  pthread_t _reducerThread;
  pthread_mutex_t _mutex;
  pthread_cond_t _mainThreadCond;
  pthread_cond_t _reducerThreadCond;
  volatile bool _inputBufferFull;
  volatile bool _needFlushOutput;
  bool _reducerThreadError;
  std::string _errorMessage;

  volatile uint32_t _ibPosition;
  volatile uint32_t _obPosition;

  KeyGroupIterState _keyGroupIterState;
  std::string _currentGroupKey;
  char * _inplaceKVBuffer;
  char * _KVBuffer;
  uint32_t _KVBufferCapacity;
public:
  RReducerHandler();
  virtual ~RReducerHandler();

  virtual void setup();
  virtual void finish();
  virtual std::string command(const std::string & cmd);
  virtual void handleInput(char * buff, uint32_t length);
  virtual void flushOutput(uint32_t length);

  // KeyGroup methods
  bool nextKey();
  virtual const char * getKey(uint32_t & len);
  virtual const char * nextValue(uint32_t & len);

  // Collector methods
  virtual void collect(const void * key, uint32_t keyLen, const void * value,
      uint32_t valueLen, int partition);
  virtual void collect(const void * key, uint32_t keyLen, const void * value,
      uint32_t valueLen);
  virtual void close();
private:
  void syncHandleInput(char * buff, uint32_t length);
  void asynHandleInput(char * buff, uint32_t length);
  void startReducerThread();
  void joinReducerThread();
  void runReducer();
  virtual void waitRead();
  char * readKVPair(); // return key position
  void reset();
};

} // namespace Hadoop


#endif /* RREDUCERHANDLER_H_ */
