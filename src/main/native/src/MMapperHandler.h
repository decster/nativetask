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

#ifndef MMAPPERHANDLER_H_
#define MMAPPERHANDLER_H_

#include "NativeTask.h"
#include "BatchHandler.h"

namespace Hadoop {

class MapOutputCollector;

class MMapperHandler :
    public BatchHandler,
    public Collector {
private:
  MapOutputCollector * _moc;
  Mapper * _mapper;
  Partitioner * _partitioner;
  int32_t _numPartition;
  // state info for large KV pairs
  char * _dest;
  uint32_t _kvlength;
  uint32_t _remain;
  uint32_t _klength;
  uint32_t _vlength;
public:
  MMapperHandler();
  virtual ~MMapperHandler();

  virtual void setup();
  virtual void finish();
  virtual void handleInput(char * buff, uint32_t length);

  // Collector methods
  virtual void collect(const void * key, uint32_t keyLen, const void * value,
      uint32_t valueLen, int partition);
  virtual void collect(const void * key, uint32_t keyLen, const void * value,
      uint32_t valueLen);
  virtual void close();
private:
  void reset();
};

} // namespace Hadoop


#endif /* MMAPPERHANDLER_H_ */
