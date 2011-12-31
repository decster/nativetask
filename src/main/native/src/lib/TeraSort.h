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

#ifndef TERASORT_H_
#define TERASORT_H_

#include <stdint.h>
#include "NativeTask.h"
#include "LineRecordReader.h"
#include "LineRecordWriter.h"

namespace NativeTask {

class TeraGen {
protected:
  uint64_t _currentRow;
  uint64_t _endRow;
  int64_t _seed;
  char _keyBytes[128];
  char _valueBytes[128];

  int64_t nextSeed();
public:
  TeraGen(uint64_t row, uint64_t splits, uint64_t index);

  ~TeraGen();

  void reset(uint64_t row, uint64_t splits, uint64_t index);

  bool next(string & key, string & value);

  bool next(string & kv);

  bool nextLine(string & line);
};

class TeraRecordReader : public LineRecordReader {
public:
  virtual bool next(Buffer & key, Buffer & value);
};

class TeraRecordWriter : public LineRecordWriter {
public:
  virtual void collect(const void * key, uint32_t keyLen,
                     const void * value, uint32_t valueLen);
};

} // namespace NativeTask


#endif /* TERASORT_H_ */
