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

#ifndef WORDCOUNT_H_
#define WORDCOUNT_H_

#include "NativeTask.h"
#include "LineRecordWriter.h"

namespace Hadoop {

class WordCountMapper : public Mapper {
protected:
  uint8_t _spaces[256];
public:
  WordCountMapper();

  virtual void map(const char * key, uint32_t keyLen,
                   const char * value, uint32_t valueLen);
};

class WordCountReducer : public Reducer {
public:
  virtual void reduce(KeyGroup & input);
};

class WordCountRecordWriter : public LineRecordWriter {
public:
  virtual void write(const Buffer & key, const Buffer & value);
};

} // namespace Hadoop


#endif /* WORDCOUNT_H_ */
