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

#ifndef LINERECORDWRITER_H_
#define LINERECORDWRITER_H_

#include "NativeTask.h"
#include "Streams.h"
#include "Buffers.h"

namespace Hadoop {

class LineRecordWriter : public RecordWriter {
protected:
  OutputStream * _stream;
  AppendBuffer _appendBuffer;
  uint32_t _bufferHint;
  bool _hasStream;
  string _keyValueSeparator;
public:
  LineRecordWriter();

  void init(OutputStream * stream, const string & codec);

  void init(const string & file, Config & config);

  virtual ~LineRecordWriter();

  virtual void configure(Config & config);

  virtual void write(const void * key, uint32_t keyLen,
                     const void * value, uint32_t valueLen);

  virtual void close();
};

} // namespace Hadoop


#endif /* LINERECORDWRITER_H_ */
