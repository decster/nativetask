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

#ifndef LINERECORDREADER_H_
#define LINERECORDREADER_H_

#include "NativeTask.h"
#include "Buffers.h"
#include "Streams.h"

namespace Hadoop {

class LineRecordReader : public RecordReader {
protected:
  InputStream * _orig;
  InputStream * _source;
  uint64_t _start;
  uint64_t _pos;
  uint64_t _end;
  DynamicBuffer _buffer;
  uint32_t _bufferHint;
  bool _hasOrig;
  uint64_t _inputLength;

  /**
   * read next line in to <code>line</code>
   * @param line contains memory position & length of the line read
   * @return bytes of the newline including the ending \r or \n
   *         0 if no more line
   */
  uint32_t readLine(Buffer & line);
public:
  LineRecordReader();

  void init(InputStream * stream, const string & codec);

  void init(const string & file, uint64_t start, uint64_t length, Config & config);

  virtual ~LineRecordReader();

  virtual void configure(Config & config);

  /**
   * @param key contains nothing
   * @param value contains the next line read
   */
  virtual bool next(Buffer & key, Buffer & value);

  virtual float getProgress();

  virtual void close();
};

class KeyValueLineRecordReader : public LineRecordReader {
protected:
  char _kvSeparator;

public:
  KeyValueLineRecordReader() : _kvSeparator('\t') {}

  virtual void configure(Config & config);

  virtual bool next(Buffer & key, Buffer & value);
};

} // namespace Hadoop

#endif /* LINERECORDREADER_H_ */
