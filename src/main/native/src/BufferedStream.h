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

#ifndef BUFFEREDSTREAM_H_
#define BUFFEREDSTREAM_H_

#include "Streams.h"

namespace Hadoop {

class BufferedInputStream : public FilterInputStream {
protected:
  char * _buff;
  uint32_t _position;
  uint32_t _limit;
  uint32_t _capacity;
public:
  BufferedInputStream(InputStream * stream, uint32_t bufferSize = 64 * 1024);

  virtual ~BufferedInputStream();

  virtual void seek(uint64_t position);

  virtual uint64_t tell();

  virtual int32_t read(void * buff, uint32_t length);
};


class BufferedOutputStream : public FilterOutputStream {
protected:
  char * _buff;
  uint32_t _position;
  uint32_t _capacity;

public:
  BufferedOutputStream(InputStream * stream, uint32_t bufferSize = 64 * 1024);

  virtual ~BufferedOutputStream();

  virtual uint64_t tell();

  virtual void write(const void * buff, uint32_t length);

  virtual void flush();

};


} // namespace Hadoop


#endif /* BUFFEREDSTREAM_H_ */
