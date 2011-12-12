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

#ifndef BUFFERS_H_
#define BUFFERS_H_

#include "commons.h"
#include "Streams.h"
#include "Compressions.h"

namespace Hadoop {


/**
 * Memory buffer with direct address content, so can be easily copied or
 * dumped to file
 */
struct InplaceBuffer {
  uint32_t length;
  char content[1];

  InplaceBuffer & next() {
    return *(InplaceBuffer*) (content + length);
  }

  uint32_t memory() {
    return sizeof(uint32_t) + length;
  }

  std::string str() {
    return std::string(content, length);
  }
};


class DynamicBuffer {
protected:
  char * _data;
  uint32_t _capacity;
  uint32_t _size;
  uint32_t _used;
public:
  DynamicBuffer();

  DynamicBuffer(uint32_t capacity);

  ~DynamicBuffer();

  void reserve(uint32_t capacity);

  void release();

  uint32_t capacity() {
    return _capacity;
  }

  char * data() {
    return _data;
  }

  uint32_t size() {
    return _size;
  }

  uint32_t used() {
    return _used;
  }

  char * current() {
    return _data + _used;
  }

  char * end() {
    return _data + _size;
  }

  uint32_t remain() {
    return _size - _used;
  }

  uint32_t freeSpace() {
    return _capacity - _size;
  }

  void use(uint32_t count) {
    _used += count;
  }

  void cleanUsed();

  int32_t refill(InputStream * stream);
};


/**
 * A lightweight read buffer, act as buffered input stream
 */
class ReadBuffer {
protected:
  char *   _buff;
  uint32_t _remain;
  uint32_t _size;
  uint32_t _capacity;

  InputStream * _stream;
  InputStream * _source;

protected:
  inline char * current() {
    return _buff + _size - _remain;
  }

  char * fillGet(uint32_t count);
  int32_t fillRead(char * buff, uint32_t len);
  int64_t fillReadVLong();
public:
  ReadBuffer();

  void init(uint32_t size, InputStream * stream, const string & codec);

  ~ReadBuffer();

  /**
   * use get() to get inplace continuous memory of small object
   */
  inline char * get(uint32_t count) {
    if (likely(count <= _remain)) {
      char * ret = current();
      _remain -= count;
      return ret;
    }
    return fillGet(count);
  }


  /**
   * read to outside buffer
   */
  inline int32_t read(char * buff, uint32_t len) {
    if (likely(len <= _remain)) {
      memcpy(buff, current(), len);
      _remain -= len;
      return len;
    }
    return fillRead(buff, len);
  }

  /**
   * read to outside buffer, use simple_memcpy
   * so buff must be have other remain space(buff.remain>=7)
   */
  inline void readUnsafe(char * buff, uint32_t len) {
    if (likely(len <= _remain)) {
      simple_memcpy(buff, current(), len);
      _remain -= len;
      return;
    }
    fillRead(buff, len);
  }

  /**
   * read VUInt
   */
  inline int64_t readVLong() {
    if (likely(_remain>0)) {
      char * mark = current();
      if (*(int8_t*)mark>=(int8_t)-112) {
        _remain--;
        return (int64_t)*mark;
      }
    }
    return fillReadVLong();
  }

  /**
   * read uint32_t little endian
   */
  inline uint32_t read_uint32_le() {
    return *(uint32_t*)get(4);
  }

  /**
   * read uint32_t big endian
   */
  inline uint32_t read_uint32_be() {
    return bswap(read_uint32_le());
  }
};


/**
 * A light weighted append buffer, used as buffered output streams
 */
class AppendBuffer {
protected:
  char *    _buff;
  uint32_t  _remain;
  uint32_t  _capacity;
  uint64_t  _counter;

  OutputStream * _stream;
  OutputStream * _dest;

protected:
  void flushd();

  inline char * current() {
    return _buff + _capacity - _remain;
  }

  void write_inner(const void * data, uint32_t len);
  void write_vlong_inner(int64_t v);
  void write_vuint2_inner(uint32_t v1, uint32_t v2);
public:
  AppendBuffer();

  ~AppendBuffer();

  void init(uint32_t size, OutputStream * stream, const string & codec);

  uint64_t getCounter() {
    return _counter;
  }

  inline char * borrowUnsafe(uint32_t len) {
    if (likely(_remain >= len)) {
      return current();
    }
    if (likely(_capacity >= len)) {
      flushd();
      return _buff;
    }
    return NULL;
  }

  inline void useUnsafe(uint32_t count) {
    _remain -= count;
  }

  inline void write(char c) {
    if (unlikely(_remain==0)) {
      flushd();
    }
    *current() = c;
    _remain--;
  }

  inline void write(const void * data, uint32_t len) {
    if (likely(len <= _remain)) { // append directly
      simple_memcpy(current(), data, len);
      _remain -= len;
      return;
    }
    write_inner(data, len);
  }

  inline void write_uint32_le(uint32_t v) {
    if (unlikely(4 > _remain)) {
      flushd();
    }
    *(uint32_t*)current() = v;
    _remain -= 4;
    return;
  }

  inline void write_uint32_be(uint32_t v) {
    write_uint32_le(bswap(v));
  }

  inline void write_uint64_le(uint64_t v) {
    if (unlikely(8 > _remain)) {
      flushd();
    }
    *(uint64_t*)current() = v;
    _remain -= 8;
    return;
  }

  inline void write_uint64_be(uint64_t v) {
    write_uint64_le(bswap64(v));
  }

  inline void write_vlong(int64_t v) {
    if (likely(_remain>0 && v<=127 && v>=-112)) {
      *(char*)current() = (char)v;
      _remain--;
      return;
    }
    write_vlong_inner(v);
  }

  inline void write_vuint(uint32_t v) {
    if (likely(_remain>0 && v<=127)) {
      *(char*)current() = (char)v;
      _remain--;
      return;
    }
    write_vlong_inner(v);
  }

  inline void write_vuint2(uint32_t v1, uint32_t v2) {
    if (likely(_remain>=2 && v1<=127 && v2<=127)) {
      *(char*)current() = (char)v1;
      *(char*)(current()+1) = (char)v2;
      _remain-=2;
      return;
    }
    write_vuint2_inner(v1, v2);
  }

  /**
   * flush current buffer, clear content
   */
  inline void flush() {
    if (_remain<_capacity) {
      flushd();
    }
  }
};

} // namespace Hadoop

#endif /* BUFFERS_H_ */
