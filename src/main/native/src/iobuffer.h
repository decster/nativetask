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

#ifndef IOBUFFER_H_
#define IOBUFFER_H_

#include "commons.h"
#include "compression.h"

namespace Hadoop {

/**
 * Light weighted io buffer supporting checksum & compression
 * TODO: support decompression
 */
class ReadBuffer {
protected:
  int read_fd;
  uint32_t capacity;
  uint32_t rest;
  char * buffer;
  char * end;
  uint64_t * offset; // read counter

  bool dochecksum;
  char * check_mark;
  uint32_t checksum;

  CompressionType compression;
protected:
  void wrap() {
//    LOG("wrap: buffer %016lx check_mark %016lx current %016lx end %016lx rest %u",
//        (size_t)buffer,
//        (size_t)check_mark,
//        (size_t)(end-rest),
//        (size_t)end,
//        rest);
    if (dochecksum) {
      if (check_mark < end-rest) {
        checksum_update(checksum, check_mark, end-rest-check_mark);
      }
    }
    if (rest > 0) {
      memcpy(buffer, end - rest, rest);
    }
    check_mark = buffer;
    ssize_t rd;
    READ(rd, read_fd, buffer + rest, capacity - rest);
    rest += rd;
    end = buffer + rest;
  }

  char * wrap_get(uint32_t count) {
    wrap();
    if (rest<count) {
      THROW_EXCEPTION(IOException, "read reach EOF");
    }
    rest -= count;
    *offset += count;
    return buffer;
  }

public:
  ReadBuffer(char * buffer, uint32_t capacity, int read_fd, uint64_t * offset,
      bool dochecksum, CompressionType compression=PLAIN) :
    read_fd(read_fd),
    capacity(capacity),
    rest(0),
    buffer(buffer),
    end(buffer),
    offset(offset),
    dochecksum(dochecksum),
    check_mark(buffer),
    checksum(checksum_init()),
    compression(compression) {
  }


  /**
   * use get() to get continuous memory of small object
   */
  inline char * get(uint32_t count) {
    if (likely(count <= rest)) {
      char * ret = end - rest;
      rest -= count;
      *offset += count;
      return ret;
    }
    else if (count <= capacity) {
      return wrap_get(count);
    }
    else {
      // caller should provide buffer directly
      return NULL;
    }
  }

  /**
   * read large object to buff, normally if len > capacity/2,
   * and buff is the final destination, this method is preferred
   */
  void read_to(char * buff, uint32_t len) {
    if (rest >= len) {
      memcpy(buff, end - len, len);
      rest -= len;
      *offset += len;
    }
    else {
      memcpy(buff, end - rest, rest);
      ssize_t rd;
      READ(rd, read_fd, buff + rest, len - rest);
      if (rd < len - rest) {
        THROW_EXCEPTION(IOException, "read get EOF");
      }
      if (dochecksum) {
        checksum_update(checksum, buff, len);
      }
      rest = 0;
      *offset += len;
      check_mark = end;
    }
  }

  /**
   * read VUInt
   */
  inline uint32_t read_vuint() {
    if (unlikely(rest == 0)) {
      wrap();
    }
    uint32_t len = GetVUInt32BufferLen(end-rest);
    char * mark = get(len);
    return ReadVUInt32(mark);
  }


  /**
   * read uint32_t big endian
   */
  inline uint32_t read_uint_be() {
    char * pos = get(sizeof(uint32_t));
    return ReadUInt32(pos);
  }

  /**
   * read uint32_t little endian
   */
  inline uint32_t read_uint32_le() {
    return *(uint32_t*)get(sizeof(uint32_t));
  }

  void init_checksum() {
    if (dochecksum) {
      checksum = checksum_init();
      check_mark = end - rest;
    }
  }

  void verify_checksum() {
    if (dochecksum) {
      char * current = end-rest;
      checksum_update(checksum, check_mark, current-check_mark);
      check_mark = current;
      char * pos = get(sizeof(uint32_t));
      *offset -= sizeof(uint32_t); // cancel offset add by checksum
      uint32_t verify = (*(uint32_t*)pos);
      check_mark = end - rest;
      if (checksum_val(checksum) != bswap(verify)) {
        THROW_EXCEPTION(IOException, "Checksum verification failed");
      }
    }
  }
};



/**
 * A light weighted append buffer, act as various of hadoop io streams,
 * it must have 1K capacity at least
 * Usage: create ~32K buffer for direct append
 *        create ~256K buffer for append with compression
 */
class AppendBuffer {
public:
  char *    start;
  char *    end;
  char *    cur;
  uint32_t  capacity;

  uint64_t * real_offset;
  uint32_t * checksum;
  int        dump_fd;

  CompressionType compression_type;
  char * compression_buffer;
  size_t compression_buffer_length;

public:
  AppendBuffer(char * buffer, uint32_t size, int fd, uint32_t * checksum,
      uint64_t * real_offset, CompressionType compression=PLAIN) :
      start(buffer),
      end(buffer + size),
      cur(buffer),
      capacity(size),
      real_offset(real_offset),
      checksum(checksum), dump_fd(fd),
      compression_type(compression),
      compression_buffer(NULL),
      compression_buffer_length(0) {
    if (compression_type!=PLAIN) {
      // add original size & compressed size
      compression_buffer_length = CompressionBufferSize(compression_type, size)
          + sizeof(uint32_t)*2;
      compression_buffer = new char[compression_buffer_length];
    }
  }

  ~AppendBuffer() {
    if (NULL != compression_buffer) {
      delete [] compression_buffer;
      compression_buffer = NULL;
      compression_buffer_length = 0;
    }
  }

  uint32_t get_checksum() {
    return *checksum;
  }

  /**
   * reserve_space & reserve_space_used_to are pair of functions:
   * call reserve_space to request small(<32bytes) chunk of memory,
   * call reserve_space_used_to to end use the memory
   * These two method must call together, and argument [len] must
   * less than [capacity]
   */
  inline char * reserve_space(uint32_t len) {
    if (unlikely(len>end-cur)) {
      flush();
    }
    return cur;
  }

  inline void reserve_space_used_to(char * mark) {
    assert(mark>=start);
    assert(mark<=end);
    if (unlikely(mark==end)) {
      inner_flush(start, end);
      cur = start;
    }
    cur = mark;
  }

  /**
   * append data to buffer
   * this method may break data in the middle
   * to fit into the buffer
   */
  inline void append(const void * data, uint32_t len) {
    if (likely(cur+len<=end)) { // append directly
      simple_memcpy(cur,data,len);
      cur += len;
      if (unlikely(cur==end)) {
        flush();
      }
    } else {
      wrap_append(data, len);
    }
  }

  /**
   * append a 4 byte uint32_t
   */
  inline void append(uint32_t val) {
    if (likely(cur+sizeof(uint32_t)<=end)) {
      *(uint32_t*)cur = val;
      cur+=sizeof(uint32_t);
      if (unlikely(cur==end)) {
        flush();
      }
    } else {
      inner_flush(start, cur);
      *(uint32_t*)start = val;
      cur = start+sizeof(uint32_t);
    }
  }

  /**
   * append data to buffer
   * this method ensures data will not be break down
   * to parts, TODO: remove
   */
  void append_no_wrap(const void * data, uint32_t len) {
    uint32_t rest = end-cur;
    if (len<rest) {
      // append directly
      simple_memcpy(cur,data,len);
      cur+=len;
    } else {
      inner_flush(start, cur);
      if (len<capacity) {
        simple_memcpy(start,data,len);
        cur=start+len;
      } else {
        flush((const char*)data, len);
        cur = start;
      }
    }
  }

  /**
   * flush rest & flush data
   * this method ensures that there will be no extra buffer copy
   * TODO: remove
   */
  void flush(const void * data, uint32_t len) {
    flush();
    inner_flush((const char*)data, (const char*)data+len);
  }

  /**
   * flush current buffer, clear content
   */
  void flush() {
    if (cur>start) {
      inner_flush(start, cur);
      cur = start;
    }
  }

protected:
  void inner_flush(const char * start, const char * end) {
    if (PLAIN == compression_type) {
      inner_flush_direct(start, end);
    } else {
      inner_flush_compressed(start, end);
    }
  }

  void inner_flush_direct(const char * start, const char * end) {
    if (NULL != checksum) {
      checksum_update(*checksum, start, (uint32_t)(end-start));
    }
    WRITE(dump_fd, start, end-start);
    if (NULL != real_offset) {
      (*real_offset) += (end-start);
    }
  }

  void inner_flush_compressed(const char * start, const char * end) {
    while (start < end) {
      size_t len = std::min((size_t)capacity, (size_t)(end-start));
      uint32_t * osize = (uint32_t*)compression_buffer;
      uint32_t * csize = osize+1;
      size_t compressed_size = compression_buffer_length - sizeof(uint32_t) * 2;
      CompressBlock(compression_type, start, len,
         compression_buffer + sizeof(uint32_t) * 2, &compressed_size);
      *osize = bswap(len);
      *csize = bswap((uint32_t)compressed_size);
      const char * compressed_end = compression_buffer +
          sizeof(uint32_t) * 2 + compressed_size;
      //LOG("compress: orig: %llu, compressed: %llu", len, compressed_size+sizeof(uint32_t)*2);
      inner_flush_direct(compression_buffer, compressed_end);
      start += len;
    }
  }

  void wrap_append(const void * data, uint32_t len) {
    if (cur == start) { // empty buffer, and len>capacity
      // doesn't need to copy, use source buffer to flush
      inner_flush((const char*)data, (const char *)data+len);
    }
    else {
      uint32_t rest = end-cur;
      simple_memcpy(cur,data,rest);
      inner_flush(start, end);
      if (len-rest >= capacity) {
        // doesn't need to copy, use source buffer to flush
        inner_flush((const char*)data+rest, (const char *)data+rest);
        cur = start;
      } else {
        simple_memcpy(start, (const char*)data+rest,len-rest);
        cur = start + len - rest;
      }
    }
  }

};


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

  void copy_to(char * dest) {
    memcpy(dest, this, memory());
  }
};

/**
 * Memory Key-Value buffer pair with direct address content, so can be
 * easily copied or dumped to file
 */
struct KVBuffer {
  InplaceBuffer key;

  InplaceBuffer & get_key() {
    return key;
  }

  InplaceBuffer & get_value() {
    return key.next();
  }

  KVBuffer & next() {
    InplaceBuffer & value = get_value();
    return *(KVBuffer*) (value.content + value.length);
  }

  uint32_t memory() {
    return key.memory() + get_value().memory();
  }

  std::string str() {
    return get_key().str() + "\t" + get_value().str();
  }

  void copy_to(char * dest) {
    memcpy(dest, this, memory());
  }
};


} // namespace Hadoop

#endif /* IOBUFFER_H_ */
