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

/**
 * High performance primitive functions
 *
 **/

#ifndef PRIMITIVES_H_
#define PRIMITIVES_H_

#include <stddef.h>
#include <stdint.h>
#include <assert.h>
#include <string>

#ifdef __GNUC__
#define likely(x)       __builtin_expect((x),1)
#define unlikely(x)     __builtin_expect((x),0)
#else
#define likely(x)       (x)
#define unlikely(x)     (x)
#endif


#define STRINGIFY(x) #x
#define TOSTRING(x) STRINGIFY(x)
#define AT __FILE__ ":" TOSTRING(__LINE__)

// #define THROW_EXCEPTION(type, what) throw type(AT":"what)

#define THROW_EXCEPTION(type, what) throw type((std::string(AT":") + what))

#define THROW_EXCEPTION_EX(type, fmt, args...) throw type(StringUtil::Format("%s:" fmt, AT, ##args))

#if !defined(USE_CHECKSUM_CRC32C) && !defined(USE_CHECKSUM_CRC32C_HARDWARE)
#define USE_CHECKSUM_CRC32
#endif

//#define MEMCMP_SSE42
//#define SIMPLE_MEMCPY


#if !defined(SIMPLE_MEMCPY)
#define simple_memcpy memcpy
#define simple_memcpy2 memcpy
#else

/**
 * This memcpy assumes src & dest are not overlapped,
 * and dest buffer have enough free space (8 bytes) for
 * writing dirty data, by doing this, many branches
 * are eliminated.
 * This function is primarily optimized for x86-64 processors,
 * on which unaligned 64-bit loads and stores are cheap
 *
 *
 * @param dest: dest buffer
 * @param src:  src buffer
 * @param size: src buffer size, must be >0
 */
inline void simple_memcpy(void * dest, const void * src, size_t size) {
  register const uint64_t* psrc = (const uint64_t*)src;
  register uint64_t* pdest = (uint64_t*)dest;
  for(;;) {
    *pdest++ = *psrc++;
    if (size<=sizeof(uint64_t))
      return;
    size -= sizeof(uint64_t);
  }
}

inline void simple_memcpy2(void * dest, const void * src, size_t size) {
  if (unlikely(size > 128)) {
    memcpy(dest, src, size);
  } else {
    simple_memcpy(dest, src, size);
  }
}
#endif


/**
 * little-endian to big-endian or vice versa
 */
inline uint32_t bswap(uint32_t val)
{
  __asm__("bswap %0" : "=r" (val) : "0" (val));
  return val;
}

inline uint64_t bswap64(uint64_t val)
{
  __asm__("bswapq %0" : "=r" (val) : "0" (val));
  return val;
}

/**
 * fast memcmp
 */
inline int fmemcmp(const char * src, const char * dest, uint32_t len) {
  const uint64_t * src8 = (const uint64_t*)src;
  const uint64_t * dest8 = (const uint64_t*)dest;
  while (len>=8) {
    uint64_t l = *src8;
    uint64_t r = *dest8;
    if (l != r) {
      l = bswap64(l);
      r = bswap64(r);
      return l > r ? 1 : -1;
    }
    ++src8;
    ++dest8;
    len -= 8;
  }
  if (len==0)
    return 0;
  if (len == 1) {
    int l = (int)(*(uint8_t*)src8);
    int r = (int)(*(uint8_t*)dest8);
    return l - r;
  }
  uint64_t mask = (1ULL << (len * 8)) - 1;
  uint64_t l = (*src8) & mask;
  uint64_t r = (*dest8) & mask;
  if (l == r) {
    return 0;
  }
  l = bswap64(l);
  r = bswap64(r);
  return l > r ? 1 : -1;
}

inline int fmemcmp2(const char * src, uint32_t srcLen, const char * dest,
    uint32_t destLen) {
  uint32_t minlen = srcLen<destLen?srcLen:destLen;
  int ret = fmemcmp(src, dest, minlen);
  if (ret) {
    return ret;
  }
  return srcLen - destLen;
}

inline bool fmemeq(const char * src, const char * dest, uint32_t len) {
  const uint64_t * src8 = (const uint64_t*)src;
  const uint64_t * dest8 = (const uint64_t*)dest;
  while (len>=8) {
    uint64_t l = *src8;
    uint64_t r = *dest8;
    if (l != r) {
      return false;
    }
    ++src8;
    ++dest8;
    len -= 8;
  }
  switch (len) {
  case 0:
    return true;
  case 1:
    return *(uint8_t*) src8 == *(uint8_t*) dest8;
  case 2:
    return *(uint16_t*) src8 == *(uint16_t*) dest8;
  case 3:
    return (*(uint32_t*) src8 & 0x00ffffff) ==
           (*(uint32_t*) dest8 & 0x00ffffff);
  case 4:
    return ((*(uint32_t*) src8) == (*(uint32_t*) dest8));
  case 5:
    return (*src8  & 0xffffffffffUL) ==
           (*dest8 & 0xffffffffffUL);
  case 6:
    return (*src8  & 0xffffffffffffUL) ==
           (*dest8 & 0xffffffffffffUL);
  case 7:
    return (*src8  & 0xffffffffffffUL) ==
           (*dest8 & 0xffffffffffffUL);
  }
  return true;
}

#endif /* PRIMITIVES_H_ */
