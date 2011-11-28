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

#ifndef COMPRESSIONS_H_
#define COMPRESSIONS_H_

#include <dlfcn.h>

#include "snappy.h"
#include "commons.h"

namespace Hadoop {

// TODO: dynamic lazy load support
//inline void load_snappy() {
//  void *libsnappy = dlopen("libsnappy.so", RTLD_LAZY | RTLD_GLOBAL);
//  dlerror();                                 // Clear any existing error
//  void dlsym_snappy_compress = dlsym(libsnappy, "snappy_compress");
//}

// TODO: use custom snappy::Sink to write to file directly

/**
 * Snappy compression
 */
// MaxCompressedLength = 32 + source_len + source_len/6;
// int compressionOverhead = (bufferSize / 6) + 32;
// src buff 128K -> dest buff 128K + 22K  = 150K
inline size_t CompressionBufferSize(CompressionType type, size_t src_len) {
  if (type==SNAPPY) {
    return snappy::MaxCompressedLength(src_len);
  }
  else {
    THROW_EXCEPTION(UnsupportException, "compression type not supported");
  }
}

inline void CompressBlock(
    CompressionType type,
    const char* input,
    size_t input_length,
    char* compressed,
    size_t* compressed_length) {
  if (type==SNAPPY) {
    snappy::RawCompress(input, input_length, compressed, compressed_length);
  }
  else {
    THROW_EXCEPTION(UnsupportException, "compression type not supported");
  }
}

} // namespace Hadoop


#endif /* COMPRESSIONS_H_ */
