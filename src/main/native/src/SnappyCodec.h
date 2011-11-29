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

#ifndef SNAPPYCODEC_H_
#define SNAPPYCODEC_H_

#include "Compressions.h"

namespace Hadoop {

class SnappyCompressStream : public CompressStream {
protected:
  uint32_t _hint;
  uint32_t _blockMax;
  char *   _tempBuffer;
  uint32_t _tempBufferSize;
public:
  SnappyCompressStream(OutputStream * stream,
                       uint32_t bufferSizeHint);

  virtual ~SnappyCompressStream();

  virtual void write(const void * buff, uint32_t length);

  virtual void flush();

  virtual void close();

  virtual void writeDirect(const void * buff, uint32_t length);

protected:
  void compressOneBlock(const void * buff, uint32_t length);
};

class SnappyDecompressStream : public DecompressStream {
protected:
  uint32_t _hint;
  uint32_t _blockMax;
  char *   _tempBuffer;
  uint32_t _tempBufferSize;
  char *   _tempDecompressBuffer;
  uint32_t _tempDecompressBufferSize;
  uint32_t _tempDecompressBufferUsed;
  uint32_t _tempDecompressBufferCapacity;
public:
  SnappyDecompressStream(InputStream * stream,
                         uint32_t bufferSizeHint);

  virtual ~SnappyDecompressStream();

  virtual int32_t read(void * buff, uint32_t length);

  virtual void close();

  virtual int32_t readDirect(void * buff, uint32_t length);

protected:
  uint32_t decompressOneBlock(uint32_t compressedSize,
                              void * buff,
                              uint32_t length);
};

} // namespace Hadoop

#endif /* SNAPPYCODEC_H_ */
