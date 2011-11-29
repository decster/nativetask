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

#include <snappy-c.h>
#include "commons.h"
#include "NativeTask.h"
#include "SnappyCodec.h"

namespace Hadoop {

SnappyCompressStream::SnappyCompressStream(
    OutputStream * stream,
    uint32_t bufferSizeHint) :
    CompressStream(stream) {
  _hint = bufferSizeHint;
  _blockMax = bufferSizeHint / 2 * 3;
  _tempBufferSize = snappy_max_compressed_length(_blockMax)+8;
  _tempBuffer = new char[_tempBufferSize];
}

SnappyCompressStream::~SnappyCompressStream() {
  delete [] _tempBuffer;
  _tempBuffer = NULL;
  _tempBufferSize = 0;
}

void SnappyCompressStream::write(const void * buff, uint32_t length) {
  while (length > 0) {
    uint32_t take = length < _blockMax ? length : _hint;
    compressOneBlock(buff, take);
    buff = ((const char *)buff) + take;
    length -= take;
  }
}

void SnappyCompressStream::flush() {
  _stream->flush();
}

void SnappyCompressStream::close() {
  flush();
}

void SnappyCompressStream::writeDirect(const void * buff, uint32_t length) {
  _stream->write(buff, length);
}

void SnappyCompressStream::compressOneBlock(const void * buff, uint32_t length) {
  size_t compressedLength = _tempBufferSize - 8;
  snappy_status ret = snappy_compress((const char*) buff, length,
                                      _tempBuffer + 8, &compressedLength);
  if (ret == SNAPPY_OK) {
    ((uint32_t*) _tempBuffer)[0] = bswap(length);
    ((uint32_t*) _tempBuffer)[1] = bswap((uint32_t) compressedLength);
    _stream->write(_tempBuffer, compressedLength + 8);
  } else if (ret == SNAPPY_INVALID_INPUT) {
    THROW_EXCEPTION(IOException, "compress SNAPPY_INVALID_INPUT");
  } else if (ret == SNAPPY_BUFFER_TOO_SMALL) {
    THROW_EXCEPTION(IOException, "compress SNAPPY_BUFFER_TOO_SMALL");
  } else {
    THROW_EXCEPTION(IOException, "compress snappy failed");
  }
}

//////////////////////////////////////////////////////////////

SnappyDecompressStream::SnappyDecompressStream(
    InputStream * stream,
    uint32_t bufferSizeHint) :
    DecompressStream(stream) {
  _hint = bufferSizeHint;
  _blockMax = bufferSizeHint / 2 * 3;
  _tempBufferSize = snappy_max_compressed_length(_blockMax) + 8;
  _tempBuffer = (char*)malloc(_tempBufferSize);
  _tempDecompressBuffer = NULL;
  _tempDecompressBufferSize = 0;
  _tempDecompressBufferUsed = 0;
  _tempDecompressBufferCapacity = 0;
}

SnappyDecompressStream::~SnappyDecompressStream() {
  close();
  if (NULL != _tempBuffer) {
    free(_tempBuffer);
    _tempBuffer = NULL;
  }
  _tempBufferSize = 0;
}

uint32_t SnappyDecompressStream::decompressOneBlock(uint32_t compressedSize,
                                                    void * buff,
                                                    uint32_t length) {
  if (compressedSize > _tempBufferSize) {
    char * newBuffer = (char *)realloc(_tempBuffer, compressedSize);
    if (newBuffer == NULL) {
      THROW_EXCEPTION(OutOfMemoryException, "realloc failed");
    }
    _tempBuffer = newBuffer;
    _tempBufferSize = compressedSize;
  }
  int32_t rd = _stream->readFully(_tempBuffer, compressedSize);
  if (rd != compressedSize) {
    THROW_EXCEPTION(IOException, "readFully reach EOF");
  }
  size_t uncompressedLength = length;
  snappy_status ret = snappy_uncompress(_tempBuffer, compressedSize,
                                        (char *) buff, &uncompressedLength);
  if (ret == SNAPPY_OK) {
    return uncompressedLength;
  } else if (ret == SNAPPY_INVALID_INPUT) {
    THROW_EXCEPTION(IOException, "decompress SNAPPY_INVALID_INPUT");
  } else if (ret == SNAPPY_BUFFER_TOO_SMALL) {
    THROW_EXCEPTION(IOException, "decompress SNAPPY_BUFFER_TOO_SMALL");
  } else {
    THROW_EXCEPTION(IOException, "decompress snappy failed");
  }
}

int32_t SnappyDecompressStream::read(void * buff, uint32_t length) {
  if (_tempDecompressBufferSize == 0) {
    uint32_t sizes[2];
    int32_t rd = _stream->readFully(&sizes, sizeof(uint32_t)*2);
    if (rd < 0) {
      // EOF
      return -1;
    }
    if (rd != sizeof(uint32_t)*2) {
      THROW_EXCEPTION(IOException, "readFully get incomplete data");
    }
    sizes[0] = bswap(sizes[0]);
    sizes[1] = bswap(sizes[1]);
    if (sizes[0] <= length) {
      uint32_t len = decompressOneBlock(sizes[1], buff, sizes[0]);
      if (len != sizes[0]) {
        THROW_EXCEPTION(IOException, "snappy decompress data error, length not match");
      }
      return len;
    } else {
      if (sizes[0] > _tempDecompressBufferCapacity) {
        char * newBuffer = (char *)realloc(_tempDecompressBuffer, sizes[0]);
        if (newBuffer == NULL) {
          THROW_EXCEPTION(OutOfMemoryException, "realloc failed");
        }
        _tempDecompressBuffer = newBuffer;
        _tempDecompressBufferCapacity = sizes[0];
      }
      uint32_t len = decompressOneBlock(sizes[1], _tempDecompressBuffer, sizes[0]);
      if (len != sizes[0]) {
        THROW_EXCEPTION(IOException, "snappy decompress data error, length not match");
      }
      _tempDecompressBufferSize = sizes[0];
      _tempDecompressBufferUsed = 0;
    }
  }
  if (_tempDecompressBufferSize > 0) {
    uint32_t left = _tempDecompressBufferSize-_tempDecompressBufferUsed;
    if (length < left) {
      memcpy(buff, _tempDecompressBuffer + _tempDecompressBufferUsed, length);
      return length;
    } else {
      memcpy(buff, _tempDecompressBuffer + _tempDecompressBufferUsed, left);
      _tempDecompressBufferSize = 0;
      _tempDecompressBufferUsed = 0;
      return left;
    }
  }
  // should not get here
  THROW_EXCEPTION(IOException, "Decompress logic error");
  return -1;
}

void SnappyDecompressStream::close() {
  if (_tempDecompressBufferSize > 0) {
    LOG("Some data left in the _tempDecompressBuffer when close()");
  }
  if (NULL != _tempDecompressBuffer) {
    free(_tempDecompressBuffer);
    _tempDecompressBuffer = NULL;
    _tempDecompressBufferCapacity = 0;
  }
  _tempDecompressBufferSize = 0;
  _tempDecompressBufferUsed = 0;
}

int32_t SnappyDecompressStream::readDirect(void * buff, uint32_t length) {
  if (_tempDecompressBufferSize > 0) {
    THROW_EXCEPTION(IOException, "temp decompress data exists when call readDirect()");
  }
  return _stream->readFully(buff, length);
}

} // namespace Hadoop

