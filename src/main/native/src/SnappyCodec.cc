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

#include "SnappyCodec.h"

namespace Hadoop {

SnappyCompressStream::SnappyCompressStream(
    OutputStream * stream,
    uint32_t bufferSizeHint) :
    CompressStream(stream) {

}

SnappyCompressStream::~SnappyCompressStream() {

}

void SnappyCompressStream::write(const void * buff, uint32_t length) {
}

void SnappyCompressStream::flush() {

}

void SnappyCompressStream::close() {

}

void SnappyCompressStream::writeDirect(const void * buff, uint32_t length) {

}

//////////////////////////////////////////////////////////////

SnappyDecompressStream::SnappyDecompressStream(
    InputStream * stream,
    uint32_t bufferSizeHint) :
    DecompressStream(stream) {

}

SnappyDecompressStream::~SnappyDecompressStream() {

}

int32_t SnappyDecompressStream::read(void * buff, uint32_t length) {
  return 0;
}

void SnappyDecompressStream::close() {

}

int32_t SnappyDecompressStream::readDirect(void * buff, uint32_t length) {
  return 0;
}

} // namespace Hadoop

