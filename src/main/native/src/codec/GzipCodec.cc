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

#include <zconf.h>
#include <zlib.h>
#include "GzipCodec.h"

namespace Hadoop {

GzipCompressStream::GzipCompressStream(
    OutputStream * stream,
    uint32_t bufferSizeHint) :
    CompressStream(stream) {

}

GzipCompressStream::~GzipCompressStream() {

}

void GzipCompressStream::write(const void * buff, uint32_t length) {
}

void GzipCompressStream::flush() {

}

void GzipCompressStream::close() {

}

void GzipCompressStream::writeDirect(const void * buff, uint32_t length) {

}

//////////////////////////////////////////////////////////////

GzipDecompressStream::GzipDecompressStream(
    InputStream * stream,
    uint32_t bufferSizeHint) :
    DecompressStream(stream) {

}

GzipDecompressStream::~GzipDecompressStream() {

}

int32_t GzipDecompressStream::read(void * buff, uint32_t length) {
  return 0;
}

void GzipDecompressStream::close() {

}

int32_t GzipDecompressStream::readDirect(void * buff, uint32_t length) {
  return 0;
}

} // namespace Hadoop

