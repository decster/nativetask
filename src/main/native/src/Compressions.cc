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

#include "commons.h"
#include "Compressions.h"
#include "GzipCodec.h"
#include "SnappyCodec.h"

namespace Hadoop {

CompressStream::~CompressStream() {
}

void CompressStream::writeDirect(const void * buff, uint32_t length) {
  THROW_EXCEPTION(UnsupportException, "writeDirect not support");
}

///////////////////////////////////////////////////////////

DecompressStream::~DecompressStream() {
}

int32_t DecompressStream::readDirect(void * buff, uint32_t length) {
  THROW_EXCEPTION(UnsupportException, "readDirect not support");
}

///////////////////////////////////////////////////////////

static const char * SUPPORTED_CODECS[][2] = {
    {"org.apache.hadoop.io.compress.GzipCodec", ".gzip"},
    {"org.apache.hadoop.io.compress.SnappyCodec", ".snappy"},
    NULL
};

bool Compressions::support(const string & codec) {
  for (int i=0;SUPPORTED_CODECS[i]!=NULL;i++) {
    if (codec == SUPPORTED_CODECS[i][0]) {
      return true;
    }
  }
  return false;
}

const string Compressions::getExtension(const string & codec) {
  for (int i=0;SUPPORTED_CODECS[i]!=NULL;i++) {
    if (codec == SUPPORTED_CODECS[i][0]) {
      return SUPPORTED_CODECS[i][1];
    }
  }
  return string();
}

const string Compressions::getCodec(const string & extension) {
  for (int i=0;SUPPORTED_CODECS[i]!=NULL;i++) {
    if (extension == SUPPORTED_CODECS[i][1]) {
      return SUPPORTED_CODECS[i][0];
    }
  }
  return string();
}

CompressStream * Compressions::getCompressionStream(
    const string & codec,
    OutputStream * stream,
    uint32_t bufferSizeHint) {
  if (codec == SUPPORTED_CODECS[0][0]) {
    return new GzipCompressStream(stream, bufferSizeHint);
  }
  if (codec == SUPPORTED_CODECS[1][0]) {
    return new SnappyCompressStream(stream, bufferSizeHint);
  }
  return NULL;
}

DecompressStream * Compressions::getDecompressionStream(
    const string & codec,
    InputStream * stream,
    uint32_t bufferSizeHint) {
  if (codec == SUPPORTED_CODECS[0][0]) {
    return new GzipDecompressStream(stream, bufferSizeHint);
  }
  if (codec == SUPPORTED_CODECS[1][0]) {
    return new SnappyDecompressStream(stream, bufferSizeHint);
  }
  return NULL;
}

} // namespace Hadoop

