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
#include "util/StringUtil.h"
#include "Buffers.h"
#include "FileSystem.h"
#include "Compressions.h"
#include "LineRecordReader.h"
#include "FileSplit.h"

namespace NativeTask {

inline char * memchr(char * p, char ch, size_t len) {
  size_t i = 0;
  for (;i+4<len;i+=4) {
    if (p[i]==ch) {
      return p+i;
    }
    if (p[i+1]==ch) {
      return p+i+1;
    }
    if (p[i+2]==ch) {
      return p+i+2;
    }
    if (p[i+3]==ch) {
      return p+i+3;
    }
  }
  for (;i<len;i++) {
    if (p[i]==ch) {
      return p+i;
    }
  }
  return NULL;
}

LineRecordReader::LineRecordReader() :
  _orig(NULL),
  _source(NULL),
  _start(0),
  _pos(0),
  _end(0),
  _bufferHint(128*1024),
  _hasOrig(true),
  _inputLength(0) {
}

LineRecordReader::~LineRecordReader() {
  close();
}

/**
 * For performance reasons,
 * Only consider \n or \r\n as new line terminator
 * memchr in some os/platform is not optimized,
 * add memchr_sse4.2 primitive
 */
uint32_t LineRecordReader::ReadLine(InputStream * source, DynamicBuffer & buffer,
                                    Buffer & line, uint32_t bufferHint,
                                    bool withEOL) {
  if (buffer.remain() > 0) {
    char * pos = (char*)memchr(buffer.current(), '\n', buffer.remain());
    if (pos>0) {
      uint32_t length = pos - buffer.current();
      int32_t hasBR = ((length>0) && (*(pos-1) == '\r')) ? 1 : 0;
      line.reset(buffer.current(), length + (withEOL?1:-hasBR));
      buffer.use(length + 1);
      return length + 1;
    }
  }
  buffer.cleanUsed();
  uint32_t findStart = 0;
  while (true) {
    if (buffer.freeSpace() < bufferHint) {
      buffer.reserve(buffer.capacity()*2);
    }
    int32_t rd = buffer.refill(source);
    if (rd<=0) {
      // reach EOF
      uint32_t ret = buffer.size();
      line.reset(buffer.data(), ret);
      buffer.use(ret);
      return ret;
    }
    char * pos = (char*)memchr(buffer.data()+findStart, '\n', buffer.size()-findStart);
    if (pos>0) {
      uint32_t length = pos - buffer.data();
      int32_t hasBR = ((length>0) && (*(pos-1) == '\r')) ? 1 : 0;
      line.reset(buffer.data(), length + (withEOL?1:-hasBR));
      buffer.use(length + 1);
      return length + 1;
    }
    findStart = buffer.size();
  }
  return 0;
}

void LineRecordReader::init(InputStream * stream, const string & codec) {
  close();
  _bufferHint = 128*1024;
  _orig = stream;
  _hasOrig = false;
  _start = 0;
  _pos = _start;
  _end = (uint64_t)-1;
  _inputLength = (uint64_t)-1;
  if (codec.length() > 0) {
    // more buffer to prevent decompression stream using extra temp buffer
    _buffer.reserve(_bufferHint*2);
    _source = Compressions::getDecompressionStream(codec, _orig, _bufferHint);
  } else {
    _buffer.reserve(_bufferHint);
    _source = _orig;
    if (_start != 0) {
      --_start;
      _pos = _start;
      _orig->seek(_start);
      Buffer line;
      _start += readLine(line);
      _pos = _start;
    }
  }
}

void LineRecordReader::init(const string & file, uint64_t start,
                            uint64_t length, Config & config) {
  close();
  _bufferHint = 128*1024;
  _orig = FileSystem::get(config).open(file);
  _hasOrig = true;
  _start = start;
  _pos = _start;
  _end = start + length;
  _inputLength = length;
  const string & codec = Compressions::getCodecByFile(file);
  if (codec != "") {
    // more buffer to prevent decompression stream using extra cache buffer
    _buffer.reserve(_bufferHint*2);
    _source = Compressions::getDecompressionStream(codec, _orig, _bufferHint);
    // dcompression stream don't have effective _pos & _end
    _end = (uint64_t)-1;
  } else {
    _buffer.reserve(_bufferHint);
    _source = _orig;
    if (_start != 0) {
      --_start;
      _pos = _start;
      _orig->seek(_start);
      Buffer line;
      _start += readLine(line);
      _pos = _start;
    }
  }
}

void LineRecordReader::configure(Config & config) {
  string splitData = config.get("native.input.split", "");
  if (splitData == "") {
    THROW_EXCEPTION(IOException, "Input split info not found in config");
  }
  FileSplit split;
  split.readFields(splitData);
  LOG("Input FileSplit: %s", split.toString().c_str());
  init(split.file(), split.start(), split.length(), config);
}

bool LineRecordReader::next(Buffer & key, Buffer & value) {
  uint32_t len = readLine(value);
  if (len == 0) {
    return false;
  }
  _pos += len;
  return true;
}

float LineRecordReader::getProgress() {
  if (_source == _orig) {
    // direct stream
    if (_end > _start) {
      float ret = (_pos - _start)/(double)(_end - _start);
      return std::min(ret, 1.0f);
    }
  } else {
    // compressed stream
    uint64_t compressedRead =
        ((DecompressStream*) _source)->compressedBytesRead();
    float ret =  compressedRead / (double) _inputLength;
    return std::min(ret, 1.0f);
  }
  return 0.0f;
}

void LineRecordReader::close() {
  if (_source != _orig) {
    delete _source;
  }
  _source = NULL;
  if (_hasOrig) {
    delete _orig;
  }
  _orig = NULL;
}

void KeyValueLineRecordReader::configure(Config & config) {
  LineRecordReader::configure(config);
  string sep = config.get("key.value.separator.in.input.line", "\t");
  if (sep.length() > 0) {
    _kvSeparator = sep[0];
  }
  else {
    _kvSeparator = '\t';
  }
}

bool KeyValueLineRecordReader::next(Buffer & key, Buffer & value) {
  Buffer line;
  uint32_t len = readLine(line);
  if (len == 0) {
    return false;
  }
  char * pos = (char*)memchr(line.data(), _kvSeparator, line.length());
  if (pos != NULL) {
    uint32_t keyLen = pos - line.data();
    key.reset(line.data(), keyLen);
    value.reset(pos+1, line.length() - keyLen - 1);
  } else {
    key.reset(line.data(), line.length());
    value.reset(NULL, 0);
  }
  return true;
}


} // namespace NativeTask
