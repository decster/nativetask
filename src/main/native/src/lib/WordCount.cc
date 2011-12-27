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

#ifndef WORDCOUNT_CC_
#define WORDCOUNT_CC_

#include "commons.h"
#include "WordCount.h"

namespace Hadoop {

WordCountMapper::WordCountMapper() {
  memset(_spaces, 0, 256);
  _spaces[' '] = 1;
  _spaces['\r'] = 1;
  _spaces['\n'] = 1;
  _spaces['\t'] = 1;
  _spaces['\f'] = 1;
}

void WordCountMapper::wordCount(const char * buff, uint32_t length) {
  uint32_t count = 1;
  uint8_t * pos = (uint8_t*)buff;
  uint8_t * end = (uint8_t*)buff + length;
  uint8_t * start = NULL;
  uint32_t len = 0;
  while (pos != end) {
    if (start) {
      if (_spaces[*pos]) {
        collect(start, len, &count, 4);
        start = NULL;
      } else {
        len++;
      }
    } else {
      if (_spaces[*pos]==0) {
        start = pos;
        len = 1;
      }
    }
    pos++;
  }
  if (start) {
    collect(start, len, &count, 4);
  }
}

void WordCountMapper::map(const char * key, uint32_t keyLen,
                          const char * value, uint32_t valueLen) {
  if (keyLen>0) {
    wordCount(key, keyLen);
  }
  if (valueLen>0) {
    wordCount(value, valueLen);
  }
}

void WordCountReducer::reduce(KeyGroup & input) {
  const char * key;
  const char * value;
  uint32_t keyLen;
  uint32_t valueLen;
  uint32_t count = 0;
  key = input.getKey(keyLen);
  while (NULL != (value=input.nextValue(valueLen))) {
    count += *(uint32_t*)value;
  }
  collect(key, keyLen, &count, 4);
}

void WordCountRMapper::map(const char * key, uint32_t keyLen,
                           const char * value, uint32_t valueLen) {
  if (_count>0) {
    if (frmemeq(_key.data(), key, _key.length(), keyLen)) {
      collect(_key.data(), _key.length(), &_count, 4);
      _key.assign(key, keyLen);
      _count = 1;
    } else {
      _count++;
    }
  } else {
    _key.assign(key, keyLen);
    _count=1;
  }
}

void WordCountRMapper::close() {
  if (_count>0) {
    collect(_key.data(), _key.length(), &_count, 4);
  }
}

void WordCountRecordWriter::write(const Buffer & key, const Buffer & value) {
  _appendBuffer.write(key.data(), key.length());
  _appendBuffer.write(_keyValueSeparator.data(), _keyValueSeparator.length());
  string s = StringUtil::ToString(*(uint32_t*)value.data());
  _appendBuffer.write(s.data(), s.length());
  _appendBuffer.write('\n');
}

} // namespace Hadoop

#endif /* WORDCOUNT_CC_ */
