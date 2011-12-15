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

#ifndef FILESPLIT_H_
#define FILESPLIT_H_

#include "NativeTask.h"
#include "BufferStream.h"
#include "WritableUtils.h"

namespace Hadoop {

class FileSplit : public InputSplit {
protected:
  string _file;
  uint64_t _start;
  uint64_t _length;
  vector<string> _locations;
public:
  FileSplit() :
    _start(0),
    _length(0) {
  }

  FileSplit(const string & file, uint64_t start, uint64_t length) :
    _file(file),
    _start(start),
    _length(length) {
  }

  virtual ~FileSplit() {}

  const string & file() {
    return _file;
  }

  uint64_t start() {
    return _start;
  }

  uint64_t length() {
    return _length;
  }

  virtual uint64_t getLength() {
    return _length;
  }

  virtual vector<string> & getLocations() {
    return _locations;
  }

  virtual void readFields(const string & data);

  virtual void writeFields(string & dest);
};

} // namespace Hadoop

#endif /* FILESPLIT_H_ */
