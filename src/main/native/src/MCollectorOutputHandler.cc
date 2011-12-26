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

#include "commons.h"
#include "MCollectorOutputHandler.h"
#include "NativeObjectFactory.h"
#include "MapOutputCollector.h"

namespace Hadoop {

using std::string;
using std::vector;

MCollectorOutputHandler::MCollectorOutputHandler() :
    _collector(NULL),
    _dest(NULL),
    _remain(0) {
}

MCollectorOutputHandler::~MCollectorOutputHandler() {
  reset();
}

void MCollectorOutputHandler::reset() {
  _dest = NULL;
  _remain = 0;
  delete _collector;
  _collector = NULL;
}

void MCollectorOutputHandler::configure(Config & config) {
  uint32_t partition = config.getInt("mapred.reduce.tasks", 1);
  _collector = new MapOutputCollector(partition);
  _collector->configure(config);
}

void MCollectorOutputHandler::finish() {
  string outputpath = this->sendCommand("GetOutputPath");
  if (hasJavaException()) {
    return;
  }
  string indexpath = this->sendCommand("GetOutputIndexPath");
  if (hasJavaException()) {
    return;
  }
  if ((outputpath.length() == 0) || (indexpath.length() == 0)) {
    THROW_EXCEPTION(IOException, "Illegal(empty) map output file/index path");
  }
  vector<string> pathes;
  StringUtil::Split(outputpath, ";", pathes);
  _collector->final_merge_and_spill(pathes, indexpath, _collector->getMapOutputSpec(), NULL);
  reset();
  BatchHandler::finish();
}

void MCollectorOutputHandler::handleInput(char * buff, uint32_t length) {
  if (_remain > 0) {
    uint32_t cp = _remain < length ? _remain : length;
    memcpy(_dest, buff, cp);
    length -= cp;
    buff += cp;
    _dest += cp;
    _remain -= cp;
  }
  while (length>0) {
    if (unlikely(length<2*sizeof(uint32_t))) {
      THROW_EXCEPTION(IOException, "k/v meta information incomplete");
    }
    uint32_t partition = ((uint32_t*)buff)[0];
    uint32_t kvlength = ((uint32_t*)buff)[1];
    buff += 2*sizeof(uint32_t);
    length -= 2*sizeof(uint32_t);
    char * dest = _collector->get_buffer_to_put(kvlength, partition);
    if (NULL == dest) {
      string spillpath = this->sendCommand("GetSpillPath");
      if (hasJavaException()) {
        return;
      }
      if (spillpath.length() == 0) {
        THROW_EXCEPTION(IOException, "Illegal(empty) spill files path");
      }
      vector<string> pathes;
      StringUtil::Split(spillpath, ";", pathes);
      _collector->mid_spill(pathes, "", _collector->getMapOutputSpec(), NULL);
      dest = _collector->get_buffer_to_put(kvlength, partition);
      if (NULL == dest) {
        // io.sort.mb too small, cann't proceed
        // should not get here, cause get_buffer_to_put can throw OOM exception
        THROW_EXCEPTION(OutOfMemoryException, "key/value pair larger than io.sort.mb");
      }
    }
    if (kvlength <= length) {
      simple_memcpy(dest, buff, kvlength);
      buff += kvlength;
      length -= kvlength;
    }
    else {
      if (length>0) {
        memcpy(dest, buff, length);
        _dest = dest + length;
        _remain = kvlength - length;
      }
      else {
        _dest = dest;
        _remain = kvlength;
      }
      break;
    }
  }
}

}
