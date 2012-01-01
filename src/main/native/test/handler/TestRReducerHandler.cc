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
#include "handler/RReducerHandler.h"
#include "test_commons.h"

namespace NativeTask {

class RReducerHandlerTester : public RReducerHandler {
  static const uint32_t BUFFSIZE = 16*1024;
  string _inputData;
  string _outputData;
  uint32_t _inputDataUsed;
  uint32_t _inputKeyGroup;
public:
  void initBuffers() {
    _ib.reset(new char[BUFFSIZE], BUFFSIZE);
    _ob.reset(new char[BUFFSIZE], BUFFSIZE-8);
  }
  void makeInputData(int cnt) {
    _inputData = "";
    _inputData.reserve(1024*1024);
    _outputData.reserve(1024*1024);
    char buff[128];
    for (int i=0;i<cnt;i++) {
      // make every 10 value a group
      snprintf(buff,128,"%010d", i);
      uint32_t len = strlen(buff)-1;
      _inputData.append((const char*)(&len), 4);
      len+=1;
      _inputData.append((const char*)(&len), 4);
      _inputData.append(buff,len-1);
      _inputData.append(buff,len);
    }
    _inputDataUsed = 0;
    _inputKeyGroup = (cnt+9)/10;
  }

  virtual void flushOutput(uint32_t length) {
    _outputData.append(_ob.buff, length);
  }

  virtual void finish() {
    flush();
  }

  virtual int32_t refill() {
    uint32_t rest = _inputData.length() - _inputDataUsed;
    uint32_t cp = 13500 < rest ? 13500 : rest;
    if (cp>0) {
      memcpy(_ib.buff, _inputData.c_str()+_inputDataUsed, cp);
    }
    _inputDataUsed += cp;
    _ib.position = cp;
    _current = _ib.buff;
    _remain = cp;
    return cp;
  }

  void varifyData() {
    ASSERT_EQ(_inputData.length(), _outputData.length());
    //ASSERT_EQ(_inputData, _outputData);
  }

  void prepare() {
    initBuffers();
    makeInputData(10000);
  }
};

}

TEST(RReducerHandler, ReducerOnly) {
  Config jobconf;
  RReducerHandlerTester t = RReducerHandlerTester();
  t.configure(jobconf);
  t.prepare();
  t.command("run");
  t.varifyData();
}

