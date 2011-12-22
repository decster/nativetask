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

#include "SyncUtils.h"
#include "test_commons.h"

class TestThread : public Thread {
  virtual void run() {
    for (int i=0;i<5;i++) {
      usleep(100);
      LOG("sleep %d", i*100);
    }
  }
};

TEST(SyncUtil, Thread) {
  TestThread a,b,c;
  a.start();
  b.start();
  c.start();
  a.join();
  b.join();
  c.join();
}

class TestR {
public:
  void foo() {
    for (int i=0;i<2;i++) {
      usleep(100);
      LOG("usleep %d", i*100);
    }
  }
  void bar(const char * msg) {
    for (int i=0;i<2;i++) {
      usleep(100);
      LOG("usleep %d %s", i*100, msg);
    }
  }
};

TEST(SyncUtil, ThreadBind) {
  TestR a = TestR();
  Thread t = Thread(Bind(a, &TestR::foo));
  Thread t2 = Thread(Bind(a, &TestR::bar, "testmsg"));
  t.start();
  t2.start();
  t.join();
  t2.join();
}

TEST(Perf, ThreadOverhead) {
  int64_t threadnum = TestConfig.getInt("thread.num", 1000);
  Thread * t = new Thread[threadnum];
  Timer timer;
  for (int i=0;i<threadnum;i++) {
    t[i].start();
  }
  for (int i=0;i<threadnum;i++) {
    t[i].join();
  }
  LOG("%lld thread %s", threadnum, timer.getInterval("start&join").c_str());
  delete [] t;
}
