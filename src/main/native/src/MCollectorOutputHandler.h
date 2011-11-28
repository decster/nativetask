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

#ifndef MCOLLECTOROUTPUTHANDLER_H_
#define MCOLLECTOROUTPUTHANDLER_H_

#include "BatchHandler.h"

namespace Hadoop {
class MapOutputCollector;

class MCollectorOutputHandler:
    public Hadoop::BatchHandler {
private:
  MapOutputCollector * _collector;
  // state info for large KV pairs
  char * _dest;
  uint32_t _remain;
public:
  MCollectorOutputHandler();
  virtual ~MCollectorOutputHandler();

  virtual void setup();
  virtual void finish();
  virtual void handleInput(char * buff, uint32_t length);

private:
  void reset();
};

}

#endif /* MCOLLECTOROUTPUTHANDLER_H_ */
