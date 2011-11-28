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

#ifndef ECHOBATCHHANDLER_H_
#define ECHOBATCHHANDLER_H_

#include "BatchHandler.h"

namespace Hadoop {
class MapOutputCollector;

/**
 * A simple batch handler just pass everything from input
 * to output.
 */
class EchoBatchHandler:
    public Hadoop::BatchHandler {
public:
  EchoBatchHandler();
  virtual ~EchoBatchHandler();

  virtual void setup();
  virtual void finish();
  virtual void handleInput(char * buff, uint32_t length);
  virtual std::string command(const std::string & cmd);
};

}

#endif /* ECHOBATCHHANDLER_H_ */
