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
#include "Streams.h"

namespace Hadoop {

int32_t InputStream::readFully(void * buff, uint32_t length) {
  int32_t ret = 0;
  while (length>0) {
    int32_t rd = read(buff, length);
    if (rd < 0) {
      return ret > 0 ? ret : -1;
    }
    ret += rd;
    buff = ((char *)buff) + rd;
    length -= rd;
  }
}


} // namespace Hadoop
