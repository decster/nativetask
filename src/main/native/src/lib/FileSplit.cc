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
#include "NativeObjectFactory.h"
#include "FileSplit.h"
#include "BufferStream.h"
#include "WritableUtils.h"

namespace NativeTask {

void FileSplit::readFields(const string & data) {
  InputBuffer input = InputBuffer(data);
  if (NativeObjectFactory::GetConfig().getInt("native.hadoop.version", 1) == 1) {
    _file = WritableUtils::ReadUTF8(&input);
  }
  else {
    _file = WritableUtils::ReadString(&input);
  }
  _start = WritableUtils::ReadLong(&input);
  _length = WritableUtils::ReadLong(&input);
}

void FileSplit::writeFields(string & dest) {
  OutputStringStream out = OutputStringStream(dest);
  if (NativeObjectFactory::GetConfig().getInt("native.hadoop.version", 1) == 1) {
    WritableUtils::WriteUTF8(&out, _file);
  }
  else {
    WritableUtils::WriteString(&out, _file);
  }
  WritableUtils::WriteLong(&out, _start);
  WritableUtils::WriteLong(&out, _length);
}

string FileSplit::toString() {
  return StringUtil::Format("%s:%llu+%llu", _file.c_str(), _start, _length);
}

} // namespace NativeTask

