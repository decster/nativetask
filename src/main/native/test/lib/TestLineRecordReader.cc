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
#include "lib/LineRecordReader.h"
#include "test_commons.h"

TEST(LineRecordReader, Read) {
  int32_t size = TestConfig.getInt("input.size", 100000);
  string type = TestConfig.get("input.type", "bytes");
  vector<string> inputs;
  Generate(inputs, size, type);
  string input;
  for (int32_t i=0;i<size;i++) {
    input.append(inputs[i]);
    if (i%2==0) {
      input.append(1, '\n');
    } else {
      input.append("\r\n");
    }
  }
  InputBuffer inputBuffer = InputBuffer(input);
  LineRecordReader reader = LineRecordReader();
  reader.init(&inputBuffer, "");
  Buffer key;
  Buffer value;
  for (int32_t i=0;i<size;i++) {
    ASSERT_TRUE(reader.next(key, value));
    ASSERT_EQ(0, key.length());
    ASSERT_EQ(inputs[i], value.toString());
  }
  ASSERT_FALSE(reader.next(key,value));
}

void TestLineRecordReaderLen(uint32_t recordLength) {
  int32_t size = TestConfig.getInt("input.size", 100000);
  string type = TestConfig.get("input.type", "bytes");
  int64_t origLength = TestConfig.getInt(GenerateLen, -1);
  TestConfig.setInt(GenerateLen, recordLength);
  vector<string> inputs;
  Generate(inputs, size, type);
  string input;
  for (int32_t i=0;i<size;i++) {
    input.append(inputs[i]);
    if (i%2==0) {
      input.append(1, '\n');
    } else {
      input.append("\r\n");
    }
  }
  InputBuffer inputBuffer = InputBuffer(input);
  LineRecordReader reader = LineRecordReader();
  reader.init(&inputBuffer, "");
  Buffer key;
  Buffer value;
  Timer timer;
  while (reader.next(key, value));
  LOG("Record Length: %u %s", recordLength, timer.getSpeedM2("read speed byte/record", input.length(), size).c_str());
  TestConfig.setInt(GenerateLen, origLength);
}

TEST(Perf, LineRecordReader) {
  TestLineRecordReaderLen(2);
  TestLineRecordReaderLen(13);
  TestLineRecordReaderLen(55);
  TestLineRecordReaderLen(100);
  TestLineRecordReaderLen(200);
  TestLineRecordReaderLen(400);
}
