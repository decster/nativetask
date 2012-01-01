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


#include "FileSystem.h"
#include "test_commons.h"

TEST(FileSystem, RawFileSystem) {
  FileSystem & fs = FileSystem::getRaw();
  fs.mkdirs("temp");
  string temppath = "temp/data";
  string content;
  GenerateKVTextLength(content, 4111111, "word");
  FileOutputStream * output = (FileOutputStream*)fs.create(temppath, true);
  output->write(content.data(), content.length());
  output->close();
  delete output;
  FileInputStream * input = (FileInputStream*)fs.open(temppath);
  char buff[1024];
  int64_t total = 0;
  while(true) {
    int rd = input->read(buff, 1024);
    if (rd<=0) {
      break;
    }
    ASSERT_EQ(content.substr(total, rd), string(buff,rd));
    total+=rd;
  }
  ASSERT_EQ(content.length(), total);
  delete input;
  ASSERT_EQ(fs.getLength(temppath), content.length());
  ASSERT_TRUE(fs.exists(temppath));
  fs.remove("temp");
  ASSERT_FALSE(fs.exists(temppath));
}


TEST(FileSystem, JavaFileSystem) {
  FileSystem & fs = FileSystem::getJava(TestConfig);
  fs.mkdirs("temp");
  string temppath = "temp/data";
  string content;
  GenerateKVTextLength(content, 4111111, "word");
  FileOutputStream * output = (FileOutputStream*)fs.create(temppath, true);
  output->write(content.data(), content.length());
  output->close();
  delete output;
  FileInputStream * input = (FileInputStream*)fs.open(temppath);
  char buff[102400];
  int64_t total = 0;
  while(true) {
    int rd = input->read(buff, 102400);
    if (rd<=0) {
      break;
    }
    ASSERT_EQ(content.substr(total, rd), string(buff,rd));
    total+=rd;
  }
  ASSERT_EQ(content.length(), total);
  delete input;
  ASSERT_EQ(fs.getLength(temppath), content.length());
  ASSERT_TRUE(fs.exists(temppath));
  fs.remove("temp");
  ASSERT_FALSE(fs.exists(temppath));
}

TEST(Perf, FileSystem) {
  int64_t inputlength = TestConfig.getInt("input.length", 32*1024*1024);
  string temppath = "temp";
  string content;
  GenerateKVTextLength(content, inputlength, "word");
  Timer timer;
  {
    timer.reset();
    FileSystem & fs = FileSystem::getRaw();
    fs.mkdirs(temppath);
    LOG("%s", timer.getInterval("init & mkdir").c_str());
    timer.reset();
    FileOutputStream * output = (FileOutputStream*)fs.create(temppath+"/data", true);
    LOG("%s", timer.getInterval("create outputstream").c_str());
    timer.reset();
    output->write(content.data(), content.length());
    output->close();
    LOG("%s", timer.getInterval("write").c_str());
    delete output;
    timer.reset();
    FileInputStream * input = (FileInputStream*)fs.open(temppath+"/data");
    LOG("%s", timer.getInterval("create input stream").c_str());
    timer.reset();
    char buff[102400];
    int64_t total = 0;
    while(true) {
      int rd = input->read(buff, 102400);
      if (rd<=0) {
        break;
      }
      total+=rd;
    }
    LOG("%s", timer.getInterval("read").c_str());
    delete input;
  }
  {
    timer.reset();
    FileSystem & fs = FileSystem::getJava(TestConfig);
    fs.mkdirs(temppath);
    LOG("%s", timer.getInterval("init & mkdir").c_str());
    timer.reset();
    FileOutputStream * output = (FileOutputStream*)fs.create(temppath+"/data", true);
    LOG("%s", timer.getInterval("create outputstream").c_str());
    timer.reset();
    output->write(content.data(), content.length());
    output->close();
    LOG("%s", timer.getInterval("write").c_str());
    delete output;
    timer.reset();
    FileInputStream * input = (FileInputStream*)fs.open(temppath+"/data");
    LOG("%s", timer.getInterval("create input stream").c_str());
    timer.reset();
    char buff[102400];
    int64_t total = 0;
    while(true) {
      int rd = input->read(buff, 102400);
      if (rd<=0) {
        break;
      }
      total+=rd;
    }
    LOG("%s", timer.getInterval("read").c_str());
    delete input;
  }
}

