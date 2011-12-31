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
#include "MMapTaskHandler.h"
#include "FileSystem.h"
#include "lib/FileSplit.h"
#include "Compressions.h"
#include "test_commons.h"

namespace NativeTask {

class MMapTaskHandlerTest : public MMapTaskHandler {
protected:
  string outputfile;
  int spillCount;
public:
  MMapTaskHandlerTest() : spillCount(0) {}

  virtual ~MMapTaskHandlerTest() {}

  virtual string sendCommand(const string & cmd) {
    if (cmd == "GetSpillPath") {
      return StringUtil::Format("spill_%d", spillCount++);
    } else if (cmd == "GetOutputPath") {
      return outputfile.length()>0?outputfile:string("map.output");
    } else if (cmd == "GetOutputIndexPath") {
      return string("map.output.index");
    } else {
      THROW_EXCEPTION_EX(UnsupportException, "Unknown command %s", cmd.c_str());
    }
  }

  void setOutputFile(const string & outputfile) {
    this->outputfile = outputfile;
  }
};

TEST(Perf, MMapTaskTeraSort) {
  string inputfile = TestConfig.get("maptask.inputfile", "tera.input");
  string outputfile = TestConfig.get("maptask.outputfile", "map.output");
  string inputcodec = Compressions::getCodecByFile(inputfile);
  string sorttype = TestConfig.get("native.sort.type", "DUALPIVOTSORT");
  bool sortFirst = TestConfig.getBool("native.spill.sort.first", true);
  string inputtype = TestConfig.get("maptask.inputtype", "tera");
  int64_t inputLength = TestConfig.getInt("maptask.inputlength", 250000000);
  int64_t iosortmb = TestConfig.getInt("io.sort.mb", 300);
  bool isTeraInput = inputtype == "tera";
  uint64_t inputFileLength = 0;
  Timer timer;
  if (!FileSystem::getRaw().exists(inputfile)) {
    LOG("start generating input");
    vector<pair<string, string> > inputdata;
    GenerateLength(inputdata, inputLength, inputtype);
    OutputStream * fout = FileSystem::getRaw().create(inputfile, true);
    AppendBuffer appendBuffer = AppendBuffer();
    appendBuffer.init(128 * 1024, fout, inputcodec);
    for (size_t i=0;i<inputdata.size();i++) {
      string & key = inputdata[i].first;
      string & value = inputdata[i].second;
      appendBuffer.write(key.data(), key.length());
      if (!isTeraInput)
        appendBuffer.write('\t');
      appendBuffer.write(value.data(), value.length());
      appendBuffer.write('\n');
    }
    appendBuffer.flush();
    inputFileLength = fout->tell();
    delete fout;
    LOG("%s", timer.getInterval("Generate input").c_str());
  } else {
    inputFileLength = FileSystem::getRaw().getLength(inputfile);
  }

  timer.reset();
  Config jobconf;
  jobconf.setInt("mapred.reduce.tasks", 200);
  jobconf.setInt("io.sort.mb", iosortmb);
  jobconf.set("mapred.mapoutput.key.class", "org.apache.hadoop.io.Text");
  jobconf.set("mapred.mapoutput.value.class", "org.apache.hadoop.io.Text");
  jobconf.setBool("native.spill.sort.first", sortFirst);
  jobconf.set("native.sort.type", sorttype);
  jobconf.set("mapred.compress.map.output", "true");
  jobconf.set("mapred.map.output.compression.codec", Compressions::Lz4Codec.name);
  FileSplit split = FileSplit(inputfile, 0, inputFileLength);
  string splitData;
  split.writeFields(splitData);
  jobconf.set("native.input.split", splitData);
  if (isTeraInput) {
    jobconf.set("native.recordreader.class", "NativeTask.TeraRecordReader");
  } else if (inputtype == "word") {
    jobconf.set("native.recordreader.class", "NativeTask.LineRecordReader");
    jobconf.set("native.mapper.class", "NativeTask.WordCountMapper");
  } else {
    jobconf.set("native.recordreader.class", "NativeTask.KeyValueLineRecordReader");
  }
  MMapTaskHandlerTest * mapRunner = new MMapTaskHandlerTest();
  mapRunner->setOutputFile(outputfile);
  mapRunner->configure(jobconf);
  mapRunner->command("run");
  LOG("%s", timer.getInterval("Map Task").c_str());
}



TEST(Perf, MMapTaskWordCount) {
  string inputfile = TestConfig.get("maptask.inputfile", "word.input");
  string outputfile = TestConfig.get("maptask.outputfile", "map.output");
  string inputcodec = Compressions::getCodecByFile(inputfile);
  string sorttype = TestConfig.get("native.sort.type", "DUALPIVOTSORT");
  bool sortFirst = TestConfig.getBool("native.spill.sort.first", true);
  bool usecombiner = TestConfig.getBool("maptask.enable.combiner",true);
  string inputtype = TestConfig.get("maptask.inputtype", "word");
  int64_t inputLength = TestConfig.getInt("maptask.inputlength", 250000000);
  int64_t iosortmb = TestConfig.getInt("io.sort.mb", 300);
  bool isTeraInput = inputtype == "tera";
  uint64_t inputFileLength = 0;
  Timer timer;
  if (!FileSystem::getRaw().exists(inputfile)) {
    LOG("start generating input");
    vector<pair<string, string> > inputdata;
    GenerateLength(inputdata, inputLength, inputtype);
    OutputStream * fout = FileSystem::getRaw().create(inputfile, true);
    AppendBuffer appendBuffer = AppendBuffer();
    appendBuffer.init(128 * 1024, fout, inputcodec);
    for (size_t i=0;i<inputdata.size();i++) {
      string & key = inputdata[i].first;
      string & value = inputdata[i].second;
      appendBuffer.write(key.data(), key.length());
      if (!isTeraInput)
        appendBuffer.write('\t');
      appendBuffer.write(value.data(), value.length());
      appendBuffer.write('\n');
    }
    appendBuffer.flush();
    inputFileLength = fout->tell();
    delete fout;
    LOG("%s", timer.getInterval("Generate input").c_str());
  } else {
    inputFileLength = FileSystem::getRaw().getLength(inputfile);
  }

  timer.reset();
  Config jobconf;
  jobconf.setInt("mapred.reduce.tasks", 100);
  jobconf.setInt("io.sort.mb", iosortmb);
  jobconf.set("mapred.mapoutput.key.class", "org.apache.hadoop.io.Text");
  jobconf.set("mapred.mapoutput.value.class", "org.apache.hadoop.io.Text");
  jobconf.setBool("native.spill.sort.first", sortFirst);
  jobconf.set("native.sort.type", sorttype);
  jobconf.set("mapred.compress.map.output", "true");
  jobconf.set("mapred.map.output.compression.codec", Compressions::Lz4Codec.name);
  FileSplit split = FileSplit(inputfile, 0, inputFileLength);
  string splitData;
  split.writeFields(splitData);
  jobconf.set("native.input.split", splitData);
  if (isTeraInput) {
    jobconf.set("native.recordreader.class", "NativeTask.TeraRecordReader");
  } else if (inputtype == "word") {
    jobconf.set("native.recordreader.class", "NativeTask.LineRecordReader");
    jobconf.set("native.mapper.class", "NativeTask.WordCountMapper");
    jobconf.set("native.combiner.class", "NativeTask.WordCountReducer");
  } else {
    jobconf.set("native.recordreader.class", "NativeTask.KeyValueLineRecordReader");
  }
  MMapTaskHandlerTest * mapRunner = new MMapTaskHandlerTest();
  mapRunner->setOutputFile(outputfile);
  mapRunner->configure(jobconf);
  mapRunner->command("run");
  LOG("%s", timer.getInterval("Map Task").c_str());
}

} // namespace Hadoop
