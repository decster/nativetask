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

#include "NativeObjectFactory.h"
#include "NativeTask.h"
#include "MapOutputCollector.h"
#include "FileSystem.h"
#include "lib/TeraSort.h"
#include "test_commons.h"

class MapTaskRunner : public Collector {
public:
MapOutputCollector * collector;
uint32_t numPartition;
uint32_t numSpill;
Partitioner * partitioner;

virtual void collect(const void * key, uint32_t keyLen,
                     const void * value, uint32_t valueLen) {
  uint32_t p = partitioner->getPartition((const char*)key, keyLen, numPartition);
  if (0 != collector->put(key, keyLen, value, valueLen, p)) {
    vector<string> spillpath;
    spillpath.push_back(StringUtil::Format("spill%u", numSpill++));
    collector->mid_spill(spillpath, "", collector->getMapOutputSpec());
  }
}

};

TEST(Perf, MapTask) {
  Timer timer;
  string inputfile = TestConfig.get("input.file", "terainput");
  uint32_t numPartition = TestConfig.getInt("mapred.reduce.tasks", 100);
  string outputfile = TestConfig.get("output.file", "map.output");
  bool sort = TestConfig.getBool("mapred.map.output.sort", true);
  bool compressMapOutput = TestConfig.getBool("mapred.compress.map.output", true);
  Config & config = NativeObjectFactory::GetConfig();
  config.set("mapred.output.key.class", "org.apache.hadoop.io.Text");
  config.set("mapred.output.value.class", "org.apache.hadoop.io.Text");
  config.setBool("mapred.map.output.sort", sort);
  config.setBool("mapred.compress.map.output", compressMapOutput);
  config.set("mapred.map.output.compression.codec", Compressions::SnappyCodec.name);
  config.set("io.sort.mb", "300");
  TeraRecordReader reader = TeraRecordReader();
  Partitioner partitioner = Partitioner();
  MapOutputCollector moc = MapOutputCollector(100);
  moc.configure(config);
  LOG("%s", timer.getInterval("prepare time").c_str());
  timer.reset();
  MapTaskRunner runner = MapTaskRunner();
  runner.collector = &moc;
  runner.numPartition = numPartition;
  runner.numSpill = 0;
  runner.partitioner = &partitioner;
  Mapper mapper = Mapper();
  mapper.setCollector(&runner);
  reader.init(inputfile, 0, FileSystem::getRaw().getLength(inputfile), config);
  Buffer key, value;
  while (reader.next(key, value)) {
    mapper.map(key.data(), key.length(), value.data(), value.length());
  }
  vector<string> outputpath;
  outputpath.push_back(outputfile);
  moc.final_merge_and_spill(outputpath, "map.output.index", moc.getMapOutputSpec());
  LOG("%s", timer.getInterval("MapTask").c_str());
}


