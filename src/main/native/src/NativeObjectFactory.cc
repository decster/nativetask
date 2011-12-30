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

#include <signal.h>
#include "commons.h"
#include "NativeTask.h"
#include "NativeObjectFactory.h"
#include "NativeLibrary.h"
#include "BatchHandler.h"
#include "EchoBatchHandler.h"
#include "MCollectorOutputHandler.h"
#include "MMapperHandler.h"
#include "MMapTaskHandler.h"
#include "RReducerHandler.h"
#include "lib/LineRecordReader.h"
#include "lib/LineRecordWriter.h"
#include "lib/TeraSort.h"
#include "lib/WordCount.h"

using namespace Hadoop;

// TODO: just for debug, should be removed
extern "C" void handler(int sig) {
  void *array[10];
  size_t size;

  // get void*'s for all entries on the stack
  size = backtrace(array, 10);

  // print out all the frames to stderr
  fprintf(stderr, "Error: signal %d:\n", sig);
  backtrace_symbols_fd(array, size, 2);
  exit(1);
}

DEFINE_NATIVE_LIBRARY(NativeTask) {
  //signal(SIGSEGV, handler);
  REGISTER_CLASS(BatchHandler, NativeTask);
  REGISTER_CLASS(EchoBatchHandler, NativeTask);
  REGISTER_CLASS(MCollectorOutputHandler, NativeTask);
  REGISTER_CLASS(MMapperHandler, NativeTask);
  REGISTER_CLASS(MMapTaskHandler, NativeTask);
  REGISTER_CLASS(RReducerHandler, NativeTask);
  REGISTER_CLASS(Mapper, NativeTask);
  REGISTER_CLASS(Reducer, NativeTask);
  REGISTER_CLASS(Partitioner, NativeTask);
  REGISTER_CLASS(Folder, NativeTask);
  REGISTER_CLASS(LineRecordReader, NativeTask);
  REGISTER_CLASS(KeyValueLineRecordReader, NativeTask);
  REGISTER_CLASS(LineRecordWriter, NativeTask);
  REGISTER_CLASS(TeraPartitioner, NativeTask);
  REGISTER_CLASS(TeraRecordReader, NativeTask);
  REGISTER_CLASS(TeraRecordWriter, NativeTask);
  REGISTER_CLASS(WordCountMapper, NativeTask);
  REGISTER_CLASS(WordCountReducer, NativeTask);
  REGISTER_CLASS(WordCountRMapper, NativeTask);
  REGISTER_CLASS(WordCountRecordWriter, NativeTask);
  NativeObjectFactory::SetDefaultClass(BatchHandlerType, "NativeTask.BatchHandler");
  NativeObjectFactory::SetDefaultClass(MapperType, "NativeTask.Mapper");
  NativeObjectFactory::SetDefaultClass(ReducerType, "NativeTask.Reducer");
  NativeObjectFactory::SetDefaultClass(PartitionerType, "NativeTask.Partitioner");
  NativeObjectFactory::SetDefaultClass(FolderType, "NativeTask.Folder");
  return 0;
}

namespace Hadoop {

static Config G_CONFIG;

vector<NativeLibrary *> NativeObjectFactory::Libraries;
map<NativeObjectType, string> NativeObjectFactory::DefaultClasses;
Config * NativeObjectFactory::GlobalConfig = &G_CONFIG;
set<Counter *> NativeObjectFactory::CounterSet;
vector<Counter *> NativeObjectFactory::Counters;
vector<uint64_t> NativeObjectFactory::CounterLastUpdateValues;
bool NativeObjectFactory::Inited = false;

bool NativeObjectFactory::Init() {
  Inited = true;
  // setup log device
  string device = GetConfig().get("native.log.device", "stderr");
  if (device == "stdout") {
    LOG_DEVICE = stdout;
  } else if (device == "stderr") {
    LOG_DEVICE = stderr;
  } else {
    LOG_DEVICE = fopen(device.c_str(), "w");
  }
  if (0 != NativeTaskInit()) {
    LOG("NativeTaskInit() failed");
    Inited = false;
    return false;
  }
  NativeLibrary * library = new NativeLibrary("nativetask.so", "NativeTask");
  library->_getObjectCreatorFunc = NativeTaskGetObjectCreator;
  LOG("NativeTask library initialized");
  Libraries.push_back(library);
  return true;
}

void NativeObjectFactory::Release() {
  for (ssize_t i = Libraries.size() - 1; i >= 0; i--) {
    delete Libraries[i];
    Libraries[i] = NULL;
  }
  Libraries.clear();
  for (size_t i = 0; i < Counters.size(); i++) {
    delete Counters[i];
  }
  Counters.clear();
  if (LOG_DEVICE != stdout &&
      LOG_DEVICE != stderr) {
    fclose(LOG_DEVICE);
    LOG_DEVICE = stderr;
  }
  Inited = false;
}

void NativeObjectFactory::CheckInit() {
  if (Inited == false) {
    if (!Init()) {
      throw new IOException("Init NativeTask library failed.");
    }
  }
}

Config & NativeObjectFactory::GetConfig() {
  return *GlobalConfig;
}

Config * NativeObjectFactory::GetConfigPtr() {
  return GlobalConfig;
}

Counter * NativeObjectFactory::GetCounter(const string & group, const string & name) {
  Counter tmpCounter(group, name);
  set<Counter *>::iterator itr =
      CounterSet.find(&tmpCounter);
  if (itr != CounterSet.end()) {
    return *itr;
  }
  Counter * ret = new Counter(group, name);
  Counters.push_back(ret);
  CounterLastUpdateValues.push_back(0);
  CounterSet.insert(ret);
  return ret;
}

const vector<Counter *> NativeObjectFactory::GetAllCounters() {
  return Counters;
}

void NativeObjectFactory::RegisterClass(const string & clz, ObjectCreatorFunc func) {
  NativeTaskClassMap__[clz] = func;
}

NativeObject * NativeObjectFactory::CreateObject(const string & clz) {
  ObjectCreatorFunc creator = GetObjectCreator(clz);
  return creator ? creator() : NULL;
}

ObjectCreatorFunc NativeObjectFactory::GetObjectCreator(const string & clz) {
  CheckInit();
  for (vector<NativeLibrary*>::reverse_iterator ritr = Libraries.rbegin();
      ritr != Libraries.rend(); ritr++) {
    ObjectCreatorFunc ret = (*ritr)->getObjectCreator(clz);
    if (NULL!=ret) {
      return ret;
    }
  }
  return NULL;
}

void NativeObjectFactory::ReleaseObject(NativeObject * obj) {
  delete obj;
}

bool NativeObjectFactory::RegisterLibrary(const string & path, const string & name) {
  CheckInit();
  NativeLibrary * library = new NativeLibrary(path, name);
  bool ret = library->init();
  if (!ret) {
    delete library;
    return false;
  }
  Libraries.push_back(library);
  return true;
}

void NativeObjectFactory::SetDefaultClass(NativeObjectType type, const string & clz) {
  DefaultClasses[type] = clz;
}

NativeObject * NativeObjectFactory::CreateDefaultObject(NativeObjectType type) {
  CheckInit();
  if (DefaultClasses.find(type) != DefaultClasses.end()) {
    string clz = DefaultClasses[type];
    return CreateObject(clz);
  }
  LOG("Default class for NativeObjectType %s not found",
      NativeObjectTypeToString(type).c_str());
  return NULL;
}

}

