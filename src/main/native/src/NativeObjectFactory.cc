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
#include "RReducerHandler.h"

using namespace Hadoop;

extern "C" {

static void handler(int sig);

// TODO: just for debug, should be removed
void handler(int sig) {
  void *array[10];
  size_t size;

  // get void*'s for all entries on the stack
  size = backtrace(array, 10);

  // print out all the frames to stderr
  fprintf(stderr, "Error: signal %d:\n", sig);
  backtrace_symbols_fd(array, size, 2);
  exit(1);
}

/**
 * CreateObjectFunc for this object library, NativeTask
 */
void * NativeTaskCreateObject(const char * name) {
  NativeObject * ret = NULL;
  std::string clazz(name);
  if (clazz=="BatchHandler") {
    // dummy handler, do nothing
    ret = new BatchHandler();
  } else if (clazz=="EchoBatchHandler") {
    ret = new EchoBatchHandler();
  } else if (clazz=="MCollectorOutputHandler") {
    ret = new MCollectorOutputHandler();
  } else if (clazz=="MMapperHandler") {
    ret = new MMapperHandler();
  } else if (clazz=="RReducerHandler") {
    ret = new RReducerHandler();
  }else if (clazz=="Mapper") {
    ret = new Mapper();
  } else if (clazz=="Reducer") {
    ret = new Reducer();
  } else if (clazz=="Partitioner") {
    ret = new Partitioner();
  } else if (clazz=="Folder")  {
    ret = new Folder();
  }
  return ret;
}

/**
 * Init function for this library, NativeTask
 */
int NativeTaskInit() {
  signal(SIGSEGV, handler);
  NativeObjectFactory::SetDefaultClass(BatchHandlerType, "BatchHandler");
  NativeObjectFactory::SetDefaultClass(MapperType, "Mapper");
  NativeObjectFactory::SetDefaultClass(ReducerType, "Reducer");
  NativeObjectFactory::SetDefaultClass(PartitionerType, "Partitioner");
  NativeObjectFactory::SetDefaultClass(FolderType, "Folder");
  return 0;
}

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
  library->_create_object_func = NativeTaskCreateObject;
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

NativeObject * NativeObjectFactory::CreateObject(const string & clz) {
  CheckInit();
  // Iterate libraries from last registered NativeLibrary to NativeTask library
  for (vector<NativeLibrary*>::reverse_iterator ritr = Libraries.rbegin();
      ritr != Libraries.rend(); ritr++) {
    NativeObject * ret = (*ritr)->createObject(clz);
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

