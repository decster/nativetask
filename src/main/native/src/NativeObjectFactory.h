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

#ifndef NATIVEOBJECTFACTORY_H_
#define NATIVEOBJECTFACTORY_H_

#include <string>
#include <vector>
#include <set>
#include <map>

#include "NativeTask.h"

namespace Hadoop {

using std::string;
using std::vector;
using std::map;
using std::set;
using std::pair;

class NativeLibrary;
class Config;
class Counter;

/**
 * Native object factory
 */
class NativeObjectFactory {
private:
  static vector<NativeLibrary *> Libraries;
  static map<NativeObjectType, string> DefaultClasses;
  static Config * GlobalConfig;
  static set<Counter *> CounterSet;
  static vector<Counter *> Counters;
  static vector<uint64_t> CounterLastUpdateValues;
  static bool Inited;
public:
  static bool Init();
  static void Release();
  static void CheckInit();
  static Config & GetConfig();
  static Config * GetConfigPtr();
  static Counter * GetCounter(const string & group, const string & name);
  static const vector<Counter *> GetAllCounters();
  static void RegisterClass(const string & clz, ObjectCreatorFunc func);
  static NativeObject * CreateObject(const string & clz);
  static ObjectCreatorFunc GetObjectCreator(const string & clz);
  static void ReleaseObject(NativeObject * obj);
  static bool RegisterLibrary(const string & path, const string & name);
  static void SetDefaultClass(NativeObjectType type, const string & clz);
  static NativeObject * CreateDefaultObject(NativeObjectType type);
};

}

#endif /* NATIVEOBJECTFACTORY_H_ */
