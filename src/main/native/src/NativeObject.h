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

#ifndef NATIVEOBJECT_H_
#define NATIVEOBJECT_H_

#include <string>

namespace Hadoop {

/**
 * NativeObjectType
 */
enum NativeObjectType {
  UnknownObjectType = 0,
  BatchHandlerType = 1,
  MapperType = 2,
  ReducerType = 3,
  PartitionerType = 4,
  CombinerType = 5,
  FolderType = 6,
  RecordReaderType = 7,
  RecordWriterType = 8
};

extern const std::string NativeObjectTypeToString(NativeObjectType type);
extern NativeObjectType NativeObjectTypeFromString(const std::string type);

/**
 * Objects that can be loaded dynamically from shared library,
 * and managed by NativeObjectFactory
 */
class NativeObject {
public:
  virtual NativeObjectType type() {
    return UnknownObjectType;
  }

  virtual ~NativeObject() {};
};

template<typename T>
NativeObject * ObjectCreator() {
  return new T();
}

typedef NativeObject * (*ObjectCreatorFunc)();

#define DEFINE_NATIVE_LIBRARY(Library) \
  static std::map<std::string, ObjectCreatorFunc> Library##ClassMap__; \
  extern "C" void * Library##CreateObject(const char * name) { \
    NativeObject * ret = NULL; \
    map<string, ObjectCreatorFunc>::iterator itr = Library##ClassMap__.find(name); \
    if (itr != Library##ClassMap__.end()) { \
      return itr->second(); \
    } \
    return NULL; \
  } \
  extern "C" int Library##Init()

#define REGISTER_CLASS(Type, Library) Library##ClassMap__[#Library"."#Type] = ObjectCreator<Type>

} // namespace Hadoop



#endif // NATIVEOBJECT_H_
