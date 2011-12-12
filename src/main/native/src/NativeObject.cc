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


#include "commons.h"
#include "NativeObject.h"

using std::string;

namespace Hadoop {

//////////////////////////////////////////////////////////////////
// NativeObjectType methods
//////////////////////////////////////////////////////////////////

const string NativeObjectTypeToString(NativeObjectType type) {
  switch (type) {
  case BatchHandlerType:
    return string("BatchHandlerType");
  case MapperType:
    return string("MapperType");
  case ReducerType:
    return string("ReducerType");
  case PartitionerType:
    return string("PartitionerType");
  case CombinerType:
    return string("CombinerType");
  case RecordReaderType:
    return string("RecordReaderType");
  case RecordWriterType:
    return string("RecordWriterType");
  default:
    return string("UnknownObjectType");
  }
}

NativeObjectType NativeObjectTypeFromString(const string type) {
  if (type == "BatchHandlerType") {
    return BatchHandlerType;
  } else if (type == "MapperType") {
    return MapperType;
  } else if (type == "ReducerType") {
    return ReducerType;
  } else if (type == "PartitionerType") {
    return PartitionerType;
  } else if (type == "CombinerType") {
    return CombinerType;
  } else if (type == "RecordReaderType") {
    return RecordReaderType;
  } else if (type == "RecordWriterType") {
    return RecordWriterType;
  }
  return UnknownObjectType;
}

} //namespace Hadoop



