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
#include "EchoBatchHandler.h"
#include "MCollectorOutputHandler.h"
#include "NativeObjectFactory.h"
#include "NativeTask.h"

namespace Hadoop {

HadoopException::HadoopException(const char * what) {
  if (what[0]=='/') {
    const char * e = strchr(what, ':');
    if (e!=NULL) {
      what++;
      while (true) {
        const char * n = strchr(what, '/');
        if (n==NULL || n>=e) {
          break;
        }
        what = n+1;
      }
    }
  }
  _reason.assign(what);
  void *array[64];
  size_t size;
  size = backtrace(array, 64);
  char ** traces = backtrace_symbols(array, size);
  for (size_t i = 0; i< size;i++) {
    _reason.append("\n\t");
    _reason.append(traces[i]);
  }
}


void Config::load(const char * filepath) {
  FILE * fin = fopen(filepath, "r");
  char buff[256];
  while (fgets(buff,256,fin)!=NULL) {
    if (buff[0]=='#') {
      continue;
    }
    std::string key = buff;
    if (key[key.length()-1] == '\n') {
      size_t br = key.find('=');
      if (br!=key.npos) {
        set(key.substr(0,br), Trim(key.substr(br+1)));
      }
    }
  }
}

uint32_t Config::get_uint32(const char * key, int default_value) {
  if (_dict.find(key) !=  _dict.end()) {
    std::string v = _dict[key];
    return (uint32_t)strtoul(v.c_str(), NULL, 10);
  }
  return default_value;
}

int Config::get_type_enum(const char * key, int default_value) {
  if (_dict.find(key) !=  _dict.end()) {
    std::string v = _dict[key];
    return NameToEnum(v);
  }
  return default_value;
}

const char * Config::get(const char * key, const char * default_value) {
  if (_dict.find(key) !=  _dict.end()) {
    std::string v = _dict[key];
    return v.c_str();
  }
  return default_value;
}

void Config::set(std::string key, std::string value) {
  _dict[key] = value;
}

void Config::set_uint32(const char * key, uint32_t value) {
  char buff[32];
  snprintf(buff,32,"%u", value);
  _dict[key] = std::string(buff);
}

Counter * ProcessorBase::getCounter(const string & group, const string & name) {
  return NULL;
}


uint32_t Partitioner::getPartition(const char * key, uint32_t & keyLen, uint32_t numPartition) {
  if (numPartition==1) {
    return 0;
  }
  return (HashBytes(key, keyLen) & 0x7fffffff) % numPartition;
}

}
