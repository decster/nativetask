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

#include <dlfcn.h>

#include "commons.h"
#include "NativeObject.h"
#include "NativeObjectFactory.h"
#include "NativeLibrary.h"

namespace Hadoop {

//////////////////////////////////////////////////////////////////
// NativeLibrary methods
//////////////////////////////////////////////////////////////////

NativeLibrary::NativeLibrary(const string path, const string name)
    :_path(path), _name(name), _create_object_func(NULL) {

}

bool NativeLibrary::init() {
  void *library = dlopen(_path.c_str(), RTLD_LAZY | RTLD_GLOBAL);
  if (NULL==library) {
    LOG("Load object library %s failed.", _path.c_str());
    return false;
  }
  // clean error status
  dlerror();
  LOG("Load object library %s(%s):", _name.c_str(), _path.c_str())

  string create_object_func_name = _name + "CreateObject";
  _create_object_func = (CreateObjectFunc)dlsym(library, create_object_func_name.c_str());
  if (NULL==_create_object_func) {
    LOG("Do not have object factory: %s", create_object_func_name.c_str());
  }

  string init_library_func_name = _name + "Init";
  InitLibraryFunc init_library_func = (InitLibraryFunc)dlsym(library, init_library_func_name.c_str());
  if (NULL==init_library_func) {
    LOG("Do not have function of %s", init_library_func_name.c_str());
  }
  else {
    if(0 != init_library_func()) {
      LOG("init(%s) failed", init_library_func_name.c_str());
      return false;
    }
    else {
      LOG("init(%s) succeed", init_library_func_name.c_str());
    }
  }
  return true;
}

NativeObject * NativeLibrary::createObject(const string clz) {
  if (NULL == _create_object_func) {
    return NULL;
  }
  // TODO: use RTTI dynamic_cast<NativeObject*>
  return (NativeObject*)(_create_object_func(clz.c_str()));
}

}

