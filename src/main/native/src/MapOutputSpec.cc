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
#include "MapOutputSpec.h"

namespace Hadoop {

void MapOutputSpec::getSpecFromConfig(Config & config, MapOutputSpec & spec) {
  spec.checksumType = CHECKSUM_CRC32;
  spec.sortType = CPPSORT;
  if (config.get("mapred.compress.map.output","false")=="true") {
    spec.codec = config.get("mapred.map.output.compression.codec");
  } else {
    spec.codec = "";
  }
  if (config.getBool("mapred.map.output.sort", false)) {
    spec.orderType = FULLSORT;
  } else {
    spec.orderType = NOSORT;
  }
  const char * key_class = config.get("mapred.mapoutput.key.class");
  if (NULL == key_class) {
    key_class = config.get("mapred.output.key.class");
  }
  if (NULL == key_class) {
    THROW_EXCEPTION(IOException, "mapred.mapoutput.key.class not set");
  }
  spec.keyType = JavaClassToKeyValueType(key_class);
  const char * value_class = config.get("mapred.mapoutput.value.class");
  if (NULL == value_class) {
    value_class = config.get("mapred.output.value.class");
  }
  if (NULL == value_class) {
    THROW_EXCEPTION(IOException, "mapred.mapoutput.value.class not set");
  }
  spec.valueType = JavaClassToKeyValueType(value_class);
}


} // namespace Hadoop
