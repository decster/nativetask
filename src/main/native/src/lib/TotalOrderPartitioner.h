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

#ifndef TOTALORDERPARTITIONER_H_
#define TOTALORDERPARTITIONER_H_

#include "NativeTask.h"

namespace Hadoop {

class TotalOrderPartitioner : public Partitioner {
protected:
  static const char * PARTITION_FILE_NAME;
  string _trie;
  vector<string> _partitions;

public:
  virtual void configure(Config & config);

  virtual uint32_t getPartition(const char * key, uint32_t & keyLen,
                                uint32_t numPartition);

  static uint32_t SearchTrie(const vector<string> & partitions, string & trie);
  static void MakeTrie(const vector<string> & partitions, string & trie, uint32_t maxLevel);
};

} // namespace Hadoop


#endif /* TOTALORDERPARTITIONER_H_ */
