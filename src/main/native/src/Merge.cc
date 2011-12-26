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

#include "Timer.h"
#include "Merge.h"

namespace Hadoop {

Merger::~Merger() {
  _heap.clear();
  for (size_t i = 0 ; i < _entries.size() ; i++) {
    delete _entries[i];
  }
  _entries.clear();
}

void Merger::addMergeEntry(MergeEntryPtr pme) {
  _entries.push_back(pme);
}

/**
 * 0 if success, have next partition
 * 1 if failed, no more
 */
int Merger::startPartition() {
  int ret = -1;
  for (size_t i = 0 ; i < _entries.size() ; i++) {
    int r = _entries[i]->nextPartition();
    if (ret == -1) {
      ret = r;
    } else if (r != ret) {
      THROW_EXCEPTION(IOException, "MergeEntry partition number not equal");
    }
  }
  if (0==ret) { // do have new partition
    _writer->startPartition();
  }
  return ret;
}

/**
 * finish one partition
 */
void Merger::endPartition() {
  _writer->endPartition();
}

void Merger::initHeap() {
  _heap.clear();
  for (size_t i = 0 ; i < _entries.size() ; i++) {
    MergeEntryPtr pme = _entries[i];
    if (0==pme->next()) {
      _heap.push_back(pme);
    }
  }
  make_heap(&(_heap[0]), &(_heap[0])+_heap.size(), MergeEntryPtrLassThan());
}

void Merger::merge() {
  Timer timer;
  uint64_t total_record = 0;
  _heap.reserve(_entries.size());
  MergeEntryPtr * base = &(_heap[0]);
  while (0==startPartition()) {
    initHeap();
    size_t cur_heap_size = _heap.size();
    while (cur_heap_size>0) {
      MergeEntryPtr top = base[0];
      _writer->writeKey(top->_key, top->_key_len, top->_value_len);
      _writer->writeValue(top->getValue(), top->_value_len);
      total_record++;
      if (0 == top->next()) { // have more, adjust heap
        if (cur_heap_size == 1) {
          continue;
        } else if (cur_heap_size == 2) {
          if (*(base[1]) < *(base[0])) {
            std::swap(base[0], base[1]);
          }
        } else {
          adjust_heap(base, 1, cur_heap_size, MergeEntryPtrLassThan());
        }
      } else { // no more, pop heap
        pop_heap(base, base+cur_heap_size, MergeEntryPtrLassThan());
        cur_heap_size--;
      }
    }
    endPartition();
  }
  double interval = (timer.now() - timer.last())/1000000000.0;
  uint64_t output_size;
  uint64_t real_output_size;
  _writer->getStatistics(output_size, real_output_size);
  LOG("Merge %lu segments: record %llu, avg %.3lf, size %llu, real %llu, time %.3lf",
      _entries.size(),
      total_record,
      (double)output_size/total_record,
      output_size,
      real_output_size,
      interval);
}

} // namespace Hadoop
