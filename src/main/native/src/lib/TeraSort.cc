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
#include "TeraSort.h"

namespace Hadoop {


static uint64_t mask32 = (1l << 32) - 1;
/**
 * The number of iterations separating the precomputed seeds.
 */
static uint32_t seedSkip = 128 * 1024 * 1024;
/**
 * The precomputed seed values after every seedSkip iterations.
 * There should be enough values so that a 2**32 iterations are
 * covered.
 */
static uint64_t seeds[] = { 0L, 4160749568L, 4026531840L, 3892314112L,
                            3758096384L, 3623878656L, 3489660928L, 3355443200L,
                            3221225472L, 3087007744L, 2952790016L, 2818572288L,
                            2684354560L, 2550136832L, 2415919104L, 2281701376L,
                            2147483648L, 2013265920L, 1879048192L, 1744830464L,
                            1610612736L, 1476395008L, 1342177280L, 1207959552L,
                            1073741824L, 939524096L, 805306368L, 671088640L,
                            536870912L, 402653184L, 268435456L, 134217728L, };


TeraGen::TeraGen(uint64_t row, uint64_t splits, uint64_t index) {
  reset(row, splits, index);
}

TeraGen::~TeraGen() {

}

void TeraGen::reset(uint64_t row, uint64_t splits, uint64_t index) {
  _currentRow = row/splits * index;
  _endRow = std::min(row/splits * (index+1), row);

  uint64_t init = _currentRow * 3;
  int baseIndex = (int) ((init & mask32) / seedSkip);
  _seed = seeds[baseIndex];
  for(int i=0; i < init % seedSkip; ++i) {
    nextSeed();
  }
}

int64_t TeraGen::nextSeed() {
  _seed = (_seed * 3141592621l + 663896637) & mask32;
  return _seed;
}

bool TeraGen::next(string & key, string & value) {
  if (_currentRow >= _endRow) {
    return false;
  }
  for(int i=0; i<3; i++) {
    int64_t temp = nextSeed() / 52;
    _keyBytes[3 + 4*i] = (' ' + (temp % 95));
    temp /= 95;
    _keyBytes[2 + 4*i] = (' ' + (temp % 95));
    temp /= 95;
    _keyBytes[1 + 4*i] = (' ' + (temp % 95));
    temp /= 95;
    _keyBytes[4*i] = (' ' + (temp % 95));
  }
  key.assign(_keyBytes, 10);
  snprintf(_valueBytes, 100, "%10llu", _currentRow);
  value.assign(_valueBytes, 10);
  int base = (int) ((_currentRow * 8) % 26);
  for(int i=0; i<7; ++i) {
    value.append(10, (char)((base+i) % 26)+'A');
  }
  value.append(8, (char)((base+7) % 26)+'A');
  _currentRow++;
  return true;
}

bool TeraGen::nextLine(string & line) {
  string key,value;
  if (next(key,value)) {
    line.reserve(100);
    line.assign(key);
    line.append(value);
    line.append("\r\n");
    return true;
  }
  return false;
}


bool TeraGen::next(string & kv) {
  string key,value;
  if (next(key,value)) {
    kv.reserve(98);
    kv.assign(key);
    kv.append(value);
    return true;
  }
  return false;
}

///////////////////////////////////////////////////////////

bool TeraRecordReader::next(Buffer & key, Buffer & value) {
  Buffer buff;
  uint32_t len = readLine(buff);
  if (len == 0) {
    return false;
  }
  uint32_t keylen = std::min(buff.length(), 10u);
  key.reset(buff.data(), keylen);
  value.reset(buff.data() + keylen, buff.length() - keylen);
  _pos += len;
  return true;
}

///////////////////////////////////////////////////////////

void TeraRecordWriter::collect(const void * key, uint32_t keyLen,
                             const void * value, uint32_t valueLen) {
  _appendBuffer.write(key, keyLen);
  _appendBuffer.write(value, valueLen);
  _appendBuffer.write("\r\n", 2);
}

} // namespace Hadoop
