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

#ifndef KVFILE_H_
#define KVFILE_H_

#include "Checksum.h"
#include "Buffers.h"
#include "PartitionIndex.h"

namespace NativeTask {

class KVFileReader {
private:
  InputStream * _stream;
  ChecksumInputStream * _source;
  ReadBuffer _reader;
  ChecksumType _checksumType;
  string _codec;
  int32_t _segmentIndex;
  IndexRange * _spillInfo;

public:
  KVFileReader(InputStream * stream, ChecksumType checksumType,
               IndexRange * spill_infos, const string & codec);

  virtual ~KVFileReader();

  /**
   * @return 0 if have next partition, none 0 if no more partition
   */
  int nextPartition();

  /**
   * get next key
   * NULL if no more, then next_partition() need to be called
   * NOTICE: before value() is called, the return pointer value is
   *         guaranteed to be valid
   */
  const char * nextKey(uint32_t & len) {
    if (unlikely(_source->getLimit()==0)) {
      return NULL;
    }
    len = _reader.read_uint32_le();
    return _reader.get(len);
  }

  /**
   * get current value
   */
  const char * value(uint32_t & len) {
    len = _reader.read_uint32_le();
    return _reader.get(len);
  }
};

class KVFileWriter {
protected:
  OutputStream * _stream;
  ChecksumOutputStream * _dest;
  ChecksumType _checksumType;
  string       _codec;
  AppendBuffer _appendBuffer;
  vector<IndexEntry> _spillInfo;

public:
  KVFileWriter(OutputStream * stream, ChecksumType checksumType,
               const string & codec);

  virtual ~KVFileWriter();

  void startPartition();

  void endPartition();

  void writeKey(const char * key, uint32_t key_len,
                              uint32_t value_len);

  void writeValue(const char * value, uint32_t value_len);

  void write(const char * key, uint32_t key_len, const char * value,
             uint32_t value_len) {
    writeKey(key, key_len, value_len);
    writeValue(value, value_len);
  }

  IndexRange * getIndex(uint32_t start);

  void getStatistics(uint64_t & offset, uint64_t & realoffset);
};

} // namespace NativeTask

#endif /* KVFILE_H_ */
