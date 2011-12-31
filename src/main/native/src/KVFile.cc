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
#include "KVFile.h"


namespace NativeTask {

///////////////////////////////////////////////////////////

KVFileReader::KVFileReader(InputStream * stream, ChecksumType checksumType,
                           IndexRange * spill_infos, const string & codec) :
    _stream(stream),
    _source(NULL),
    _checksumType(checksumType),
    _codec(codec),
    _segmentIndex(-1),
    _spillInfo(spill_infos) {
  _source = new ChecksumInputStream(_stream, _checksumType);
  _source->setLimit(0);
  _reader.init(128*1024, _source, _codec);
}

KVFileReader::~KVFileReader() {
  delete _source;
  _source = NULL;
}

/**
 * 0 if success
 * 1 if end
 */
int KVFileReader::nextPartition() {
  if (0 != _source->getLimit()) {
    THROW_EXCEPTION(IOException, "bad ifile segment length");
  }
  if (_segmentIndex >= 0) {
    // verify checksum
    uint32_t chsum = 0;
    if (4 != _stream->readFully(&chsum, 4)) {
      THROW_EXCEPTION(IOException, "read ifile checksum failed");
    }
    uint32_t actual = bswap(chsum);
    uint32_t expect = _source->getChecksum();
    if (actual != expect) {
      THROW_EXCEPTION_EX(IOException, "read ifile checksum not match, actual %x expect %x", actual, expect);
    }
  }
  _segmentIndex++;
  if (_segmentIndex < (int)(_spillInfo->length)) {
    int64_t end_pos = (int64_t)_spillInfo->segments[_spillInfo->start + _segmentIndex].realEndPosition;
    if (_segmentIndex > 0) {
      end_pos -= (int64_t)_spillInfo->segments[_spillInfo->start + _segmentIndex - 1].realEndPosition;
    }
    if (end_pos < 0) {
      THROW_EXCEPTION(IOException, "bad ifile format");
    }
    // exclude checksum
    _source->setLimit(end_pos - 4);
    _source->resetChecksum();
    return 0;
  }
  else {
    return 1;
  }
}


///////////////////////////////////////////////////////////

KVFileWriter::KVFileWriter(OutputStream * stream, ChecksumType checksumType,
                           const string & codec) :
    _stream(stream),
    _dest(NULL),
    _checksumType(checksumType),
    _codec(codec) {
  _dest = new ChecksumOutputStream(_stream, _checksumType);
  _appendBuffer.init(128*1024, _dest, _codec);
}

KVFileWriter::~KVFileWriter() {
  delete _dest;
  _dest = NULL;
}

void KVFileWriter::startPartition() {
  _spillInfo.push_back(IndexEntry());
  _dest->resetChecksum();
}

void KVFileWriter::endPartition() {
  _appendBuffer.flush();
  uint32_t chsum = _dest->getChecksum();
  chsum = bswap(chsum);
  _stream->write(&chsum, sizeof(chsum));
  _stream->flush();
  IndexEntry * info = &(_spillInfo[_spillInfo.size()-1]);
  info->endPosition = _appendBuffer.getCounter();
  info->realEndPosition = _stream->tell();
}

void KVFileWriter::writeKey(const char * key, uint32_t key_len, uint32_t value_len) {
  _appendBuffer.write_uint32_le(key_len);
  if (key_len>0) {
    _appendBuffer.write(key, key_len);
  }
}

void KVFileWriter::writeValue(const char * value, uint32_t value_len) {
  _appendBuffer.write_uint32_le(value_len);
  if (value_len>0) {
    _appendBuffer.write(value, value_len);
  }
}


IndexRange * KVFileWriter::getIndex(uint32_t start) {
  IndexEntry * segs = new IndexEntry[_spillInfo.size()];
  for (size_t i = 0; i < _spillInfo.size(); i++) {
    segs[i] = _spillInfo[i];
  }
  return new IndexRange(start, (uint32_t) _spillInfo.size(), "", segs);
}

void KVFileWriter::getStatistics(uint64_t & offset, uint64_t & realoffset) {
  if (_spillInfo.size()>0) {
    offset = _spillInfo[_spillInfo.size()-1].endPosition;
    realoffset = _spillInfo[_spillInfo.size()-1].realEndPosition;
  } else{
    offset = 0;
    realoffset = 0;
  }
}

} // namespace NativeTask
