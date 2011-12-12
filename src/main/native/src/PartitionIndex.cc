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
#include "Streams.h"
#include "FileSystem.h"
#include "Buffers.h"
#include "PartitionIndex.h"

namespace Hadoop {

void IndexRange::delete_file() {
  if (filepath.length()>0) {
    struct stat st;
    if (0 == stat(filepath.c_str(),&st)) {
      remove(filepath.c_str());
    }
  }
}

void PartitionIndex::writeIFile(const std::string & filepath) {
  OutputStream * fout = FileSystem::getRaw().create(filepath, true);
  {
    ChecksumOutputStream dest = ChecksumOutputStream(fout, CHECKSUM_CRC32);
    AppendBuffer appendBuffer;
    appendBuffer.init(32*1024, &dest, "");
    uint64_t current_base = 0;
    for (size_t i = 0; i < ranges.size(); i++) {
      IndexRange * range = ranges[i];
      for (size_t j=0; j<range->length;j++) {
        IndexEntry * segment = &(range->segments[j]);
        if (j==0) {
          appendBuffer.write_uint64_be(current_base);
          appendBuffer.write_uint64_be(segment->endPosition);
          appendBuffer.write_uint64_be(segment->realEndPosition);
        }
        else {
          appendBuffer.write_uint64_be(current_base
                                       + range->segments[j - 1].realEndPosition);
          appendBuffer.write_uint64_be(segment->endPosition
                                       - range->segments[j - 1].endPosition);
          appendBuffer.write_uint64_be(segment->realEndPosition
                                       - range->segments[j - 1].realEndPosition);
        }
      }
      current_base += range->segments[range->length - 1].realEndPosition;
    }
    appendBuffer.flush();
    uint32_t chsum = dest.getChecksum();
#ifdef SPILLRECORD_CHECKSUM_UINT
    chsum = bswap(chsum);
    fout->write(&chsum, sizeof(uint32_t));
#else
    uint64_t wtchsum = bswap64((uint64_t)chsum);
    fout->write(&wtchsum, sizeof(uint64_t));
#endif
  }
  fout->close();
  delete fout;
}

}


