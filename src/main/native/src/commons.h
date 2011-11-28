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


#ifndef COMMONS_H_
#define COMMONS_H_

#include <stdint.h>
#include <assert.h>
#include <execinfo.h>
#include <string.h>
#include <stdio.h>
#include <memory.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <limits>
#include <string>
#include <vector>
#include <map>
#include <algorithm>

#include "primitives.h"
#include "NativeTask.h"

namespace Hadoop {

#define PRINT_LOG

#ifdef PRINT_LOG

extern FILE * LOG_DEVICE;
#define LOG(_fmt_, args...)   if (LOG_DEVICE) { \
    time_t log_timer; struct tm log_tm; \
    time(&log_timer); localtime_r(&log_timer, &log_tm); \
    fprintf(LOG_DEVICE, "%02d/%02d/%02d %02d:%02d:%02d INFO "_fmt_"\n", \
    log_tm.tm_year%100, log_tm.tm_mon+1, log_tm.tm_mday, \
    log_tm.tm_hour, log_tm.tm_min, log_tm.tm_sec, \
    ##args);}

#else

#define LOG(_fmt_, args...)

#endif


/**
 * internal sort method
 * CQSORT: using standard c qsort routine, faster on large data (>200M)
 * CPPSORT: using c++ std::sort, faster on small data (<2M)
 */
enum SortType {
  CQSORT = 0,
  CPPSORT = 1,
};


/**
 * spill file type
 * INTERMEDIATE: a simple key/value sequence file
 * IFILE: classic hadoop IFile
 */
enum OutputFileType {
  INTERMEDIATE = 0,
  IFILE = 1,
};

/**
 * key/value recored order requirements
 * FULLSORT: hadoop  standard
 * GROUPBY:  same key are grouped together, but not in order
 * NOSORT:   no order at all
 */
enum RecordOrderType {
  FULLSORT = 0,
  GROUPBY = 1,
  NOSORT = 2,
};

enum CompressionType {
  PLAIN = 0,
  SNAPPY = 1,
};

enum KeyValueType {
  TextType = 0,
  BytesType = 1,
  UnknownType = 2
};

KeyValueType JavaClassToKeyValueType(const std::string & clazz);

int NameToEnum(const std::string & name);

std::string ToLower(const std::string & name);

std::string Trim(const std::string & str);

std::vector<std::string> SplitString(const std::string & src,
                                     const std::string & splitChar,
                                     bool clean=false);

std::string JoinString(const std::vector<std::string> & strs,
                       const std::string & joinChar);

inline static uint32_t GetCeil(uint32_t v, uint32_t unit) {
  return ((v + unit - 1) / unit) * unit;
}

inline static int32_t HashBytes(const char * bytes, int length) {
  int hash = 1;
  for (int i = 0; i < length; i++)
    hash = (31 * hash) + (int32_t)bytes[i];
  return hash;
}


/**
 * io operation
 */

#define READ(ret, fd, buff, len) \
  ret = ::read(fd, buff, len); \
  if (ret < 0) { \
    THROW_EXCEPTION(IOException, "sys call read() failed"); \
  }

#define WRITE(fd, start, len) if (::write(fd, start, len) < 0) {\
  THROW_EXCEPTION(IOException, "sys call write() error");}


/**
 * Dump memory content for debug
 */
extern void dump_memory(const void * pos, size_t length, const char * filename);

/**
 * print stack trace
 */
extern void print_trace(int fd);


} // namespace Hadoop




#endif /* COMMONS_H_ */
