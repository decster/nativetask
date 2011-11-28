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

namespace Hadoop {

#ifdef PRINT_LOG

FILE * LOG_DEVICE = stderr;

#endif

void print_trace(int fd) {
  void *array[32];
  size_t size;
  size = backtrace(array, 32);
  backtrace_symbols_fd(array, size, fd);
}

KeyValueType JavaClassToKeyValueType(const std::string & clazz) {
  if (clazz == "org.apache.hadoop.io.BytesWritable") {
    return BytesType;
  }
  else if (clazz == "org.apache.hadoop.io.Text") {
    return TextType;
  }
  return UnknownType;
}

int NameToEnum(const std::string & name) {
  std::string lname = ToLower(name);
  if (lname.length()==0) {
    return 0;
  }
  if (isdigit(lname[0])) {
    return atoi(lname.c_str());
  }
  if (lname == "cqsort") {
    return CQSORT;
  } else if (lname == "cppsort") {
    return CPPSORT;
  } else if (lname == "intermediate") {
    return INTERMEDIATE;
  } else if (lname == "ifile") {
    return IFILE;
  } else if (lname == "fullsort") {
    return FULLSORT;
  } else if (lname == "groupby") {
    return GROUPBY;
  } else if (lname == "nosort") {
    return NOSORT;
  } else if (lname == "plain") {
    return PLAIN;
  } else if (lname == "snappy") {
    return SNAPPY;
  } else if (lname == "text") {
    return TextType;
  } else if (lname == "bytes") {
    return BytesType;
  }
  return 0;
}


std::string ToLower(const std::string & name) {
  std::string ret = name;
  for (size_t i = 0 ; i < ret.length() ; i++) {
    ret.at(i) = ::tolower(ret[i]);
  }
  return ret;
}

std::string Trim(const std::string & str) {
  if (str.length()==0) {
    return str;
  }
  size_t l = 0;
  while (l<str.length() && isspace(str[l])) {
    l++;
  }
  if (l>=str.length()) {
    return std::string();
  }
  size_t r = str.length();
  while (isspace(str[r-1])) {
    r--;
  }
  return str.substr(l, r-l);
}

std::vector<std::string> SplitString(const std::string & src,
                                     const std::string & splitChar,
                                     bool clean) {
  std::vector<std::string> dest;
  if (splitChar.length()==0) {
    return dest;
  }
  size_t cur = 0;
  while (true) {
    size_t pos;
    if (splitChar.length()==1) {
      pos = src.find(splitChar[0], cur);
    } else {
      pos = src.find(splitChar, cur);
    }
    std::string add = src.substr(cur,pos-cur);
    if (clean) {
      std::string trimed = Trim(add);
      if (trimed.length()>0) {
        dest.push_back(trimed);
      }
    } else {
      dest.push_back(add);
    }
    if (pos==std::string::npos) {
      break;
    }
    cur=pos+splitChar.length();
  }
  return dest;
}

std::string JoinString(const std::vector<std::string> & strs,
                       const std::string & joinChar) {
  std::string ret;
  for (size_t i = 0; i < strs.size(); i++) {
    if (i > 0) {
      ret.append(joinChar);
    }
    ret.append(strs[i]);
  }
  return ret;
}


void dump_memory(const void * pos, size_t length, const char * filename) {
  FILE * fout = fopen(filename, "wb");
  fwrite(pos, length, 1, fout);
  fclose(fout);
}


} //namespace Hadoop
