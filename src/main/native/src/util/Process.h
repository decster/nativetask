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

#ifndef PROCESS_H_
#define PROCESS_H_

#include <string>
#include "Streams.h"

namespace NativeTask {

using std::string;

class PipeInputStream : public InputStream {
private:
  uint64_t _read;
  int    _fd;
public:
  int getFd() {
    return _fd;
  }
  void setFd(int fd) {
    _fd = fd;
  }

  PipeInputStream(int fd=-1) :
    _read(0),
    _fd(fd) {
  }

  virtual ~PipeInputStream() {
  }

  virtual void seek(uint64_t position);

  virtual uint64_t tell();

  virtual int32_t read(void * buff, uint32_t length);

  virtual void close();
};

/**
 * Local raw filesystem file output stream
 */
class PipeOutputStream : public OutputStream {
private:
  uint64_t _written;
  int    _fd;
public:
  int getFd() {
    return _fd;
  }
  void setFd(int fd) {
    _fd = fd;
  }

  PipeOutputStream(int fd=-1) :
    _written(0),
    _fd(fd) {
  }

  virtual ~PipeOutputStream() {
  }

  virtual uint64_t tell();

  virtual void write(const void * buff, uint32_t length);

  virtual void flush();

  virtual void close();
};

class Process {
public:
  /**
   * Popen with stdin,stdout,stderr
   * @return subprocess pid
   */
  static int Popen(const string & cmd,
                   int & fdstdin,
                   int & fdstdout,
                   int & fdstderr);

  static int Popen(const string & cmd,
                   FILE *& fstdin,
                   FILE *& fstdout,
                   FILE *& fstderr);

  static int Popen(const string & cmd,
                   PipeOutputStream & pstdin,
                   PipeInputStream & pstdout,
                   PipeInputStream & pstderr);

  static int Run(const string & cmd, string * out, string * err);
};

} // namespace NativeTask


#endif /* PROCESS_H_ */
