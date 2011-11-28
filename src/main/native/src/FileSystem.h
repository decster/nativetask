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

#ifndef FILESYSTEM_H_
#define FILESYSTEM_H_

#include "Streams.h"

namespace Hadoop {

class FileSystem;

/**
 * Local raw filesystem file input stream
 */
class FileInputStream : public InputStream {
private:
  string _path;
  FILE * _handle;
  int    _fd;
public:
  FileInputStream(const string & path);
  virtual ~FileInputStream();

  virtual void seek(uint64_t position);

  virtual uint64_t tell();

  virtual int32_t read(void * buff, uint32_t length);

  virtual void close();
};

/**
 * Local raw filesystem file output stream
 */
class FileOutputStream : public OutputStream {
private:
  string _path;
  FILE * _handle;
  int    _fd;
public:
  FileOutputStream(const string & path, bool overwite = true);
  virtual ~FileOutputStream();

  virtual uint64_t tell();

  virtual void write(const void * buff, uint32_t length);

  virtual void flush();

  virtual void close();
};


/**
 * Simple wrapper for java org.apache.hadoop.fs.FSDataInputstream
 */
class FSDataInputStream : public InputStream {
  friend class FileSystem;
private:
  void * _streamObjRef;
  void * _buffObjRef;
  uint32_t _buffSize;

  FSDataInputStream(void * jenv);
public:
  ~FSDataInputStream();

  virtual void seek(uint64_t position);

  virtual uint64_t tell();

  virtual int32_t read(void * buff, uint32_t length);

  virtual void close();
};

/**
 * Simple wrapper for java org.apache.hadoop.fs.FSDataOutputstream
 */
class FSDataOutputStream : public OutputStream {
  friend class FileSystem;
private:
  void * _streamObjRef;
  void * _buffObjRef;
  uint32_t _buffSize;

  FSDataOutputStream(void * jenv);
public:
  ~FSDataOutputStream();

  virtual uint64_t tell();

  virtual void write(const void * buff, uint32_t length);

  virtual void flush();

  virtual void close();
};

/**
 * FileSystem interface
 */
class FileSystem {
private:
  static void * _tempJEnv;
  static bool _IDInited;

  static void checkInitID();
public:
  static void setTempJEnv(void * tempJEnv);

  static FSDataInputStream * openJava(const string & path,
                                  uint32_t buffSize = 32 * 1024);
  static FSDataOutputStream * createJava(const string & path,
                                     bool overwrite = true,
                                     uint32_t buffSize = 32 * 1024);
  static uint64_t getLengthJava(const string & path);

  static FileInputStream * openRaw(const string & path);
  static FileOutputStream * createRaw(const string & path, bool overwrite = true);
  static uint64_t getLength(const string & path);
};

} // namespace Hadoop


#endif /* FILESYSTEM_H_ */
