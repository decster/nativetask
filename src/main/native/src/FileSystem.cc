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

#include <jni.h>
#include "commons.h"
#include "NativeTask.h"
#include "FileSystem.h"

namespace Hadoop {

/////////////////////////////////////////////////////////////

void InputStream::seek(uint64_t position) {
  THROW_EXCEPTION(UnsupportException, "seek not support");
}

uint64_t InputStream::tell() {
  THROW_EXCEPTION(UnsupportException, "tell not support");
}

/////////////////////////////////////////////////////////////

uint64_t OutputStream::tell() {
  THROW_EXCEPTION(UnsupportException, "tell not support");
}

/////////////////////////////////////////////////////////////

FileInputStream::FileInputStream(const string & path) {
  _handle = fopen(path.c_str(), "rb");
  if (_handle != NULL) {
    _fd = fileno(_handle);
    _path = path;
  } else {
    _fd = -1;
  }
}

FileInputStream::~FileInputStream() {
  close();
}

void FileInputStream::seek(uint64_t position) {
  ::lseek(_fd, position, SEEK_SET);
}

uint64_t FileInputStream::tell() {
  return ::lseek(_fd, 0, SEEK_CUR);
}

int32_t FileInputStream::read(void * buff, uint32_t length) {
  return ::read(_fd, buff, length);
}

void FileInputStream::close() {
  if (_handle != NULL) {
    fclose(_handle);
    _handle = NULL;
    _fd = -1;
  }
}

/////////////////////////////////////////////////////////////

FileOutputStream::FileOutputStream(const string & path, bool overwite) {
  _handle = fopen(path.c_str(), "wb");
  if (_handle != NULL) {
    _fd = fileno(_handle);
    _path = path;
  } else {
    _fd = -1;
  }
}

FileOutputStream::~FileOutputStream() {
  close();
}

uint64_t FileOutputStream::tell() {
  return ::lseek(_fd, 0, SEEK_CUR);
}

void FileOutputStream::write(const void * buff, uint32_t length) {
  if (::write(_fd, buff, length) < length) {
    THROW_EXCEPTION(IOException, "::write error");
  }
}

void FileOutputStream::flush() {
}

void FileOutputStream::close() {
  if (_handle != NULL) {
    fclose(_handle);
    _handle = NULL;
    _fd = -1;
  }
}


/////////////////////////////////////////////////////////////

FSDataInputStream::FSDataInputStream(void * jenv) :
    _streamObjRef(NULL),
    _buffObjRef(NULL),
    _buffSize(0) {
}

FSDataInputStream::~FSDataInputStream() {

}

void FSDataInputStream::seek(uint64_t position) {

}

uint64_t FSDataInputStream::tell() {
  return 0;
}

int32_t FSDataInputStream::read(void * buff, uint32_t length) {
  return 0;
}

void FSDataInputStream::close() {

}


/////////////////////////////////////////////////////////////

FSDataOutputStream::FSDataOutputStream(void * jenv) :
    _streamObjRef(NULL),
    _buffObjRef(NULL),
    _buffSize(0) {

}

FSDataOutputStream::~FSDataOutputStream() {

}

uint64_t FSDataOutputStream::tell() {
  return 0;
}

void FSDataOutputStream::write(const void * buff, uint32_t length) {
}

void FSDataOutputStream::flush() {

}

void FSDataOutputStream::close() {

}

/////////////////////////////////////////////////////////////
void * FileSystem::_tempJEnv = NULL;
bool FileSystem::_IDInited = false;

void FileSystem::checkInitID() {
  if (_IDInited==false) {
    _IDInited = true;
  }
}

void FileSystem::setTempJEnv(void * tempJEnv) {
  _tempJEnv = tempJEnv;
}

FSDataInputStream * FileSystem::openJava(const string & path,
                                     uint32_t buffSize) {
  return NULL;
}

FSDataOutputStream * FileSystem::createJava(const string & path, bool overwrite,
                                        uint32_t buffSize) {
  return NULL;
}

uint64_t FileSystem::getLengthJava(const string & path) {
  return 0;
}

FileInputStream * FileSystem::openRaw(const string & path) {
  return new FileInputStream(path);
}

FileOutputStream * FileSystem::createRaw(const string & path, bool overwrite) {
  return new FileOutputStream(path, overwrite);
}

uint64_t FileSystem::getLength(const string & path) {
  struct stat st;
  ::stat(path.c_str(), &st);
  return st.st_size;
}

} // namespace Hadoap
