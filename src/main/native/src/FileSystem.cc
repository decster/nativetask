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

#include <errno.h>
#include <jni.h>
#include "commons.h"
#include "jniutils.h"
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

class RawFileSystem : public FileSystem {
public:
  InputStream * open(const string & path) {
    return new FileInputStream(path);
  }

  OutputStream * create(const string & path, bool overwrite) {
    return new FileOutputStream(path, overwrite);
  }

  uint64_t getLength(const string & path) {
    struct stat st;
    if (::stat(path.c_str(), &st) != 0) {
      char buff[256];
      strerror_r(errno, buff, 256);
      THROW_EXCEPTION(IOException, StringUtil::Format(
          "stat path %s failed, %s", path.c_str(), buff));
    }
    return st.st_size;
  }

  void remove(const string & path) {
    if (!exists(path)) {
      LOG("remove file %s not exists, ignore", path.c_str());
      return;
    }
    if (::remove(path.c_str()) != 0) {
      int err = errno;
      if (::system(StringUtil::Format("rm -rf %s", path.c_str()).c_str()) == 0) {
        return;
      }
      char buff[256];
      strerror_r(err, buff, 256);
      THROW_EXCEPTION(IOException, StringUtil::Format(
          "remove path %s failed, %s", path.c_str(), buff));
    }
  }

  bool exists(const string & path) {
    struct stat st;
    if (::stat(path.c_str(), &st) != 0) {
      return false;
    }
    return true;
  }

  void mkdirs(const string & path) {
    int ret = ::mkdir(path.c_str(), 0755);
    if (ret != 0) {
      if (errno != EEXIST) {
        char buff[256];
        strerror_r(errno, buff, 256);
        THROW_EXCEPTION(IOException, StringUtil::Format(
            "mkdirs path %s failed, %s", path.c_str(), buff));
      }
    }
  }
};

class JavaFileSystem : public FileSystem {
private:
  static int inited;
public:
  static jclass NativeRuntimeClass;
  static jmethodID OpenFileMethodID;
  static jmethodID CreateFileMethodID;
  static jmethodID GetFileLengthMethodID;
  static jmethodID ExistsMethodID;
  static jmethodID RemoveMethodID;
  static jmethodID MkdirsMethodID;

  static jclass FSDataInputStreamClass;
  static jmethodID ReadMethodID;
  static jmethodID SeekMethodID;
  static jmethodID IGetPosMethodID;
  static jmethodID ICloseMethodID;


  static jclass FSDataOutputStreamClass;
  static jmethodID WriteMethodID;
  static jmethodID OGetPosMethodID;
  static jmethodID OFlushMethodID;
  static jmethodID OCloseMethodID;

  static bool initIDs() {
    JNIEnv * env = JNU_GetJNIEnv();
    jclass cls = env->FindClass("org/apache/hadoop/mapred/nativetask/NativeRuntime");
    if (cls == NULL) {
      LOG("Can not found class org/apache/hadoop/mapred/nativetask/NativeRuntime");
      return false;
    }
    NativeRuntimeClass = (jclass)env->NewGlobalRef(cls);
    OpenFileMethodID = env->GetStaticMethodID(cls, "openFile", "([B)Lorg/apache/hadoop/fs/FSDataInputStream;");
    CreateFileMethodID = env->GetStaticMethodID(cls, "createFile", "([BZ)Lorg/apache/hadoop/fs/FSDataOutputStream;");
    GetFileLengthMethodID = env->GetStaticMethodID(cls, "getFileLength", "([B)J");
    ExistsMethodID = env->GetStaticMethodID(cls, "exists", "([B)Z");
    RemoveMethodID = env->GetStaticMethodID(cls, "remove", "([B)Z");
    MkdirsMethodID = env->GetStaticMethodID(cls, "mkdirs", "([B)Z");
    env->DeleteLocalRef(cls);

    jclass fsincls = env->FindClass("org/apache/hadoop/fs/FSDataInputStream");
    if (fsincls == NULL) {
      LOG("Can not found class org/apache/hadoop/fs/FSDataInputStream");
      return false;
    }
    FSDataInputStreamClass = (jclass)env->NewGlobalRef(fsincls);
    ReadMethodID = env->GetMethodID(fsincls, "read", "([BII)I");
    SeekMethodID = env->GetMethodID(fsincls, "seek", "(J)V");
    IGetPosMethodID = env->GetMethodID(fsincls, "getPos", "()J");
    ICloseMethodID = env->GetMethodID(fsincls, "close", "()V");
    env->DeleteLocalRef(fsincls);

    jclass fsoutcls = env->FindClass("org/apache/hadoop/fs/FSDataOutputStream");
    if (fsoutcls == NULL) {
      LOG("Can not found class org/apache/hadoop/fs/FSDataOutputStream");
      return false;
    }
    FSDataOutputStreamClass = (jclass)env->NewGlobalRef(fsincls);
    WriteMethodID = env->GetMethodID(fsoutcls, "write", "([BII)V");
    OGetPosMethodID = env->GetMethodID(fsoutcls, "getPos", "()J");
    OFlushMethodID = env->GetMethodID(fsoutcls, "flush", "()V");
    OCloseMethodID = env->GetMethodID(fsoutcls, "close", "()V");
    env->DeleteLocalRef(fsoutcls);
    return true;
  }

  static void checkInit() {
    if (inited == 0) {
      if (initIDs()) {
        inited = 1;
      } else {
        inited = -1;
      }
    }
    if (inited != 1) {
      THROW_EXCEPTION(HadoopException, "load NativeRuntime jni for JavaFileSystem failed");
    }
  }
public:
  JavaFileSystem() {
  }

  InputStream * open(const string & path) {
    checkInit();
    JNIEnv * env = JNU_GetJNIEnv();
    jbyteArray pathobject = env->NewByteArray(path.length());
    env->SetByteArrayRegion(pathobject, 0, path.length(), (jbyte*)path.c_str());
    jobject ret = env->CallStaticObjectMethod(NativeRuntimeClass, OpenFileMethodID, pathobject);
    env->DeleteLocalRef(pathobject);
    if (env->ExceptionCheck()) {
      env->ExceptionDescribe();
      env->ExceptionClear();
      THROW_EXCEPTION_EX(HadoopException, "open java file [%s] got exception", path.c_str());
    }
    jobject fref = (jclass)env->NewGlobalRef(ret);
    env->DeleteLocalRef(ret);
    return new FSDataInputStream(fref);
  }

  OutputStream * create(const string & path, bool overwrite) {
    checkInit();
    JNIEnv * env = JNU_GetJNIEnv();
    jbyteArray pathobject = env->NewByteArray(path.length());
    env->SetByteArrayRegion(pathobject, 0, path.length(), (jbyte*)path.c_str());
    jobject ret = env->CallStaticObjectMethod(NativeRuntimeClass, CreateFileMethodID,
                                        pathobject, (jboolean) overwrite);
    env->DeleteLocalRef(pathobject);
    if (env->ExceptionCheck()) {
      env->ExceptionDescribe();
      env->ExceptionClear();
      THROW_EXCEPTION_EX(HadoopException, "create java file [%s] got exception", path.c_str());
    }
    jobject fref = (jclass)env->NewGlobalRef(ret);
    env->DeleteLocalRef(ret);
    return new FSDataOutputStream(fref);
  }

  uint64_t getLength(const string & path) {
    checkInit();
    JNIEnv * env = JNU_GetJNIEnv();
    jbyteArray pathobject = env->NewByteArray(path.length());
    env->SetByteArrayRegion(pathobject, 0, path.length(), (jbyte*)path.c_str());
    jlong ret = env->CallStaticLongMethod(NativeRuntimeClass, GetFileLengthMethodID,
                                        pathobject);
    env->DeleteLocalRef(pathobject);
    if (env->ExceptionCheck()) {
      env->ExceptionDescribe();
      env->ExceptionClear();
      THROW_EXCEPTION_EX(HadoopException, "getLength file [%s] got exception", path.c_str());
    }
    return (uint64_t)ret;
  }

  void remove(const string & path) {
    checkInit();
    JNIEnv * env = JNU_GetJNIEnv();
    jbyteArray pathobject = env->NewByteArray(path.length());
    env->SetByteArrayRegion(pathobject, 0, path.length(), (jbyte*)path.c_str());
    jboolean ret = env->CallStaticBooleanMethod(NativeRuntimeClass, RemoveMethodID,
                                        pathobject);
    env->DeleteLocalRef(pathobject);
    if (env->ExceptionCheck()) {
      env->ExceptionDescribe();
      env->ExceptionClear();
      THROW_EXCEPTION_EX(HadoopException, "remove path [%s] got exception", path.c_str());
    }
    if (!ret) {
      THROW_EXCEPTION_EX(HadoopException, "remove path [%s] return false", path.c_str());
    }
  }

  bool exists(const string & path) {
    checkInit();
    JNIEnv * env = JNU_GetJNIEnv();
    jbyteArray pathobject = env->NewByteArray(path.length());
    env->SetByteArrayRegion(pathobject, 0, path.length(), (jbyte*)path.c_str());
    jboolean ret = env->CallStaticBooleanMethod(NativeRuntimeClass, ExistsMethodID,
                                        pathobject);
    env->DeleteLocalRef(pathobject);
    if (env->ExceptionCheck()) {
      env->ExceptionDescribe();
      env->ExceptionClear();
      THROW_EXCEPTION_EX(HadoopException, "test exists() path [%s] got exception", path.c_str());
    }
    return ret;
  }

  void mkdirs(const string & path) {
    checkInit();
    JNIEnv * env = JNU_GetJNIEnv();
    jbyteArray pathobject = env->NewByteArray(path.length());
    env->SetByteArrayRegion(pathobject, 0, path.length(), (jbyte*)path.c_str());
    jboolean ret = env->CallStaticBooleanMethod(NativeRuntimeClass, MkdirsMethodID,
                                        pathobject);
    env->DeleteLocalRef(pathobject);
    if (env->ExceptionCheck()) {
      env->ExceptionDescribe();
      env->ExceptionClear();
      THROW_EXCEPTION_EX(HadoopException, "mkdirs path [%s] got exception", path.c_str());
    }
    if (!ret) {
      THROW_EXCEPTION_EX(HadoopException, "mkdirs path [%s] return false", path.c_str());
    }
  }
};

int JavaFileSystem::inited = 0;
jclass JavaFileSystem::NativeRuntimeClass = NULL;
jmethodID JavaFileSystem::OpenFileMethodID = NULL;
jmethodID JavaFileSystem::CreateFileMethodID = NULL;
jmethodID JavaFileSystem::GetFileLengthMethodID = NULL;
jmethodID JavaFileSystem::ExistsMethodID = NULL;
jmethodID JavaFileSystem::RemoveMethodID = NULL;
jmethodID JavaFileSystem::MkdirsMethodID = NULL;

jclass JavaFileSystem::FSDataInputStreamClass = NULL;
jmethodID JavaFileSystem::ReadMethodID = NULL;
jmethodID JavaFileSystem::SeekMethodID = NULL;
jmethodID JavaFileSystem::IGetPosMethodID = NULL;
jmethodID JavaFileSystem::ICloseMethodID = NULL;


jclass JavaFileSystem::FSDataOutputStreamClass = NULL;
jmethodID JavaFileSystem::WriteMethodID = NULL;
jmethodID JavaFileSystem::OGetPosMethodID = NULL;
jmethodID JavaFileSystem::OFlushMethodID = NULL;
jmethodID JavaFileSystem::OCloseMethodID = NULL;

/////////////////////////////////////////////////////////////

FSDataInputStream::FSDataInputStream(void * jobject) :
    _jobject(jobject) {
}

FSDataInputStream::~FSDataInputStream() {
  close();
}

void FSDataInputStream::seek(uint64_t position) {
  JNIEnv * env = JNU_GetJNIEnv();
  env->CallVoidMethod((jobject)_jobject, JavaFileSystem::SeekMethodID, (jlong)position);
  if (env->ExceptionCheck()) {
    THROW_EXCEPTION(HadoopException, "seek failed");
  }
}

uint64_t FSDataInputStream::tell() {
  JNIEnv * env = JNU_GetJNIEnv();
  jlong pos = env->CallLongMethod((jobject)_jobject, JavaFileSystem::IGetPosMethodID);
  if (env->ExceptionCheck()) {
    THROW_EXCEPTION(HadoopException, "tell failed");
  }
  return (uint64_t)pos;
}

int32_t FSDataInputStream::read(void * buff, uint32_t length) {
  JNIEnv * env = JNU_GetJNIEnv();
  jbyteArray ba = env->NewByteArray(length);
  env->SetByteArrayRegion(ba, 0, length, (jbyte*)buff);
  jint rd = env->CallIntMethod((jobject) _jobject,
                               JavaFileSystem::ReadMethodID, ba, (jint) 0,
                               (jint) length);
  if (rd>0) {
    env->GetByteArrayRegion(ba, 0, rd, (jbyte*)buff);
  }
  env->DeleteLocalRef(ba);
  if (env->ExceptionCheck()) {
    THROW_EXCEPTION(HadoopException, "read throw exception in java side");
  }
  return rd;
}

void FSDataInputStream::close() {
  if (_jobject != NULL) {
    JNIEnv * env = JNU_GetJNIEnv();
    env->CallVoidMethod((jobject)_jobject, JavaFileSystem::ICloseMethodID);
    if (env->ExceptionCheck()) {
      THROW_EXCEPTION(HadoopException, "close failed");
    }
    _jobject = NULL;
  }
}


///////////////////////////////////////////////////////////

FSDataOutputStream::FSDataOutputStream(void * jobject) :
    _jobject(jobject) {

}

FSDataOutputStream::~FSDataOutputStream() {
  JNIEnv * env = JNU_GetJNIEnv();
  env->DeleteGlobalRef((jobject)_jobject);
}

uint64_t FSDataOutputStream::tell() {
  JNIEnv * env = JNU_GetJNIEnv();
  jlong pos = env->CallLongMethod((jobject)_jobject, JavaFileSystem::OGetPosMethodID);
  if (env->ExceptionCheck()) {
    THROW_EXCEPTION(HadoopException, "tell failed");
  }
  return (uint64_t)pos;
}

void FSDataOutputStream::write(const void * buff, uint32_t length) {
  JNIEnv * env = JNU_GetJNIEnv();
  jbyteArray ba = env->NewByteArray(length);
  env->SetByteArrayRegion(ba, 0, length, (jbyte*)buff);
  env->CallVoidMethod((jobject) _jobject, JavaFileSystem::WriteMethodID, ba,
                      (jint) 0, (jint) length);
  env->DeleteLocalRef(ba);
  if (env->ExceptionCheck()) {
    THROW_EXCEPTION(HadoopException, "write throw exception in java side");
  }
}

void FSDataOutputStream::flush() {
  JNIEnv * env = JNU_GetJNIEnv();
  env->CallVoidMethod((jobject)_jobject, JavaFileSystem::OFlushMethodID);
  if (env->ExceptionCheck()) {
    THROW_EXCEPTION(HadoopException, "flush failed");
  }
}

void FSDataOutputStream::close() {
  if (_jobject != NULL) {
    JNIEnv * env = JNU_GetJNIEnv();
    env->CallVoidMethod((jobject)_jobject, JavaFileSystem::OCloseMethodID);
    if (env->ExceptionCheck()) {
      THROW_EXCEPTION(HadoopException, "close failed");
    }
    _jobject = NULL;
  }
}


///////////////////////////////////////////////////////////

extern RawFileSystem RawFileSystemInstance;
extern JavaFileSystem JavaFileSystemInstance;

RawFileSystem RawFileSystemInstance = RawFileSystem();
JavaFileSystem JavaFileSystemInstance = JavaFileSystem();

string FileSystem::getDefaultUri(Config & config) {
  const char * nm = config.get("fs.default.name");
  if (nm == NULL) {
    nm = config.get("fs.defaultFS");
  }
  if (nm == NULL) {
    return string("file:///");
  } else {
    return string(nm);
  }
}

FileSystem & FileSystem::getRaw() {
  return RawFileSystemInstance;
}

FileSystem & FileSystem::getJava(Config & config) {
  return JavaFileSystemInstance;
}

FileSystem & FileSystem::get(Config & config) {
  string uri = getDefaultUri(config);
  if (uri == "file:///") {
    return RawFileSystemInstance;
  } else {
    return JavaFileSystemInstance;
  }
}

} // namespace Hadoap
