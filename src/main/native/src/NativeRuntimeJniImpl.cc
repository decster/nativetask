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


#include "org_apache_hadoop_mapred_nativetask_NativeRuntime.h"
#include "commons.h"
#include "jniutils.h"
#include "NativeObjectFactory.h"

///////////////////////////////////////////////////////////////
// NativeRuntime JNI methods
///////////////////////////////////////////////////////////////

/*
 * Class:     org_apache_hadoop_mapred_nativetask_NativeRuntime
 * Method:    JNIRelease
 * Signature: ()V
 */
JNIEXPORT void JNICALL Java_org_apache_hadoop_mapred_nativetask_NativeRuntime_JNIRelease
  (JNIEnv * jenv, jclass nativeRuntimeClass) {
  try {
    Hadoop::NativeObjectFactory::Release();
  }
  catch (Hadoop::UnsupportException e) {
    JNU_ThrowByName(jenv, "java/lang/UnsupportedOperationException", e.what());
  }
  catch (Hadoop::OutOfMemoryException e) {
    JNU_ThrowByName(jenv, "java/lang/OutOfMemoryException", e.what());
  }
  catch (Hadoop::IOException e) {
    JNU_ThrowByName(jenv, "java/io/IOException", e.what());
  }
  catch (std::exception e) {
    JNU_ThrowByName(jenv, "java/io/IOException", e.what());
  }
  catch (...) {
    JNU_ThrowByName(jenv, "java/io/IOException", "Unkown std::exception");
  }
}

/*
 * Class:     org_apache_hadoop_mapred_nativetask_NativeRuntime
 * Method:    JNIConfigure
 * Signature: ([[B)V
 */
JNIEXPORT void JNICALL Java_org_apache_hadoop_mapred_nativetask_NativeRuntime_JNIConfigure
  (JNIEnv * jenv, jclass nativeRuntimeClass, jobjectArray configs) {
  try {
    Hadoop::Config & config = Hadoop::NativeObjectFactory::GetConfig();
    jsize len = jenv->GetArrayLength(configs);
    for (jsize i = 0; i + 1 < len; i += 2) {
      jbyteArray key_obj = (jbyteArray) jenv->GetObjectArrayElement(configs, i);
      jbyteArray val_obj = (jbyteArray) jenv->GetObjectArrayElement(configs, i + 1);
      config.set(JNU_ByteArrayToString(jenv, key_obj),
          JNU_ByteArrayToString(jenv, val_obj));
    }
  }
  catch (Hadoop::UnsupportException e) {
    JNU_ThrowByName(jenv, "java/lang/UnsupportedOperationException", e.what());
  }
  catch (Hadoop::OutOfMemoryException e) {
    JNU_ThrowByName(jenv, "java/lang/OutOfMemoryException", e.what());
  }
  catch (Hadoop::IOException e) {
    JNU_ThrowByName(jenv, "java/io/IOException", e.what());
  }
  catch (std::exception e) {
    JNU_ThrowByName(jenv, "java/io/IOException", e.what());
  }
  catch (...) {
    JNU_ThrowByName(jenv, "java/io/IOException", "Unkown std::exception");
  }
}

/*
 * Class:     org_apache_hadoop_mapred_nativetask_NativeRuntime
 * Method:    JNICreateNativeObject
 * Signature: ([B[B)J
 */
jlong JNICALL Java_org_apache_hadoop_mapred_nativetask_NativeRuntime_JNICreateNativeObject
  (JNIEnv * jenv, jclass nativeRuntimeClass, jbyteArray clazz) {
  try {
    std::string typeString = JNU_ByteArrayToString(jenv, clazz);
    return (jlong)(Hadoop::NativeObjectFactory::CreateObject(typeString));
  }
  catch (Hadoop::UnsupportException e) {
    JNU_ThrowByName(jenv, "java/lang/UnsupportedOperationException", e.what());
  }
  catch (Hadoop::OutOfMemoryException e) {
    JNU_ThrowByName(jenv, "java/lang/OutOfMemoryException", e.what());
  }
  catch (Hadoop::IOException e) {
    JNU_ThrowByName(jenv, "java/io/IOException", e.what());
  }
  catch (std::exception e) {
    JNU_ThrowByName(jenv, "java/io/IOException", e.what());
  }
  catch (...) {
    JNU_ThrowByName(jenv, "java/io/IOException", "Unknown exception");
  }
  return 0;
}

/*
 * Class:     org_apache_hadoop_mapred_nativetask_NativeRuntime
 * Method:    JNICreateDefaultNativeObject
 * Signature: ([B)J
 */
JNIEXPORT jlong JNICALL Java_org_apache_hadoop_mapred_nativetask_NativeRuntime_JNICreateDefaultNativeObject
  (JNIEnv * jenv, jclass nativeRuntimeClass, jbyteArray type) {
  try {
    std::string typeString = JNU_ByteArrayToString(jenv, type);
    Hadoop::NativeObjectType type = Hadoop::NativeObjectTypeFromString(typeString.c_str());
    return (jlong)(Hadoop::NativeObjectFactory::CreateDefaultObject(type));
  }
  catch (Hadoop::UnsupportException e) {
    JNU_ThrowByName(jenv, "java/lang/UnsupportedOperationException", e.what());
  }
  catch (Hadoop::OutOfMemoryException e) {
    JNU_ThrowByName(jenv, "java/lang/OutOfMemoryException", e.what());
  }
  catch (Hadoop::IOException e) {
    JNU_ThrowByName(jenv, "java/io/IOException", e.what());
  }
  catch (std::exception e) {
    JNU_ThrowByName(jenv, "java/io/IOException", e.what());
  }
  catch (...) {
    JNU_ThrowByName(jenv, "java/io/IOException", "Unknown exception");
  }
  return 0;
}

/*
 * Class:     org_apache_hadoop_mapred_nativetask_NativeRuntime
 * Method:    JNIReleaseNativeObject
 * Signature: (J)V
 */
void JNICALL Java_org_apache_hadoop_mapred_nativetask_NativeRuntime_JNIReleaseNativeObject
  (JNIEnv * jenv, jclass nativeRuntimeClass, jlong objectAddr) {
  try {
    Hadoop::NativeObject * nobj = ((Hadoop::NativeObject *)objectAddr);
    if (NULL==nobj) {
      JNU_ThrowByName(jenv, "java/lang/IllegalArgumentException",
          "Object addr not instance of NativeObject");
      return;
    }
    Hadoop::NativeObjectFactory::ReleaseObject(nobj);
  }
  catch (Hadoop::UnsupportException e) {
    JNU_ThrowByName(jenv, "java/lang/UnsupportedOperationException", e.what());
  }
  catch (Hadoop::OutOfMemoryException e) {
    JNU_ThrowByName(jenv, "java/lang/OutOfMemoryException", e.what());
  }
  catch (Hadoop::IOException e) {
    JNU_ThrowByName(jenv, "java/io/IOException", e.what());
  }
  catch (std::exception e) {
    JNU_ThrowByName(jenv, "java/io/IOException", e.what());
  }
  catch (...) {
    JNU_ThrowByName(jenv, "java/io/IOException", "Unknown exception");
  }
}

/*
 * Class:     org_apache_hadoop_mapred_nativetask_NativeRuntime
 * Method:    registerModule
 * Signature: ([B[B)I
 */
jint JNICALL Java_org_apache_hadoop_mapred_nativetask_NativeRuntime_registerModule
  (JNIEnv * jenv, jclass nativeRuntimeClass, jbyteArray modulePath, jbyteArray moduleName) {
  try {
    std::string pathString = JNU_ByteArrayToString(jenv, modulePath);
    std::string nameString = JNU_ByteArrayToString(jenv, moduleName);
    if (true == Hadoop::NativeObjectFactory::RegisterLibrary(pathString, nameString)) {
      return 0;
    }
  }
  catch (Hadoop::UnsupportException e) {
    JNU_ThrowByName(jenv, "java/lang/UnsupportedOperationException", e.what());
  }
  catch (Hadoop::OutOfMemoryException e) {
    JNU_ThrowByName(jenv, "java/lang/OutOfMemoryException", e.what());
  }
  catch (Hadoop::IOException e) {
    JNU_ThrowByName(jenv, "java/io/IOException", e.what());
  }
  catch (std::exception e) {
    JNU_ThrowByName(jenv, "java/io/IOException", e.what());
  }
  catch (...) {
    JNU_ThrowByName(jenv, "java/io/IOException", "Unknown exception");
  }
  return 1;
}
