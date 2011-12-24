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

#include "org_apache_hadoop_mapred_nativetask_NativeBatchProcessor.h"
#include "commons.h"
#include "jniutils.h"
#include "BatchHandler.h"
#include "NativeObjectFactory.h"

///////////////////////////////////////////////////////////////
// NativeBatchProcessor jni util methods
///////////////////////////////////////////////////////////////

static jfieldID  InputBufferFieldID    = NULL;
static jfieldID  OutputBufferFieldID   = NULL;
static jmethodID FlushOutputMethodID   = NULL;
static jmethodID FinishOutputMethodID  = NULL;
static jmethodID SendCommandToJavaMethodID   = NULL;

///////////////////////////////////////////////////////////////
// BatchHandler methods
///////////////////////////////////////////////////////////////

namespace Hadoop {

BatchHandler::BatchHandler() :
  _tempJNIEnv(NULL),
  _tempJProcessor(NULL),
  _hasJavaException(false) {
}

BatchHandler::~BatchHandler() {
}

void BatchHandler::onInputData(uint32_t length) {
  if (length>_ib.capacity) {
    THROW_EXCEPTION(IOException, "length larger than input buffer capacity");
  }
  _ib.position = length;
  handleInput(_ib.buff, length);
}

void BatchHandler::flushOutput(uint32_t length) {
  if (NULL == _ob.buff) {
    return;
  }
  ((JNIEnv*)_tempJNIEnv)->CallVoidMethod((jobject)_tempJProcessor,
      FlushOutputMethodID, (jint) length);
  if (((JNIEnv*) _tempJNIEnv)->ExceptionCheck()) {
    _hasJavaException = true;
  }
}

void BatchHandler::finishOutput() {
  if (NULL == _ob.buff) {
    return;
  }
  ((JNIEnv*)_tempJNIEnv)->CallVoidMethod((jobject)_tempJProcessor,
      FinishOutputMethodID);
  if (((JNIEnv*) _tempJNIEnv)->ExceptionCheck()) {
    _hasJavaException = true;
  }
}

void BatchHandler::onSetup(char * inputBuffer, uint32_t inputBufferCapacity,
    char * outputBuffer, uint32_t outputBufferCapacity) {
  _ib.reset(inputBuffer, inputBufferCapacity);
  // reserve space for simple_memcpy
  if (NULL != outputBuffer) {
    if (outputBufferCapacity <= 1024) {
      THROW_EXCEPTION(IOException, "Output buffer size too small for BatchHandler");
    }
    _ob.reset(outputBuffer, outputBufferCapacity - sizeof(uint64_t));
  }
  configure(NativeObjectFactory::GetConfig());
}

std::string BatchHandler::sendCommand(const std::string & data) {
  jbyteArray jcmdData = ((JNIEnv*) _tempJNIEnv)->NewByteArray(data.length());
  ((JNIEnv*) _tempJNIEnv)->SetByteArrayRegion(jcmdData, 0,
      (jsize) data.length(), (jbyte*) data.c_str());
  jbyteArray ret = (jbyteArray) ((JNIEnv*) _tempJNIEnv)->CallObjectMethod(
      (jobject) _tempJProcessor, SendCommandToJavaMethodID, jcmdData);
  if (((JNIEnv*) _tempJNIEnv)->ExceptionCheck()) {
    _hasJavaException = true;
    return std::string("");
  }
  return JNU_ByteArrayToString(((JNIEnv*) _tempJNIEnv), ret);
}

} // namespace Hadoop


///////////////////////////////////////////////////////////////
// NativeBatchProcessor jni methods
///////////////////////////////////////////////////////////////
using namespace Hadoop;

/*
 * Class:     org_apache_hadoop_mapred_nativetask_NativeBatchProcessor
 * Method:    setupHandler
 * Signature: (J)V
 */
void JNICALL Java_org_apache_hadoop_mapred_nativetask_NativeBatchProcessor_setupHandler
  (JNIEnv * jenv, jobject processor, jlong handler) {
  try {
    //LOG("native setup handler %llu", handler);
    Hadoop::BatchHandler * batchHandler = (Hadoop::BatchHandler *)((void*)handler);
    if (NULL==batchHandler) {
      JNU_ThrowByName(jenv, "java/lang/IllegalArgumentException", "BatchHandler is null");
      return;
    }
    jobject jinputBuffer  = jenv->GetObjectField(processor, InputBufferFieldID);
    if (NULL == jinputBuffer) {
      THROW_EXCEPTION(Hadoop::IOException, "BatchHandler input buffer null");
    }
    jobject joutputBuffer = jenv->GetObjectField(processor, OutputBufferFieldID);
    char * inputBufferAddr = (char*)(jenv->GetDirectBufferAddress(jinputBuffer));
    uint32_t inputBufferCapacity = jenv->GetDirectBufferCapacity(jinputBuffer);
    char * outputBufferAddr = NULL;
    uint32_t outputBufferCapacity = 0;
    if (NULL != joutputBuffer) {
      outputBufferAddr = (char*)(jenv->GetDirectBufferAddress(joutputBuffer));
      outputBufferCapacity = jenv->GetDirectBufferCapacity(joutputBuffer);
    }
    batchHandler->onSetup(inputBufferAddr, inputBufferCapacity, outputBufferAddr, outputBufferCapacity);
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
  catch (Hadoop::JavaException e) {
    LOG("JavaException: %s", e.what());
    // Do nothing, let java side handle
  }
  catch (std::exception e) {
    JNU_ThrowByName(jenv, "java/io/IOException", e.what());
  }
  catch (...) {
    JNU_ThrowByName(jenv, "java/io/IOException", "Unknown exception");
  }
}

/*
 * Class:     org_apache_hadoop_mapred_nativetask_NativeBatchProcessor
 * Method:    nativeProcessInput
 * Signature: (JI)V
 */
void JNICALL Java_org_apache_hadoop_mapred_nativetask_NativeBatchProcessor_nativeProcessInput
  (JNIEnv * jenv, jobject processor, jlong handler, jint length) {
  try {
    //LOG("native process input %llu", handler);
    Hadoop::BatchHandler * batchHandler = (Hadoop::BatchHandler *)((void*)handler);
    if (NULL==batchHandler) {
      JNU_ThrowByName(jenv, "java/lang/IllegalArgumentException", "handler not instance of BatchHandler");
      return;
    }
    batchHandler->setTemps(jenv, processor);
    batchHandler->onInputData(length);
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
  catch (Hadoop::JavaException e) {
    LOG("JavaException: %s", e.what());
    // Do nothing, let java side handle
  }
  catch (std::exception e) {
    JNU_ThrowByName(jenv, "java/io/IOException", e.what());
  }
  catch (...) {
    JNU_ThrowByName(jenv, "java/io/IOException", "Unknown exception");
  }
}

/*
 * Class:     org_apache_hadoop_mapred_nativetask_NativeBatchProcessor
 * Method:    nativeFinish
 * Signature: (J)V
 */
void JNICALL Java_org_apache_hadoop_mapred_nativetask_NativeBatchProcessor_nativeFinish
  (JNIEnv * jenv, jobject processor, jlong handler) {
  try {
    //LOG("native finish %llu", handler);
    Hadoop::BatchHandler * batchHandler = (Hadoop::BatchHandler *)((void*)handler);
    if (NULL==batchHandler) {
      JNU_ThrowByName(jenv, "java/lang/IllegalArgumentException", "handler not instance of BatchHandler");
      return;
    }
    batchHandler->setTemps(jenv, processor);
    batchHandler->onFinish();
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
  catch (Hadoop::JavaException e) {
    LOG("JavaException: %s", e.what());
    // Do nothing, let java side handle
  }
  catch (std::exception e) {
    JNU_ThrowByName(jenv, "java/io/IOException", e.what());
  }
  catch (...) {
    JNU_ThrowByName(jenv, "java/io/IOException", "Unknown exception");
  }
}

/*
 * Class:     org_apache_hadoop_mapred_nativetask_NativeBatchProcessor
 * Method:    nativeCommand
 * Signature: (J[B)[B
 */
jbyteArray JNICALL Java_org_apache_hadoop_mapred_nativetask_NativeBatchProcessor_nativeCommand
  (JNIEnv * jenv, jobject processor, jlong handler, jbyteArray cmdData) {
  try {
    Hadoop::BatchHandler * batchHandler = (Hadoop::BatchHandler *)((void*)handler);
    if (NULL==batchHandler) {
      JNU_ThrowByName(jenv, "java/lang/IllegalArgumentException", "handler not instance of BatchHandler");
      return NULL;
    }
    batchHandler->setTemps(jenv, processor);
    std::string cmdDataStr = JNU_ByteArrayToString(jenv, cmdData);
    std::string retString = batchHandler->onCommand(cmdDataStr);
    if (batchHandler->hasJavaException()) {
      return NULL;
    }
    jbyteArray ret = jenv->NewByteArray(retString.length());
    jenv->SetByteArrayRegion(ret, 0, retString.length(), (jbyte*)retString.c_str());
    return ret;
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
  catch (Hadoop::JavaException e) {
    LOG("JavaException: %s", e.what());
    // Do nothing, let java side handle
  }
  catch (std::exception e) {
    JNU_ThrowByName(jenv, "java/io/IOException", e.what());
  }
  catch (...) {
    JNU_ThrowByName(jenv, "java/io/IOException", "Unknown exception");
  }
  return NULL;
}

/*
 * Class:     org_apace_hadoop_mapred_nativetask_NativeBatchProcessor
 * Method:    InitIDs
 * Signature: ()V
 */
void JNICALL Java_org_apache_hadoop_mapred_nativetask_NativeBatchProcessor_InitIDs
  (JNIEnv * jenv, jclass processorClass) {
  InputBufferFieldID   = jenv->GetFieldID(processorClass, "inputBuffer", "Ljava/nio/ByteBuffer;");
  OutputBufferFieldID  = jenv->GetFieldID(processorClass, "outputBuffer", "Ljava/nio/ByteBuffer;");
  FlushOutputMethodID  = jenv->GetMethodID(processorClass, "flushOutput", "(I)V");
  FinishOutputMethodID = jenv->GetMethodID(processorClass,"finishOutput", "()V");
  SendCommandToJavaMethodID  = jenv->GetMethodID(processorClass, "sendCommandToJava", "([B)[B");
}

