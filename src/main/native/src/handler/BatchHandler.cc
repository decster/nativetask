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

#ifndef QUICK_BUILD
#include "org_apache_hadoop_mapred_nativetask_NativeBatchProcessor.h"
#endif
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

namespace NativeTask {

BatchHandler::BatchHandler() :
  _processor(NULL) {
}

BatchHandler::~BatchHandler() {
  releaseProcessor();
}

void BatchHandler::releaseProcessor() {
  if (_processor != NULL) {
    JNIEnv * env = JNU_GetJNIEnv();
    env->DeleteGlobalRef((jobject)_processor);
    _processor = NULL;
  }
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
  JNIEnv * env = JNU_GetJNIEnv();
  env->CallVoidMethod((jobject)_processor,
      FlushOutputMethodID, (jint) length);
  if (env->ExceptionCheck()) {
    THROW_EXCEPTION(JavaException, "FlushOutput throw exception");
  }
}

void BatchHandler::finishOutput() {
  if (NULL == _ob.buff) {
    return;
  }
  JNIEnv * env = JNU_GetJNIEnv();
  env->CallVoidMethod((jobject)_processor,
      FinishOutputMethodID);
  if (env->ExceptionCheck()) {
    THROW_EXCEPTION(JavaException, "FinishOutput throw exception");
  }
}

void BatchHandler::onSetup(char * inputBuffer, uint32_t inputBufferCapacity,
    char * outputBuffer, uint32_t outputBufferCapacity) {
  _ib.reset(inputBuffer, inputBufferCapacity);
  if (NULL != outputBuffer) {
    if (outputBufferCapacity <= 1024) {
      THROW_EXCEPTION(IOException, "Output buffer size too small for BatchHandler");
    }
    _ob.reset(outputBuffer, outputBufferCapacity - sizeof(uint64_t));
  }
  configure(NativeObjectFactory::GetConfig());
}

std::string BatchHandler::sendCommand(const std::string & data) {
  JNIEnv * env = JNU_GetJNIEnv();
  jbyteArray jcmdData = env->NewByteArray(data.length());
  env->SetByteArrayRegion(jcmdData, 0,
      (jsize) data.length(), (jbyte*) data.c_str());
  jbyteArray ret = (jbyteArray) env->CallObjectMethod(
      (jobject) _processor, SendCommandToJavaMethodID, jcmdData);
  if (env->ExceptionCheck()) {
    THROW_EXCEPTION(JavaException, "SendCommandToJava throw exception");
  }
  return JNU_ByteArrayToString(env, ret);
}

} // namespace NativeTask


///////////////////////////////////////////////////////////////
// NativeBatchProcessor jni methods
///////////////////////////////////////////////////////////////
using namespace NativeTask;

/*
 * Class:     org_apache_hadoop_mapred_nativetask_NativeBatchProcessor
 * Method:    setupHandler
 * Signature: (J)V
 */
void JNICALL Java_org_apache_hadoop_mapred_nativetask_NativeBatchProcessor_setupHandler
  (JNIEnv * jenv, jobject processor, jlong handler) {
  try {
    NativeTask::BatchHandler * batchHandler = (NativeTask::BatchHandler *)((void*)handler);
    if (NULL==batchHandler) {
      JNU_ThrowByName(jenv, "java/lang/IllegalArgumentException", "BatchHandler is null");
      return;
    }
    jobject jinputBuffer  = jenv->GetObjectField(processor, InputBufferFieldID);
    char * inputBufferAddr = NULL;
    uint32_t inputBufferCapacity = 0;
    if (NULL != jinputBuffer) {
      inputBufferAddr = (char*)(jenv->GetDirectBufferAddress(jinputBuffer));
      inputBufferCapacity = jenv->GetDirectBufferCapacity(jinputBuffer);
    }
    jobject joutputBuffer = jenv->GetObjectField(processor, OutputBufferFieldID);
    char * outputBufferAddr = NULL;
    uint32_t outputBufferCapacity = 0;
    if (NULL != joutputBuffer) {
      outputBufferAddr = (char*)(jenv->GetDirectBufferAddress(joutputBuffer));
      outputBufferCapacity = jenv->GetDirectBufferCapacity(joutputBuffer);
    }
    batchHandler->setProcessor(jenv->NewGlobalRef(processor));
    batchHandler->onSetup(inputBufferAddr, inputBufferCapacity, outputBufferAddr, outputBufferCapacity);
  }
  catch (NativeTask::UnsupportException e) {
    JNU_ThrowByName(jenv, "java/lang/UnsupportedOperationException", e.what());
  }
  catch (NativeTask::OutOfMemoryException e) {
    JNU_ThrowByName(jenv, "java/lang/OutOfMemoryException", e.what());
  }
  catch (NativeTask::IOException e) {
    JNU_ThrowByName(jenv, "java/io/IOException", e.what());
  }
  catch (NativeTask::JavaException e) {
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
    NativeTask::BatchHandler * batchHandler = (NativeTask::BatchHandler *)((void*)handler);
    if (NULL==batchHandler) {
      JNU_ThrowByName(jenv, "java/lang/IllegalArgumentException", "handler not instance of BatchHandler");
      return;
    }
    batchHandler->onInputData(length);
  }
  catch (NativeTask::UnsupportException e) {
    JNU_ThrowByName(jenv, "java/lang/UnsupportedOperationException", e.what());
  }
  catch (NativeTask::OutOfMemoryException e) {
    JNU_ThrowByName(jenv, "java/lang/OutOfMemoryException", e.what());
  }
  catch (NativeTask::IOException e) {
    JNU_ThrowByName(jenv, "java/io/IOException", e.what());
  }
  catch (NativeTask::JavaException e) {
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
    NativeTask::BatchHandler * batchHandler = (NativeTask::BatchHandler *)((void*)handler);
    if (NULL==batchHandler) {
      JNU_ThrowByName(jenv, "java/lang/IllegalArgumentException", "handler not instance of BatchHandler");
      return;
    }
    batchHandler->onFinish();
  }
  catch (NativeTask::UnsupportException e) {
    JNU_ThrowByName(jenv, "java/lang/UnsupportedOperationException", e.what());
  }
  catch (NativeTask::OutOfMemoryException e) {
    JNU_ThrowByName(jenv, "java/lang/OutOfMemoryException", e.what());
  }
  catch (NativeTask::IOException e) {
    JNU_ThrowByName(jenv, "java/io/IOException", e.what());
  }
  catch (NativeTask::JavaException e) {
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
    NativeTask::BatchHandler * batchHandler = (NativeTask::BatchHandler *)((void*)handler);
    if (NULL==batchHandler) {
      JNU_ThrowByName(jenv, "java/lang/IllegalArgumentException", "handler not instance of BatchHandler");
      return NULL;
    }
    std::string cmdDataStr = JNU_ByteArrayToString(jenv, cmdData);
    std::string retString = batchHandler->onCommand(cmdDataStr);
    jbyteArray ret = jenv->NewByteArray(retString.length());
    jenv->SetByteArrayRegion(ret, 0, retString.length(), (jbyte*)retString.c_str());
    return ret;
  }
  catch (NativeTask::UnsupportException e) {
    JNU_ThrowByName(jenv, "java/lang/UnsupportedOperationException", e.what());
  }
  catch (NativeTask::OutOfMemoryException e) {
    JNU_ThrowByName(jenv, "java/lang/OutOfMemoryException", e.what());
  }
  catch (NativeTask::IOException e) {
    JNU_ThrowByName(jenv, "java/io/IOException", e.what());
  }
  catch (NativeTask::JavaException e) {
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

