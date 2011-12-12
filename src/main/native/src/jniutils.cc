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
#include "StringUtil.h"
#include "jniutils.h"

using namespace Hadoop;

void JNU_ThrowByName(JNIEnv *jenv, const char *name, const char *msg) {
  jclass cls = jenv->FindClass(name);
  if (cls != NULL) {
    jenv->ThrowNew(cls, msg);
  }
  jenv->DeleteLocalRef(cls);
}

std::string JNU_ByteArrayToString(JNIEnv * jenv, jbyteArray src) {
  if (NULL != src) {
    jsize len = jenv->GetArrayLength(src);
    std::string ret(len, '\0');
    jenv->GetByteArrayRegion(src, 0, len, (jbyte*)ret.data());
    return ret;
  }
  return std::string();
}

/**
 * getJNIEnv: A helper function to get the JNIEnv* for the given thread.
 * If no JVM exists, then one will be created. JVM command line arguments
 * are obtained from the LIBHDFS_OPTS environment variable.
 *
 * @param: None.
 * @return The JNIEnv* corresponding to the thread.
 */
JNIEnv* JNU_GetJNIEnv(void) {
  const jsize vmBufLength = 1;
  JavaVM* vmBuf[vmBufLength];
  JNIEnv *env;
  jint rv = 0;
  jint noVMs = 0;

  rv = JNI_GetCreatedJavaVMs(&(vmBuf[0]), vmBufLength, &noVMs);
  if (rv != 0) {
    THROW_EXCEPTION(Hadoop::HadoopException, "JNI_GetCreatedJavaVMs failed");
  }

  if (noVMs == 0) {
//    THROW_EXCEPTION(Hadoop::HadoopException, "Not in a JNI environment");
    char *hadoopClassPath = getenv("CLASSPATH");
    if (hadoopClassPath == NULL) {
      THROW_EXCEPTION(Hadoop::HadoopException, "Environment variable CLASSPATH not set!");
      return NULL;
    }
    const char *hadoopClassPathVMArg = "-Djava.class.path=";
    size_t optHadoopClassPathLen = strlen(hadoopClassPath)
        + strlen(hadoopClassPathVMArg) + 1;
    char *optHadoopClassPath = (char*)malloc(sizeof(char) * optHadoopClassPathLen);
    snprintf(optHadoopClassPath, optHadoopClassPathLen, "%s%s",
             hadoopClassPathVMArg, hadoopClassPath);

    int noArgs = 1;
    JavaVMOption options[noArgs];
    options[0].optionString = optHadoopClassPath;

    //Create the VM
    JavaVMInitArgs vm_args;
    JavaVM *vm;
    vm_args.version = JNI_VERSION_1_2;
    vm_args.options = options;
    vm_args.nOptions = noArgs;
    vm_args.ignoreUnrecognized = 1;

    rv = JNI_CreateJavaVM(&vm, (void**) &env, &vm_args);
    if (rv != 0) {
      THROW_EXCEPTION(Hadoop::HadoopException, "JNI_CreateJavaVM failed");
      return NULL;
    }
    free(optHadoopClassPath);
  }
  else {
    //Attach this thread to the VM
    JavaVM* vm = vmBuf[0];
    rv = vm->AttachCurrentThread((void **)&env, NULL);
    if (rv != 0) {
      THROW_EXCEPTION(Hadoop::HadoopException, "Call to AttachCurrentThread failed");
    }
  }

  return env;
}


jmethodID methodIdFromClass(const char *className, const char *methName,
                            const char *methSignature, bool isStatic,
                            JNIEnv *env) {
  jclass cls = env->FindClass(className);
  if (cls == NULL) {
    THROW_EXCEPTION_EX(Hadoop::HadoopException, "class not found: %s", className);
    return NULL;
  }

  jmethodID mid = 0;

  if (isStatic) {
      mid = env->GetStaticMethodID(cls, methName, methSignature);
  }
  else {
      mid = env->GetMethodID(cls, methName, methSignature);
  }
  if (mid == NULL) {
    THROW_EXCEPTION_EX(Hadoop::HadoopException, "method not found: %s", methName);
  }
  env->DeleteLocalRef(cls);
  return mid;
}
