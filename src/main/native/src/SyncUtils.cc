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

#include <tr1/functional>
#include "commons.h"
#include "SyncUtils.h"

namespace Hadoop {

static void PthreadCall(const char* label, int result) {
  if (result != 0) {
    THROW_EXCEPTION_EX(IOException, "pthread %s: %s", label, strerror(result));
  }
}

Lock::Lock() {
  PthreadCall("init mutex", pthread_mutex_init(&_mutex, NULL));
}

Lock::~Lock() {
  PthreadCall("destroy mutex", pthread_mutex_destroy(&_mutex));
}

void Lock::lock() {
  PthreadCall("lock", pthread_mutex_lock(&_mutex));
}

void Lock::unlock() {
  PthreadCall("unlock", pthread_mutex_unlock(&_mutex));
}


#ifdef __MACH__
  SpinLock::SpinLock() : _spin(0) {
  }

  SpinLock::~SpinLock() {

  }

  void SpinLock::lock() {
    OSSpinLockLock(&_spin);
  }

  void SpinLock::unlock() {
    OSSpinLockUnlock(&_spin);
  }
#else
  SpinLock::SpinLock() {
    PthreadCall("init mutex", pthread_spin_init(&_spin, NULL));
  }

  SpinLock::~SpinLock() {
    PthreadCall("destroy mutex", pthread_spin_destroy(&_spin));
  }

  void SpinLock::lock() {
    PthreadCall("lock", pthread_spin_lock(&_spin));
  }

  void SpinLock::unlock() {
    PthreadCall("unlock", pthread_spin_unlock(&_spin));
  }
#endif


Condition::Condition(Lock* mu)
    : _lock(mu) {
    PthreadCall("init cv", pthread_cond_init(&_condition, NULL));
}

Condition::~Condition() {
  PthreadCall("destroy cv", pthread_cond_destroy(&_condition));
}

void Condition::wait() {
  PthreadCall("wait", pthread_cond_wait(&_condition, &_lock->_mutex));
}

void Condition::signal() {
  PthreadCall("signal", pthread_cond_signal(&_condition));
}

void Condition::signalAll() {
  PthreadCall("broadcast", pthread_cond_broadcast(&_condition));
}

void * Thread::ThreadRunner(void * pthis) {
  ((Thread*)pthis)->run();
  return NULL;
}

Thread::Thread() :
  _thread((pthread_t)0), // safe for linux & macos
  _runable(NULL) {
}

Thread::Thread(const Runnable & runnable) :
  _thread((pthread_t)0),
  _runable(const_cast<Runnable*>(&runnable)) {
}

void Thread::setTask(const Runnable & runnable) {
  _runable = const_cast<Runnable*>(&runnable);
}

Thread::~Thread() {
}

void Thread::start() {
  PthreadCall("pthread_create", pthread_create(&_thread, NULL, ThreadRunner, this));
}

void Thread::join() {
  PthreadCall("pthread_join", pthread_join(_thread, NULL));
}

void Thread::stop() {
  PthreadCall("pthread_cancel", pthread_cancel(_thread));
}

void Thread::run() {
  if (_runable!=NULL) {
    _runable->run();
  }
}

} // namespace Hadoop
