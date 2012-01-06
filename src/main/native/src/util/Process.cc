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

#include <errno.h>
#include "commons.h"
#include "util/SyncUtils.h"
#include "BufferStream.h"
#include "Process.h"

namespace NativeTask {

void PipeInputStream::seek(uint64_t position) {
  THROW_EXCEPTION(IOException, "seek not support for Pipe");
}

uint64_t PipeInputStream::tell() {
  return _read;
}

int32_t PipeInputStream::read(void * buff, uint32_t length) {
  if (_fd >= 0) {
    int32_t rd = ::read(_fd, buff, length);
    if (rd > 0) {
      _read += rd;
    }
    return rd;
  }
  THROW_EXCEPTION(IOException, "Pipe closed in read()");
}

void PipeInputStream::close() {
  ::close(_fd);
  _fd = -1;
}

uint64_t PipeOutputStream::tell() {
  return _written;
}

void PipeOutputStream::write(const void * buff, uint32_t length) {
  if (_fd >= 0) {
    ssize_t wt = ::write(_fd, buff, length);
    if (wt != length) {
      THROW_EXCEPTION(IOException, "write pipe error");
    }
    _written += wt;
  }
  THROW_EXCEPTION(IOException, "Pipe closed in write()");
}

void PipeOutputStream::flush() {
}

void PipeOutputStream::close() {
  ::close(_fd);
  _fd = -1;
}

int Process::Popen(const string & cmd,
                   int & fdstdin,
                   int & fdstdout,
                   int & fdstderr) {
  int outfd[2];
  int infd[2];
  int errfd[2];

  pipe(outfd);
  pipe(infd);
  pipe(errfd);

  int pid = fork();
  if (0 == pid) {
    close(0);
    close(1);
    close(2);
    dup2(outfd[0], 0); // Make the read end of outfd pipe as stdin
    dup2(infd[1], 1); // Make the write end of infd as stdout
    dup2(errfd[1], 2); // stderr
    close(outfd[0]);
    close(outfd[1]);
    close(infd[0]);
    close(infd[1]);
    close(errfd[0]);
    close(errfd[1]);
    const char * args[] = { "/bin/sh", "-c", cmd.c_str(), NULL };
    int ret = execv(args[0], (char * const *) args);
    if (0 != ret) {
      LOG("Error when execv [%s] ret: %d, error: %s\n", cmd.c_str(), ret, strerror(errno));
      return -1;
    }
    return 0;
  }
  else {
    close(outfd[0]);
    close(infd[1]);
    close(errfd[1]);
    fdstdin = outfd[1];
    fdstdout = infd[0];
    fdstderr = errfd[0];
    return pid;
  }
}

int Process::Popen(const string & cmd,
                   FILE *& fstdin,
                   FILE *& fstdout,
                   FILE *& fstderr) {
  int fdstdin;
  int fdstdout;
  int fdstderr;
  int ret = Popen(cmd, fdstdin, fdstdout, fdstderr);
  if (ret<=0) {
    return ret;
  }
  fstdin = fdopen(fdstdin, "w");
  fstdout = fdopen(fdstdout, "r");
  fstderr = fdopen(fdstderr, "r");
  return ret;
}

int Process::Popen(const string & cmd,
                   PipeOutputStream & pstdin,
                   PipeInputStream & pstdout,
                   PipeInputStream & pstderr) {
  int fdstdin;
  int fdstdout;
  int fdstderr;
  int ret = Popen(cmd, fdstdin, fdstdout, fdstderr);
  if (ret<=0) {
    return ret;
  }
  pstdin.setFd(fdstdin);
  pstdout.setFd(fdstdout);
  pstderr.setFd(fdstderr);
  return ret;
}

int Process::Run(const string & cmd, string * out, string * err) {
  PipeOutputStream pin;
  PipeInputStream pout;
  PipeInputStream perr;
  int pid = Popen(cmd, pin, pout, perr);
  if (pid<0) {
    return pid;
  }
  pin.close();
  OutputStream * sout = NULL;
  if (out == NULL) {
    sout = new OutputStream();
  } else {
    sout = new OutputStringStream(*out);
  }
  OutputStream * serr = NULL;
  if (err == NULL) {
    serr = new OutputStream();
  } else {
    serr = new OutputStringStream(*err);
  }
  Thread errThread = Thread(Bind(perr, &InputStream::readAllTo, *serr, 4096));
  errThread.start();
  pout.readAllTo(*sout, 4096);
  errThread.join();
  int retcode = 0;
  if (pid != waitpid(pid, &retcode, 0)) {
    retcode = -1;
  }
  delete sout;
  delete serr;
  return retcode;
}


} // namespace NativeTask


