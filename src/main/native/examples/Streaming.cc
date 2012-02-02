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
#include "Log.h"
#include "StringUtil.h"
#include "Process.h"
#include "NativeTask.h"
#include "SyncUtils.h"
#include "LineRecordReader.h"
#include "LineRecordWriter.h"

namespace Streaming {

using namespace NativeTask;

class StreamingReader : public LineRecordReader {
public:
  virtual bool next(Buffer & key, Buffer & value) {
    // TODO: only find EOL at split end
    uint32_t len = readLine(value, true);
    if (len == 0) {
      return false;
    }
    _pos += len;
    return true;
  }
};

class StreamingWriter : public LineRecordWriter {
public:
  virtual void collect(const void * key, uint32_t keyLen,
                       const void * value, uint32_t valueLen) {
    _appendBuffer.write(value, valueLen);
  }
};

class StreamingMapper : public Mapper {
protected:
  int pid;
  FILE * fstdin;
  FILE * fstdout;
  FILE * fstderr;
  Runnable * handleStdoutTask;
  Runnable * handleStderrTask;
  Thread stdoutThread;
  Thread stderrThread;
public:
  StreamingMapper() :
    handleStderrTask(NULL),
    handleStdoutTask(NULL) {
  }

  virtual ~StreamingMapper() {
    delete handleStdoutTask;
    handleStdoutTask = NULL;
    delete handleStderrTask;
    handleStderrTask = NULL;
  }

  virtual void close() {
    fclose(fstdin);
    stderrThread.join();
    stdoutThread.join();
    int retcode;
    if (pid != waitpid(pid, &retcode, 0)) {
      THROW_EXCEPTION(IOException, "waitpid failed");
    }
    if (retcode != 0) {
      THROW_EXCEPTION_EX(IOException,
                         "streaming sub-process return %d",
                         retcode);
    }
  }

  void handleStderr() {
    Thread::EnableJNI();
    const int buffLen = 4096;
    char buff[buffLen];
    while (fgets(buff, buffLen, fstderr) != NULL) {
      fputs(buff, stderr);
    }
    Thread::ReleaseJNI();
  }
};

class MStreamingMapper : public StreamingMapper {
protected:
  string mapOutputSeparator;
public:
  virtual void configure(Config & config) {
    mapOutputSeparator = config.get("native.stream.map.output.separator", "\t");
    string cmd = config.get("native.map.processor", "cat");
    LOG("Streaming map processor: [%s]", cmd.c_str());
    pid = Process::Popen(cmd, fstdin, fstdout, fstderr);
    if (pid <= 0) {
      THROW_EXCEPTION_EX(IOException, "Create subprocess failed, cmd: %s", cmd.c_str());
    }
    handleStdoutTask = BindNew(*this, &MStreamingMapper::handleStdout);
    stdoutThread.setTask(*handleStdoutTask);
    handleStderrTask = BindNew(*this, &StreamingMapper::handleStderr);
    stderrThread.setTask(*handleStderrTask);
    stdoutThread.start();
    stderrThread.start();
  }

  virtual void map(const char * key, uint32_t keyLen,
                   const char * value, uint32_t valueLen) {
    if (valueLen > 0) {
      if (1 != ::fwrite(value, valueLen, 1, fstdin)) {
        THROW_EXCEPTION_EX(IOException, "fwrite to pipe error: %s",
                           strerror(errno));
      }
    }
  }

  void handleStdout() {
    Thread::EnableJNI();
    PipeInputStream inputStream(fileno(fstdout));
    DynamicBuffer lineBuffer;
    lineBuffer.reserve(4096);
    Buffer line;
    while (true) {
      uint32_t len = LineRecordReader::ReadLine(&inputStream, lineBuffer, line,
                                                4096, false);
      if (len<=0) {
        break;
      }
      uint32_t i = 0;
      for (; i < line.length(); i++) {
        if (line.data()[i] == mapOutputSeparator[0]) {
          collect(line.data(), i, line.data() + i + 1, line.length() - i - 1);
        }
      }
      if (i == line.length()) {
        collect(line.data(), line.length(), line.data() + line.length(), 0);
      }
    }
    Thread::ReleaseJNI();
  }
};

class RStreamingMapper : public StreamingMapper {
protected:
  string reduceInputSeparator;
public:
  virtual void configure(Config & config) {
    reduceInputSeparator = config.get("native.stream.reduce.input.separator",
                                      "\t");
    string cmd = config.get("native.reduce.processor", "cat");
    LOG("Streaming reduce processor: [%s]", cmd.c_str());
    pid = Process::Popen(cmd, fstdin, fstdout, fstderr);
    if (pid <= 0) {
      THROW_EXCEPTION_EX(IOException,
                         "Create subprocess failed, cmd: %s",
                         cmd.c_str());
    }
    handleStdoutTask = BindNew(*this, &RStreamingMapper::handleStdout);
    stdoutThread.setTask(*handleStdoutTask);
    handleStderrTask = BindNew(*this, &StreamingMapper::handleStderr);
    stderrThread.setTask(*handleStderrTask);
    stdoutThread.start();
    stderrThread.start();
  }

  virtual void map(const char * key, uint32_t keyLen,
                   const char * value, uint32_t valueLen) {
    if (keyLen > 0) {
      if (1 != ::fwrite(key, keyLen, 1, fstdin)) {
        THROW_EXCEPTION_EX(IOException, "fwrite to pipe error: %s",
                           strerror(errno));
      }
    }
    if (valueLen > 0) {
      if (keyLen>0) {
        if (EOF == fputc(reduceInputSeparator[0], fstdin)) {
          THROW_EXCEPTION_EX(IOException, "fwrite to pipe error: %s",
                             strerror(errno));
        }
      }
      if (1 != ::fwrite(value, valueLen, 1, fstdin)) {
        THROW_EXCEPTION_EX(IOException, "fwrite to pipe error: %s",
                           strerror(errno));
      }
    }
    if (EOF == fputc('\n', fstdin)) {
      THROW_EXCEPTION_EX(IOException, "fwrite to pipe error: %s",
                         strerror(errno));
    }
  }

  void handleStdout() {
    Thread::EnableJNI();
    int fd = fileno(fstdout);
    char buff[4096];
    while (true) {
      ssize_t rd = ::read(fd, buff, 4096);
      if (rd <= 0) {
        if (rd < 0) {
          THROW_EXCEPTION_EX(
              IOException,
              "Read from pipe failed, error: %s",
              strerror(errno));
        }
        break;
      }
      collect(NULL, 0, buff, rd);
    }
    Thread::ReleaseJNI();
  }
};

} // namespace Streaming

using namespace Streaming;

DEFINE_NATIVE_LIBRARY(Streaming) {
  REGISTER_CLASS(StreamingReader, Streaming);
  REGISTER_CLASS(StreamingWriter, Streaming);
  REGISTER_CLASS(MStreamingMapper, Streaming);
  REGISTER_CLASS(RStreamingMapper, Streaming);
}
