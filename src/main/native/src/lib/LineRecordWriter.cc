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

#include "commons.h"
#include "Compressions.h"
#include "FileSystem.h"
#include "LineRecordWriter.h"

namespace Hadoop {

LineRecordWriter::LineRecordWriter() :
  _stream(NULL),
  _bufferHint(128*1024),
  _hasStream(false),
  _keyValueSeparator("\t") {
}

void LineRecordWriter::init(OutputStream * stream, const string & codec) {
  close();
  _stream = stream;
  _hasStream = false;
  _appendBuffer.init(_bufferHint, stream, codec);
}

void LineRecordWriter::init(const string & file, Config & config) {
  close();
  _stream = FileSystem::getRaw().create(file);
  _hasStream = true;
  string codec = Compressions::getCodecByFile(file);
  _appendBuffer.init(_bufferHint, _stream, codec);
}

LineRecordWriter::~LineRecordWriter() {
  close();
}

void LineRecordWriter::configure(Config & config) {
  _keyValueSeparator = config.get("mapred.textoutputformat.separator", "\t");
  bool isCompress = config.getBool("mapred.output.compress", false);
  const char * workdir = config.get("mapred.work.output.dir");
  if (workdir == NULL) {
    THROW_EXCEPTION(IOException, "Can not find mapred.work.output.dir for LineRecordWriter");
  }
  const char * outputname = config.get("native.output.file.name");
  if (outputname == NULL) {
    THROW_EXCEPTION(IOException, "Can not find native.output.file.name for LineRecordWriter");
  }
  if (isCompress) {
    string codec = config.get("mapred.output.compression.codec", Compressions::GzipCodec.name);
    string ext = Compressions::getExtension(codec);
    string outputpath = StringUtil::Format("%s/%s%s", workdir, outputname, ext.c_str());
    init(outputpath, config);
  } else {
    string outputpath = StringUtil::Format("%s/%s", workdir, outputname);
    init(outputpath, config);
  }
}

void LineRecordWriter::collect(const void * key, uint32_t keyLen,
                             const void * value, uint32_t valueLen) {
  if (keyLen > 0) {
    _appendBuffer.write(key, keyLen);
    if (valueLen > 0) {
      if (_keyValueSeparator.length() > 0) {
        _appendBuffer.write(_keyValueSeparator.c_str(),
                            _keyValueSeparator.length());
      }
      _appendBuffer.write(value, valueLen);
    }
  } else if (valueLen>0) {
    _appendBuffer.write(value, valueLen);
  }
  _appendBuffer.write('\n');
}

void LineRecordWriter::close() {
  if (_stream != NULL) {
    _appendBuffer.flush();
  }
  if (_hasStream) {
    delete _stream;
  }
  _stream = NULL;
}

} // namespace Hadoop

