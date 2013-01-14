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

#include <algorithm>
#include "commons.h"
#include "BufferStream.h"
#include "FileSystem.h"
#include "IFile.h"
#include "KVFile.h"
#include "test_commons.h"

IndexRange * writeIFile(int partition,
                             vector<pair<string, string> > & kvs,
                             const string & path,
                             KeyValueType type,
                             const string & codec) {
  FileOutputStream * fout = (FileOutputStream*)FileSystem::getLocal().create(path);
  IFileWriter * iw = new IFileWriter(fout, CHECKSUM_CRC32, type, type, codec);
  for (int i=0;i<partition;i++) {
    iw->startPartition();
    for (size_t i = 0;i<kvs.size();i++) {
      pair<string, string> & p = kvs[i];
      iw->write(p.first.c_str(), p.first.length(),
                p.second.c_str(), p.second.length());
    }
    iw->endPartition();
  }
  IndexRange * info = iw->getIndex(0);
  delete iw;
  delete fout;
  return info;
}

void readIFile(vector<pair<string, string> > & kvs, const string & path,
                KeyValueType type, IndexRange * info, const string & codec) {
  FileInputStream * fin = (FileInputStream*) FileSystem::getLocal().open(path);
  IFileReader * ir = new IFileReader(fin, CHECKSUM_CRC32, type, type, info, codec);
  size_t count = 0;
  while (ir->nextPartition() == 0) {
    const char * key, *value;
    uint32_t keyLen, valueLen;
    while (NULL != (key = ir->nextKey(keyLen))) {
      value = ir->value(valueLen);
      string keyS(key, keyLen);
      string valueS(value, valueLen);
      kvs.push_back(std::make_pair(keyS, valueS));
    }
  }
  delete ir;
  delete fin;
}

void TestIFileReadWrite(KeyValueType kvtype, int partition, int size,
                        vector<pair<string, string> > & kvs, const string & codec="") {
  string outputpath = "ifilewriter";
  IndexRange * info = writeIFile(partition, kvs, outputpath, kvtype, codec);
  LOG("write finished");
  vector<pair<string, string> > readkvs;
  readIFile(readkvs, outputpath, kvtype, info, codec);
  LOG("read finished");
  ASSERT_EQ(kvs.size()*partition, readkvs.size());
  for (int i=0;i<partition;i++) {
    vector<pair<string, string> > cur_part(readkvs.begin()+i*kvs.size(),
                                           readkvs.begin()+(i+1)*kvs.size());
    ASSERT_EQ(kvs.size(), cur_part.size());
//    for (size_t j=0;j<kvs.size();j++) {
//      SCOPED_TRACE(j);
//      ASSERT_EQ(kvs[j], cur_part[j]);
//    }
    ASSERT_EQ(kvs, cur_part);
  }
  FileSystem::getLocal().remove(outputpath);
}


TEST(IFile, WriteRead) {
  int partition = TestConfig.getInt("ifile.partition", 7);
  int size = TestConfig.getInt("partition.size", 20000);
  vector<pair<string, string> > kvs;
  Generate(kvs, size, "bytes");
  TestIFileReadWrite(TextType, partition, size, kvs);
  TestIFileReadWrite(BytesType, partition, size, kvs);
  TestIFileReadWrite(UnknownType, partition, size, kvs);
  TestIFileReadWrite(TextType, partition, size, kvs, "org.apache.hadoop.io.compress.SnappyCodec");
}



void TestIFileWriteRead2(vector<pair<string, string> > & kvs, char * buff,
                         size_t buffsize, const string & codec,
                         ChecksumType checksumType, KeyValueType type) {
  int partition = TestConfig.getInt("ifile.partition", 50);
  Timer timer;
  OutputBuffer outputBuffer = OutputBuffer(buff, buffsize);
  IFileWriter * iw = new IFileWriter(&outputBuffer, checksumType, type, type, codec);
  timer.reset();
  for (int i=0;i<partition;i++) {
    iw->startPartition();
    for (size_t j=0;j<kvs.size();j++) {
      iw->write(kvs[j].first.c_str(), kvs[j].first.length(),
                kvs[j].second.c_str(), kvs[j].second.length());
    }
    iw->endPartition();
  }
  IndexRange * info = iw->getIndex(0);
  LOG("%s", timer.getSpeedM2("Write data", info->getEndPosition(),
          info->getRealEndPosition()).c_str());
  delete iw;

  InputBuffer inputBuffer = InputBuffer(buff, outputBuffer.tell());
  IFileReader * ir = new IFileReader(&inputBuffer, checksumType, type, type, info, codec);
  timer.reset();
  while (0 == ir->nextPartition()) {
    const char * key, * value;
    uint32_t keyLen, valueLen;
    while (NULL != (key=ir->nextKey(keyLen))) {
      value = ir->value(valueLen);
    }
  }
  LOG("%s", timer.getSpeedM2(" Read data", info->getEndPosition(),
          info->getRealEndPosition()).c_str());
  delete ir;
  delete info;
}

void TestKVFileWriteRead2(vector<pair<string, string> > & kvs, char * buff,
                         size_t buffsize, const string & codec,
                         ChecksumType checksumType, KeyValueType type) {
  int partition = TestConfig.getInt("ifile.partition", 50);
  Timer timer;
  OutputBuffer outputBuffer = OutputBuffer(buff, buffsize);
  KVFileWriter * iw = new KVFileWriter(&outputBuffer, checksumType, codec);
  timer.reset();
  for (int i=0;i<partition;i++) {
    iw->startPartition();
    for (size_t j=0;j<kvs.size();j++) {
      iw->write(kvs[j].first.c_str(), kvs[j].first.length(),
                kvs[j].second.c_str(), kvs[j].second.length());
    }
    iw->endPartition();
  }
  IndexRange * info = iw->getIndex(0);
  LOG("%s", timer.getSpeedM2("Write data", info->getEndPosition(),
          info->getRealEndPosition()).c_str());
  delete iw;

  InputBuffer inputBuffer = InputBuffer(buff, outputBuffer.tell());
  KVFileReader * ir = new KVFileReader(&inputBuffer, checksumType, info, codec);
  timer.reset();
  while (0 == ir->nextPartition()) {
    const char * key, * value;
    uint32_t keyLen, valueLen;
    while (NULL != (key=ir->nextKey(keyLen))) {
      value = ir->value(valueLen);
    }
  }
  LOG("%s", timer.getSpeedM2(" Read data", info->getEndPosition(),
          info->getRealEndPosition()).c_str());
  delete ir;
  delete info;
}

TEST(Perf, IFile) {
  int size = TestConfig.getInt("partition.size", 20000);
  string codec = TestConfig.get("ifile.codec", "");
  string type = TestConfig.get("ifile.type", "bytes");

  vector<pair<string, string> > kvs;
  Generate(kvs, size, type);
  std::sort(kvs.begin(), kvs.end());

  size_t buffsize = 200*1024*1024;
  char * buff = new char[buffsize];
  memset(buff, 0, buffsize);

  LOG("Test TextType CRC32");
  TestIFileWriteRead2(kvs, buff, buffsize, codec, CHECKSUM_CRC32, TextType);
  LOG("Test BytesType CRC32");
  TestIFileWriteRead2(kvs, buff, buffsize, codec, CHECKSUM_CRC32, BytesType);
  LOG("Test UnknownType CRC32");
  TestIFileWriteRead2(kvs, buff, buffsize, codec, CHECKSUM_CRC32, UnknownType);
  LOG("Test KVFile CRC32");
  TestKVFileWriteRead2(kvs, buff, buffsize, codec, CHECKSUM_CRC32, UnknownType);

  LOG("Test TextType CRC32C");
  TestIFileWriteRead2(kvs, buff, buffsize, codec, CHECKSUM_CRC32C, TextType);
  LOG("Test BytesType CRC32C");
  TestIFileWriteRead2(kvs, buff, buffsize, codec, CHECKSUM_CRC32C, BytesType);
  LOG("Test UnknownType CRC32C");
  TestIFileWriteRead2(kvs, buff, buffsize, codec, CHECKSUM_CRC32C, UnknownType);
  LOG("Test KVFile CRC32C");
  TestKVFileWriteRead2(kvs, buff, buffsize, codec, CHECKSUM_CRC32C, UnknownType);

  delete [] buff;
}
