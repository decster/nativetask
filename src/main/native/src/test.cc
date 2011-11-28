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

/////////////////////////////////////////////////////////////////
// unittests & test/debug tools
/////////////////////////////////////////////////////////////////

#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <stdio.h>
#include <time.h>


#ifdef __MACH__
#include <mach/clock.h>
#include <mach/mach.h>

void clock_gettime(timespec*ts){
  clock_serv_t cclock;
  mach_timespec_t mts;
  host_get_clock_service(mach_host_self(), CALENDAR_CLOCK, &cclock);
  clock_get_time(cclock, &mts);
  mach_port_deallocate(mach_task_self(), cclock);
  ts->tv_sec = mts.tv_sec;
  ts->tv_nsec = mts.tv_nsec;
}

#define TIMED(name, func) \
  {timespec s,e; \
  clock_gettime(&s); \
  func \
  clock_gettime(&e); \
  fprintf(stderr, "%s time: %.5lf\n", name, \
      (e.tv_sec-s.tv_sec) + (double)(e.tv_nsec-s.tv_nsec)/1000000000.0);}

#else

#define TIMED(name, func) \
  {timespec s,e; \
  clock_gettime(_POSIX_CPUTIME, &s); \
  func \
  clock_gettime(_POSIX_CPUTIME, &e); \
  fprintf(stderr, "%s time: %.5lf\n", name, \
      (e.tv_sec-s.tv_sec) + (double)(e.tv_nsec-s.tv_nsec)/1000000000.0);}

#endif

#include <set>
#include "NativeObjectFactory.h"
#include "MapOutputCollector.h"
#include "fileutils.h"
#include "merge.h"

namespace Hadoop {
using std::string;
using std::vector;

/**
 * IFileIndex stuff, just used for test data verification
 * internally SpillSegmentInfo are used
 */
struct IFileIndexEntry {
  uint64_t start;
  uint64_t raw_length;
  uint64_t part_length;
};

class IFileIndex {
public:
  uint32_t partition;
  IFileIndexEntry * entries;
  std::string filepath;

  IFileIndex() : partition(0), entries(NULL) {
  }

  ~IFileIndex() {
    delete [] entries;
    entries = NULL;
  }

  static IFileIndex * create(const char * filepath) {
    struct stat st;
    if (0 == stat(filepath, &st)) {
      size_t size = (st.st_size - sizeof(uint32_t));
      if (size%sizeof(IFileIndexEntry)==0) {
        size /= sizeof(IFileIndexEntry);
        FILE * fin = fopen(filepath, "rb");
        if (NULL != fin) {
          IFileIndexEntry * entries = new IFileIndexEntry[size];
          size_t rd = fread(entries, sizeof(IFileIndexEntry)*size, 1, fin);
          uint32_t chsum_verify;
          size_t rdc = fread(&chsum_verify, sizeof(uint32_t), 1, fin);
          fclose(fin);
          if ((rd != 1) || (rdc != 1)) {
            delete [] entries;
            THROW_EXCEPTION(IOException, "Read IFileIndexFile error");
          }
          uint32_t chsum = checksum_init();
          checksum_update(chsum, entries, sizeof(IFileIndexEntry)*size);
          chsum = checksum_val(chsum);
          chsum_verify = bswap(chsum_verify);
          if (chsum_verify != chsum) {
            LOG("checksum %08x != %08x", chsum, chsum_verify);
            delete [] entries;
            THROW_EXCEPTION(IOException, "Read IFileIndexFile checksum verify error");
          }
          for (size_t i = 0 ; i < size; i++) {
            entries[i].start = bswap64(entries[i].start);
            entries[i].raw_length = bswap64(entries[i].raw_length);
            entries[i].part_length = bswap64(entries[i].part_length);
          }
          IFileIndex * ret = new IFileIndex();
          ret->partition = (uint32_t)size;
          ret->entries = entries;
          ret->filepath = filepath;
          return ret;
        }
      }
    }
    THROW_EXCEPTION(IOException, "Read IFileIndexFile failed");
  }

  SpillRangeInfo * to_spill_range_info(std::string spill_file_name) {
    SpillSegmentInfo * infos = new SpillSegmentInfo[partition];
    for (size_t i = 0 ; i < partition; i++) {
      infos[i].end_position =
          (i == 0 ? 0 : infos[i - 1].end_position)
          + entries[i].raw_length;
      infos[i].real_end_position =
          (i == 0 ? 0 : infos[i - 1].real_end_position)
           + entries[i].part_length;
    }
    SpillRangeInfo * ret = new SpillRangeInfo(0, partition, spill_file_name, infos);
    return ret;
  }
};



/**
 * K/v pair writer for debug & tests
 */
class DummyMapOutputWriter : public MapOutputWriter {
protected:
  FILE * _dump;
  int _partition;
  int _count;
public:
  DummyMapOutputWriter(FILE * dump):_dump(dump), _partition(-1),_count(0) {
  }

  virtual ~DummyMapOutputWriter() {
  }


  virtual void start_partition() {
    _partition ++;
    fprintf(_dump, "start partition %d\n", _partition);
  }

  virtual void end_partition() {
    fprintf(_dump, "end partition %d\n", _partition);
  }

  virtual void write_key_part(const char * key, uint32_t key_len, uint32_t value_len) {
    _count ++;
    fprintf(_dump, "key %08d: [", _count);
    fwrite(key, key_len, 1, _dump);
    fputc(']',_dump);
  }

  virtual void write_value_part(const char * value, uint32_t value_len) {
    fputc('[', _dump);
    fwrite(value, value_len, 1, _dump);
    fputc(']',_dump);
    fputc('\n',_dump);
  }
};


/**
 * generate k/v pairs with normal compression ratio
 *
 */
class KVGenerator {
protected:
  uint32_t keylen;
  uint32_t vallen;
  bool unique;
  long factor;
  char * keyb;
  char * valb;
  char keyformat[32];
  std::set<long> old_keys;

public:
  KVGenerator(uint32_t keylen, uint32_t vallen, bool unique=false) :
      keylen(keylen), vallen(vallen), unique(unique) {
    factor = 2999999;
    keyb = new char[keylen+32];
    valb = new char[vallen+32];
    snprintf(keyformat, 32, "%%0%ulx", keylen);
  }

  ~KVGenerator() {
    delete [] keyb;
    delete [] valb;
  }

  char * key(uint32_t & kl) {
    long v;
    if (unique) {
      while (true) {
        v = lrand48();
        if (old_keys.find(v) == old_keys.end()) {
          old_keys.insert(v);
          break;
        }
      }
    }
    else {
      v = lrand48();
    }
    snprintf(keyb, keylen+32, keyformat, v);
    kl = keylen;
    return keyb;
  }

  char * value(uint32_t & vl) {
    uint32_t off = 0;
    while (off < vallen) {
      long v = lrand48();
      v = (v/factor) * factor;
      uint32_t wn = snprintf(valb+off, vallen+32-off, "%09lx\t", v);
      off += wn;
    }
    vl = vallen;
    return valb;
  }

  void write(FILE * fout, int64_t totallen) {
    while (totallen > 0) {
      uint32_t kl,vl;
      char * key = this->key(kl);
      char * value = this->value(vl);
      fwrite(key, kl, 1, fout);
      fputc('\t',fout);
      fwrite(value, vl, 1, fout);
      fputc('\n',fout);
      totallen -= (kl+vl+2);
    }
    fflush(fout);
  }
};


} // namespace Hadoop


using namespace Hadoop;


// prevent compiler optimization
static uint64_t ret_value = 0;

typedef int (*TestFunction) (int, char **);

inline uint32_t hashcode(void * key, uint32_t keylen) {
  uint32_t ret = 0;
  for (uint32_t idx = 0; idx < keylen; idx++) {
    ret = ret * 31 + ((uint8_t*)key)[idx];
  }
  return ret;
}


static int test_VUInt(int argc, char **argv) {
  char buff[32];
  for (uint32_t i = 0 ; i < 0x7fffffff; i+=(i/10+1)) {
    char * p = buff;
    WriteVUInt32(p, i);
    char * p2 = buff;
    uint32_t v = ReadVUInt32(p2);
    assert(p==p2);
    assert(i==v);
    assert(p2-buff == GetVUInt32Len(i));
    assert(p2-buff == GetVUInt32BufferLen(buff));
  }
  return 0;
}

static int test_normalbswap(int argc, char **argv) {
  uint32_t dd[8];
  for (int i=0;i<8;i++) {
    dd[i] = i*0x393389efU;
    assert(normal_bswap(dd[i]) == bswap(dd[i]));
  }
  for (int i=0;i<1000000;i++) {
    dd[0] = normal_bswap(dd[0]);
    dd[1] = normal_bswap(dd[1]);
    dd[2] = normal_bswap(dd[2]);
    dd[3] = normal_bswap(dd[3]);
    dd[4] = normal_bswap(dd[4]);
    dd[5] = normal_bswap(dd[5]);
    dd[6] = normal_bswap(dd[6]);
    dd[7] = normal_bswap(dd[7]);
  }
  for (int i=0;i<8;i++) {
    assert(dd[i] == i*0x393389efU);
    ret_value += dd[i];
  }
  return 0;
}



static int test_normalbswap64(int argc, char **argv) {
  uint64_t dd[8];
  for (int i=0;i<8;i++) {
    dd[i] = i*0x393389efUL;
    assert(normal_bswap64(dd[i]) == bswap64(dd[i]));
  }
  for (int i=0;i<1000000;i++) {
    dd[0] = normal_bswap64(dd[0]);
    dd[1] = normal_bswap64(dd[1]);
    dd[2] = normal_bswap64(dd[2]);
    dd[3] = normal_bswap64(dd[3]);
    dd[4] = normal_bswap64(dd[4]);
    dd[5] = normal_bswap64(dd[5]);
    dd[6] = normal_bswap64(dd[6]);
    dd[7] = normal_bswap64(dd[7]);
  }
  for (int i=0;i<8;i++) {
    assert(dd[i] == i*0x393389efUL);
    ret_value += dd[i];
  }
  return 0;
}

static int test_bswap(int argc, char **argv) {
  uint32_t dd[8];
  for (int i=0;i<8;i++) {
    dd[i] = i*0x393389efU;
  }
  for (int i=0;i<1000000;i++) {
    dd[0] = bswap(dd[0]);
    dd[1] = bswap(dd[1]);
    dd[2] = bswap(dd[2]);
    dd[3] = bswap(dd[3]);
    dd[4] = bswap(dd[4]);
    dd[5] = bswap(dd[5]);
    dd[6] = bswap(dd[6]);
    dd[7] = bswap(dd[7]);
  }
  for (int i=0;i<8;i++) {
    assert(dd[i] == i*0x393389efU);
    ret_value += dd[i];
  }
  return 0;
}

static int test_bswap64(int argc, char **argv) {
  uint64_t dd[8];
  for (int i=0;i<8;i++) {
    dd[i] = i*0x393389efUL;
  }
  for (int i=0;i<1000000;i++) {
    dd[0] = bswap64(dd[0]);
    dd[1] = bswap64(dd[1]);
    dd[2] = bswap64(dd[2]);
    dd[3] = bswap64(dd[3]);
    dd[4] = bswap64(dd[4]);
    dd[5] = bswap64(dd[5]);
    dd[6] = bswap64(dd[6]);
    dd[7] = bswap64(dd[7]);
  }
  for (int i=0;i<8;i++) {
    assert(dd[i] == i*0x393389efUL);
    ret_value += dd[i];
  }
  return 0;
}

static int test_memcmp(int argc, char **argv) {
  uint8_t buff[2048];
  for (int i=0;i<2048;i++) {
    buff[i] = i & 0xff;
  }
  std::random_shuffle(buff, buff+2048);
  int r = 0;
  for (int i=0;i<1000000;i++) {
    int offset = i % 1000;
    r += memcmp(buff, buff+1024, 5);
    r += memcmp(buff+offset, buff+1124, 9);
    r += memcmp(buff+offset, buff+1224, 10);
    r += memcmp(buff+offset, buff+1324, 15);
    r += memcmp(buff+offset, buff+1424, 16);
    r += memcmp(buff+offset, buff+1524, 17);
    r += memcmp(buff+offset, buff+1624, 18);
    r += memcmp(buff+offset, buff+1724, 19);
  }
  ret_value += r;
  return 0;
}


static int test_fmemcmp(int argc, char ** argv) {
  char buff[2048];
  for (int i=0;i<2048;i++) {
    buff[i] = i & 0xff;
  }
  std::random_shuffle(buff, buff+2048);
  int r = 0;
  for (int i=0;i<1000000;i++) {
    int offset = i % 1000;
    r += fmemcmp(buff, buff+1024, 5);
    r += fmemcmp(buff+offset, buff+1124, 9);
    r += fmemcmp(buff+offset, buff+1224, 10);
    r += fmemcmp(buff+offset, buff+1324, 15);
    r += fmemcmp(buff+offset, buff+1424, 16);
    r += fmemcmp(buff+offset, buff+1524, 17);
    r += fmemcmp(buff+offset, buff+1624, 18);
    r += fmemcmp(buff+offset, buff+1724, 19);
  }
  ret_value += r;
  return 0;
}

static int test_fmemcmp_correctness(int argc, char **argv) {
  std::vector<std::string> vs;
  char buff[14];
  vs.push_back("");
  for (int i=0;i<5000;i++) {
    snprintf(buff, 14, "%d", i*31);
    vs.push_back(buff);
    snprintf(buff, 10, "%010d", i);
    vs.push_back(buff);
  }
  for (size_t i=0;i<vs.size();i++) {
    for (size_t j = 0; j<vs.size(); j++) {
      std::string & ls = vs[i];
      std::string & rs = vs[j];
      size_t m = std::min(ls.length(),rs.length());
      int c = memcmp(ls.c_str(),rs.c_str(), m);
      int t = fmemcmp(ls.c_str(),rs.c_str(), m);
      if (!((c==0 && t==0) || (c>0 && t>0) || (c<0 && t<0))) {
        assert(false);
        ret_value = 2;
      }
    }
  }
  return 0;
}

static void test_memcpy_perf_len(char * src, char * dest, size_t len, size_t time) {
  for (size_t i=0;i<time;i++) {
    memcpy(src, dest, len);
    memcpy(dest, src, len);
  }
}

static void test_simple_memcpy_perf_len(char * src, char * dest, size_t len, size_t time) {
  for (size_t i=0;i<time;i++) {
    simple_memcpy(src, dest, len);
    simple_memcpy(dest, src, len);
  }
}

static void test_simple_memcpy2_perf_len(char * src, char * dest, size_t len, size_t time) {
  for (size_t i=0;i<time;i++) {
    simple_memcpy2(src, dest, len);
    simple_memcpy2(dest, src, len);
  }
}

int test_memcpy_perf(int argc, char ** argv) {
  char * src = new char[10240];
  char * dest = new char[10240];
  char buff[32];
  for (size_t len = 1; len < 256; len=len+31) {
    fprintf(stderr, "------------------------------\n");
    snprintf(buff, 32, "       memcpy %luB\t", len);
    TIMED(buff, test_memcpy_perf_len(src,dest,len,1000000);)
    snprintf(buff, 32, "simple_memcpy %luB\t", len);
    TIMED(buff, test_simple_memcpy_perf_len(src,dest,len,1000000);)
    snprintf(buff, 32, "simple_memcpy2 %luB\t", len);
    TIMED(buff, test_simple_memcpy2_perf_len(src,dest,len,1000000);)
  }
  delete [] src;
  delete [] dest;
}

static void SplitEqual(string lhs, string lhsSplit, bool lhsclean,
                       string rhs, string rhsSplit, bool rhsclean) {
  vector<string> lret = SplitString(lhs, lhsSplit, lhsclean);
  vector<string> rret = SplitString(rhs, rhsSplit, rhsclean);
  string ljoin = JoinString(lret, "|");
  string rjoin = JoinString(rret, "|");
  if (ljoin!=rjoin) {
    printf("[%s] != [%s]\n", ljoin.c_str(), rjoin.c_str());
    assert(false);
  }
}

int test_string_utils(int argc, char ** argv) {
  assert(Trim("  \tass  \t  ")=="ass");
  assert(Trim("  \t  \t  ")=="");
  assert(Trim("")=="");
  SplitEqual("aaaa bbbb",   "",  false, "   adlkj sl ",         "",    true);
  SplitEqual("1aaa bbb ccc", " ", false, "1aaa \t bbb \t ccc",    " \t ",false);
  SplitEqual("2aaa bbb ccc", " ", false, "2aaa \t bbb \t ccc \t", "\t",  true);
  return 0;
}

int test_PartitionBucket(int argc, char ** argv) {
  SortType sorttype;
  FILE * data = stdin;
  if (argc == 2) {
    if (strcmp(argv[1], "c") == 0) {
      sorttype = CQSORT;
    }
    else {
      sorttype = CPPSORT;
    }
  } else {
    fprintf(stderr, "Usage: %s <c|cpp>\n", argv[0]);
    return 1;
  }
  MemoryBlockPool::init(300*1024*1024, 64*1024);
  PartitionBucket pb = PartitionBucket(0);
  char buff[1024];
  while (fgets(buff,1024,data)!=NULL) {
    uint32_t len = strlen(buff);
    if (buff[len-1]=='\n') {
      len--;
    }
    char * pos = strchr(buff,'\t');
    uint32_t keylen = pos - buff;
    uint32_t vallen = len - (pos - buff) -1;
    char * dest = pb.get_buffer_to_put(keylen, vallen);
    if (NULL == dest) {
      fprintf(stderr, "Error no memory, need to spill\n");
      break;
    }
    InplaceBuffer * keyb = (InplaceBuffer*)dest;
    keyb->length = keylen;
    memcpy(keyb->content, buff, keylen);
    InplaceBuffer & valb = keyb->next();
    valb.length = vallen;
    memcpy(valb.content, pos + 1, vallen);
  }

  if (sorttype == CQSORT) {
    TIMED("c sort",
    pb.sort(CQSORT);
    )
  } else {
    TIMED("cpp sort",
    pb.sort(CPPSORT);
    )
  }
  MemoryBlockPool::dump(stderr);
  return 0;
}


int _test_MapOutputBuffer(uint32_t num_partition, const char * configfile,
    const char * outputfile, FILE * data) {
  MapOutputCollector * moc = new MapOutputCollector(num_partition);
  Config config;
  config.load(configfile);
  moc->configure(config);
  char buff[1024];
  while (fgets(buff, 1024, data) != NULL) {
    uint32_t len = strlen(buff);
    if (buff[len - 1] == '\n') {
      len--;
    }
    char * key = buff;
    char * value = strchr(buff, '\t');
    uint32_t keylen = value - buff;
    uint32_t vallen = len - (value - buff) - 1;
    uint32_t partition = (hashcode(key, keylen) & 0x7fffffff) % num_partition;
    int ret = moc->put(key, keylen, value+1, vallen, partition);
    if (ret == 1) {
      fprintf(stderr, "Error no memory, need to spill\n");
      break;
    }
  }
  try {
    TIMED("sort",
        moc->sort_all(moc->get_sort_type());
    )
    std::vector<std::string> paths;
    paths.push_back(outputfile);
    std::string idx_file("");
    if (paths[0]!="/dev/null") {
      idx_file = paths[0] + ".index";
    }
    TIMED("spill",
        moc->final_merge_and_spill(paths, idx_file);
    )
  }
  catch (HadoopException e) {
    LOG("Spill exception: %s\n", e.what());
  }
  delete moc;
  return 0;
}

int test_MapOutputBuffer(int argc, char ** argv) {
  if (argc != 4) {
    fprintf(stderr, "Usage: %s <partition num> <configfile> <outputfile>\n",
        argv[0]);
    return 1;
  }
  int ret;
  TIMED("test_MapOutputBuffer",
      ret = _test_MapOutputBuffer(atoi(argv[1]), argv[2], argv[3], stdin);)
  return ret;
}

int test_writer(int argc, char ** argv) {
  if (argc<4) {
    fprintf(stderr, "Usage: %s <outputfile> <size(M)> [partition] [type]\n",
        argv[0]);
    return 1;
  }
  int64_t totalsize = atol(argv[2]) * 1024*1024;
  int partition = 1;
  if (argc>=4) {
    partition = atoi(argv[3]);
  }
  int type = 1;
  if (argc>=5) {
    if (strcmp("inter", argv[4])==0) {
      type = 0;
    } else if (strcmp("bytes", argv[4])==0) {
      type = 2;
    }
  }
  char key[128];
  char value[128];
  uint32_t kl = 9;
  uint32_t vl = 89;
  for (int i=0;i<128;i++) {
    key[i] = 'a' + (i%26);
    value[i] = 'A' + (i%26);
  }
  FILE * fout = fopen(argv[1],"wb");
  int fd = fileno(fout);
  uint32_t buffsize = 32*1024-sizeof(uint64_t);
  char * wbuff = new char[32*1024];
  MapOutputWriter * writer;
  if (type == 0) {
    writer = new IntermediateFileWriter(fd, wbuff, buffsize, PLAIN);
  } else if (type==1) {
    writer = new IFileWriter(fd, wbuff, buffsize, TextType, TextType, PLAIN);
  } else if (type==2) {
    writer = new IFileWriter(fd, wbuff, buffsize, BytesType, BytesType, PLAIN);
  } else {
    THROW_EXCEPTION(UnsupportException, "output type not supported");
  }
  int64_t nexp = totalsize / partition;
  int64_t current = 0;
  writer->start_partition();
  while (current<totalsize) {
    if (current>=nexp) {
      writer->end_partition();
      nexp += (totalsize/partition);
      writer->start_partition();
    }
    writer->write_key_part(key, kl, vl);
    writer->write_value_part(value, vl);
    current += (kl+vl+8);
  }
  writer->end_partition();
  delete writer;
  delete [] wbuff;
  fclose(fout);
}


/**
 * Tool to dump ifile contents
 */
static void dump_ifile(std::string path, KeyValueType kvtype,
    uint32_t buff_size, int fdout) {
  std::string indexfile = path + ".index";
  IFileIndex * index = IFileIndex::create(indexfile.c_str());
  SpillRangeInfo * spill_info = index->to_spill_range_info(path);
  delete index;
  FILE * fin = fopen(path.c_str(),"rb");
  int fdin = fileno(fin);
  char * buff = new char[buff_size+sizeof(uint64_t)];
  IFileReader reader = IFileReader(fdin, buff, buff_size, kvtype, kvtype, spill_info);
  char * writebuff = new char[buff_size+sizeof(uint64_t)];
  uint64_t real_offset = 0;
  uint32_t checksum = checksum_init();
  AppendBuffer appender
      = AppendBuffer(writebuff, buff_size, fdout, &checksum, &real_offset);
  while (reader.next_partition() == 0) {
    appender.append("-------------------------------------",32);
    appender.append("\n",1);
    const char * key;
    uint32_t key_len;
    while (NULL != (key=reader.next_key(key_len))) {
      appender.append("[", 1);
      appender.append(key,key_len);
      appender.append("][", 2);
      const char * value;
      uint32_t value_len;
      value = reader.value(value_len);
      appender.append(value, value_len);
      appender.append("]\n", 2);
    }
  }
  appender.flush();
  delete [] writebuff;
  delete [] buff;
  fclose(fin);
}

/**
 * tool to dump intermediate file
 */
static void dump_interfile(std::string path, uint32_t buff_size, int fdout) {
  std::string indexfile = path + ".index";
  SpillRangeInfo * spill_info = NULL;
  struct stat st;
  if (stat(indexfile.c_str(), &st)!=0) {
    stat(path.c_str(), &st);
    SpillSegmentInfo * segs = new SpillSegmentInfo[1];
    segs[0].end_position = st.st_size-sizeof(uint32_t);
    segs[0].real_end_position = st.st_size;
    spill_info = new SpillRangeInfo(0, 1, path, segs);
  } else {
    IFileIndex * index = IFileIndex::create(indexfile.c_str());
    spill_info = index->to_spill_range_info(path);
    delete index;
  }
  IntermediateFileReader * ifr = new IntermediateFileReader(spill_info, buff_size);
  char * writebuff = new char[buff_size+sizeof(uint64_t)];
  uint64_t real_offset = 0;
  uint32_t checksum = checksum_init();
  AppendBuffer appender
      = AppendBuffer(writebuff, buff_size, fdout, &checksum, &real_offset);
  while (0==ifr->next_partition()) {
    appender.append("-------------------------------------",32);
    appender.append("\n",1);
    const char * key;
    uint32_t key_len;
    while (NULL != (key=ifr->next_key(key_len))) {
      appender.append("[", 1);
      appender.append(key,key_len);
      appender.append("][", 2);
      uint32_t value_len = ifr->current_value_len();
      const char * value = ifr->value();
      appender.append(value, value_len);
      appender.append("]\n", 2);
    }
  }
  appender.flush();
  delete ifr;
  delete [] writebuff;
  delete spill_info;
}

int test_reader(int argc, char **argv) {
  if (argc<2) {
    fprintf(stderr, "Usage: %s <filepath> [KeyValueType|inter]\n", argv[0]);
    return 1;
  }
  KeyValueType kvtype = TextType;
  if (argc>=3) {
    if (std::string(argv[2])=="bytes") {
      kvtype = BytesType;
    } else if (std::string(argv[2])=="inter") {
      kvtype = (KeyValueType)-1;
    }
  }
  if (kvtype>=0) {
    dump_ifile(argv[1], kvtype, 64*1024, fileno(stdout));
  } else {
    dump_interfile(argv[1], 64*1024, fileno(stdout));
  }
  return 0;
}


int test_merge(int argc, char ** argv) {
  if (argc<4) {
    fprintf(stderr,
        "Usage: %s <partition num> <config file> <output file> [io.sort.mb]\n",
        argv[0]);
    return 1;
  }
  int num_partition=atoi(argv[1]);
  MapOutputCollector * moc = new MapOutputCollector(num_partition);
  Config & config = NativeObjectFactory::GetConfig();
  config.load(argv[2]);
  if (argc>=5) {
    config.set_uint32("io.sort.mb", atoi(argv[4]));
  }
  else {
    config.set_uint32("io.sort.mb", 10);
  }
  moc->configure(config);
  int mid_spill_num = 0;
  char buff[1024];
  while (fgets(buff, 1024, stdin) != NULL) {
    uint32_t len = strlen(buff);
    if (buff[len - 1] == '\n') {
      len--;
    }
    char * key = buff;
    char * value = strchr(buff, '\t');
    uint32_t keylen = value - buff;
    uint32_t vallen = len - (value - buff) - 1;
    uint32_t partition = (hashcode(key, keylen) & 0x7fffffff) % num_partition;
    int ret = moc->put(key, keylen, value+1, vallen, partition);
    if (ret == 1) {
      char tbuff[128];
      snprintf(tbuff, 128, "spill_%03d", mid_spill_num++);
      std::vector<std::string> filename;
      filename.push_back(tbuff);
      TIMED("mid-sort",
          moc->sort_all(moc->get_sort_type());)
      TIMED("mid-spill",
      if (0!=moc->mid_spill(filename)) {
        THROW_EXCEPTION(IOException,"mid-spill failed");
      })
      int ret = moc->put(key, keylen, value+1, vallen, partition);
    }
  }
  try {
    std::vector<std::string> paths;
    paths.push_back(argv[3]);
    std::string idx_file("");
    if (paths[0]!="/dev/null") {
      idx_file = paths[0] + ".index";
    } else {
      idx_file = paths[0];
    }
    TIMED("last sort", moc->sort_all(moc->get_sort_type());)
    TIMED("merge&spill", moc->final_merge_and_spill(paths, idx_file);)
  }
  catch (HadoopException e) {
    LOG("Spill exception: %s\n", e.what());
  }
  delete moc;
  return 0;
}

int test_kvgenerator(int argc, char ** argv) {
  if (argc<4) {
    fprintf(stderr,
        "Usage: %s <key len> <value len> <size[M]> [unique]\n",
        argv[0]);
    return 1;
  }
  bool unique = false;
  if (argc>=5) {
    if (argv[4][0]=='u') {
      unique = true;
    }
  }
  uint32_t kl = atoi(argv[1]);
  uint32_t vl = atoi(argv[2]);
  int64_t total = atol(argv[3]) * 1024 * 1024;
  KVGenerator gen = KVGenerator(kl,vl,unique);
  gen.write(stdout, total);
  return 0;
}

#include "RReducerHandler.h"

namespace Hadoop {
using std::string;

class TestRReducerHandler : public RReducerHandler {
  static const uint32_t BUFFSIZE = 16*1024;
  string _inputData;
  string _outputData;
  uint32_t _inputDataUsed;
  uint32_t _inputKeyGroup;
public:
  void initBuffers() {
    _ib.reset(new char[BUFFSIZE], BUFFSIZE);
    _ob.reset(new char[BUFFSIZE], BUFFSIZE-8);
  }
  void makeInputData(int cnt) {
    _inputData = "";
    _inputData.reserve(1024*1024);
    _outputData.reserve(1024*1024);
    char buff[128];
    for (int i=0;i<cnt;i++) {
      // make every 10 value a group
      snprintf(buff,128,"%010d", i);
      uint32_t len = strlen(buff)-1;
      _inputData.append((const char*)(&len), 4);
      len+=1;
      _inputData.append((const char*)(&len), 4);
      _inputData.append(buff,len-1);
      _inputData.append(buff,len);
    }
    uint32_t len = (uint32_t)-1;
    _inputData.append((const char*)(&len), 4);
    _inputData.append((const char*)(&len), 4);
    _inputDataUsed = 0;
    _inputKeyGroup = (cnt+9)/10;
  }
  virtual void flushOutput(uint32_t length) {
    _outputData.append(_ob.buff, length);
  }
  virtual void waitRead() {
    uint32_t rest = _inputData.length() - _inputDataUsed;
    uint32_t cp = 13500 < rest ? 13500 : rest;
    memcpy(_ib.buff, _inputData.c_str()+_inputDataUsed, cp);
    _inputDataUsed += cp;
    _ib.position = cp;
    _ibPosition = cp;
    _current = _ib.buff;
    _remain = _ibPosition;
  }
  void process() {
    initBuffers();
    makeInputData(10000);
    uint32_t outputKeyGroup = 0;
    while (nextKey()) {
      outputKeyGroup++;
      const char * key;
      const char * value;
      uint32_t keyLen;
      uint32_t valLen;
      key = getKey(keyLen);
      //printf("key: [%s]\n", string(key, keyLen).c_str());
      //int maxt = 3;
      while ((value=nextValue(valLen)) != NULL) {
        //printf("value: [%s]\n", string(value, valLen).c_str());
        //if (--maxt==0)
        //  break;
        _outputData.append((const char *)&keyLen, 4);
        _outputData.append((const char *)&valLen, 4);
        _outputData.append(key, keyLen);
        _outputData.append(value,valLen);
      }
    }
    uint32_t eofLen = (uint32_t)-1;
    _outputData.append((const char *)&eofLen, 4);
    _outputData.append((const char *)&eofLen, 4);
    //printf("input key group: %u output key group: %u\n", _inputKeyGroup, outputKeyGroup);
    assert(outputKeyGroup==_inputKeyGroup);
    assert(_outputData==_inputData);
  }
};
}

int test_rreducerhandler(int argc, char ** argv) {
  TestRReducerHandler * t = new TestRReducerHandler();
  t->process();
  return 0;
}

int main(int argc, char **argv) {
  std::vector<std::pair<std::string, TestFunction> > tests;
  tests.push_back(std::make_pair("vuint", test_VUInt));
  tests.push_back(std::make_pair("fmemcmp_correctness", test_fmemcmp_correctness));
  tests.push_back(std::make_pair("memcmp", test_memcmp));
  tests.push_back(std::make_pair("fmemcmp", test_fmemcmp));
  tests.push_back(std::make_pair("memcpy", test_memcpy_perf));
  tests.push_back(std::make_pair("normalbswap", test_normalbswap));
  tests.push_back(std::make_pair("bswap", test_bswap));
  tests.push_back(std::make_pair("normalbswap64", test_normalbswap64));
  tests.push_back(std::make_pair("bswap64", test_bswap64));
  tests.push_back(std::make_pair("string",test_string_utils));
  tests.push_back(std::make_pair("rreducerhandler", test_rreducerhandler));
  int ret = ret_value;
  if (argc == 1) {
    fprintf(stderr, "Usage: %s <all|<testname>>\n", argv[0]);
    fprintf(stderr, "           do basic unittest(s)\n");
    fprintf(stderr, "   or: \n");
    fprintf(stderr, "       %s <mob|pb|merge|writer|reader|gen> ...\n", argv[0]);
    fprintf(stderr, "           do function perf test/debug \n");
  }
  else if (argc == 2) {
    std::string name(argv[1]);
    if (name == "all") {
      for (size_t i = 0 ; i < tests.size() ; i++) {
        std::pair<std::string, TestFunction> & t = tests[i];
        TIMED(t.first.c_str(),
            ret = t.second(argc, argv););
        if (ret!=0) {
          fprintf(stderr, "test %s failed, return %d\n ret_val %llu", t.first.c_str(), ret, ret_value);
          return ret;
        }
      }
      return ret;
    } else {
      bool found = false;
      for (size_t i = 0 ; i < tests.size() ; i++) {
        std::pair<std::string, TestFunction> & t = tests[i];
        if (t.first == name) {
          found = true;
          TIMED(t.first.c_str(),
              ret = t.second(argc, argv););
          if (ret!=0) {
            fprintf(stderr, "test %s failed, return %d\n", t.first.c_str(), ret);
          }
        }
      }
      if (found) {
        return ret;
      }
    }
  }
  std::string name(argv[1]);
  if (name == "mob") {
    test_MapOutputBuffer(argc - 1, argv + 1);
  }
  else if (name == "pb") {
    test_PartitionBucket(argc - 1, argv + 1);
  }
  else if (name == "merge") {
    test_merge(argc - 1, argv + 1);
  }
  else if (name == "writer") {
    test_writer(argc - 1, argv + 1);
  }
  else if (name == "reader") {
    test_reader(argc - 1, argv + 1);
  }
  else if (name == "gen") {
    test_kvgenerator(argc - 1, argv + 1);
  }
  else {
    fprintf(stderr, "test %s not found\n", name.c_str());
    ret = 1;
  }
  return ret;
}


