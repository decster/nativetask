export PLATFORM = $(shell uname | tr [A-Z] [a-z])

ACLOCAL_AMFLAGS = -I m4

SNAPPY_PREFIX ?= $(HOME)/local

if PLATFORM_IS_MACOS
# macos 
JNI_INCLUDE ?= -I/System/Library/Frameworks/JavaVM.framework/Versions/Current/Headers/
PLATFORM_LDFLAGS = -framework JavaVM
endif

if PLATFORM_IS_LINUX
# linux
JNI_INCLUDE ?= -I$(JAVA_HOME)/include -I$(JAVA_HOME)/include/$(PLATFORM)
PLATFORM_LDFLAGS = -lrt -L$(JAVA_HOME)/jre/lib/amd64/server -ljvm
endif

# primatives configs 
PRFLAGS=-DSIMPLE_MEMCPY 
#-DSPILLRECORD_CHECKSUM_UINT

AM_LDFLAGS = -L$(SNAPPY_PREFIX)/lib -lsnappy -lpthread -ldl -lz $(PLATFORM_LDFLAGS)
AM_CFLAGS = $(PRFLAGS) $(JNI_INCLUDE) -I$(SNAPPY_PREFIX)/include -Ijavah -I$(top_srcdir)/src -I$(top_srcdir)/src/util

COMMONSOURCES = \
lz4/lz4.c                              \
cityhash/city.cc                       \
src/codec/GzipCodec.cc                 \
src/codec/BlockCodec.cc                \
src/codec/SnappyCodec.cc               \
src/codec/Lz4Codec.cc                  \
src/handler/BatchHandler.cc            \
src/handler/EchoBatchHandler.cc        \
src/handler/MCollectorOutputHandler.cc \
src/handler/MMapperHandler.cc          \
src/handler/MMapTaskHandler.cc         \
src/handler/RReducerHandler.cc         \
src/lib/FileSplit.cc                   \
src/lib/LineRecordWriter.cc            \            
src/lib/LineRecordReader.cc            \
src/lib/TotalOrderPartitioner.cc       \
src/lib/TeraSort.cc                    \
src/lib/WordCount.cc                   \
src/util/Hash.cc                       \
src/util/Random.cc                     \
src/util/Checksum.cc                   \
src/util/Timer.cc                      \
src/util/StringUtil.cc                 \
src/util/SyncUtils.cc                  \
src/util/WritableUtils.cc              \
src/commons.cc                         \
src/mempool.cc                         \
src/jniutils.cc                        \
src/NativeRuntimeJniImpl.cc            \
src/Buffers.cc                         \
src/Streams.cc                         \
src/BufferStream.cc                    \
src/Path.cc                            \
src/FileSystem.cc                      \
src/Compressions.cc                    \
src/PartitionIndex.cc                  \
src/KVFile.cc                          \
src/IFile.cc                           \
src/MapOutputSpec.cc                   \
src/MapOutputCollector.cc              \
src/Merge.cc                           \
src/TaskCounters.cc                    \
src/NativeObjectFactory.cc             \
src/NativeLibrary.cc                   \
src/NativeTask.cc                      



COMMONHEADERS = \
src/util/Checksum.h                   \
src/util/StringUtil.h                 \
src/util/WritableUtils.h              \
src/FileSystem.h                      \
src/Streams.h                         \
src/NativeObjectFactory.h             \
src/NativeTask.h                      


TESTSOURCES = \
test/handler/TestRReducerHandler.cc   \
test/handler/TestMMapTaskHandler.cc   \
test/lib/TestLineRecordReader.cc      \
test/lib/TestTotalOrderPartitioner.cc \
test/util/TestChecksum.cc             \
test/util/TestHash.cc                 \
test/util/TestStringUtil.cc           \
test/util/TestSyncUtils.cc            \
test/util/TestWritableUtils.cc        \
test/test_commons.cc                  \
test/TestBuffers.cc                   \
test/TestCompressions.cc              \
test/TestException.cc                 \
test/TestFileSystem.cc                \
test/TestIFile.cc                     \
test/TestPrimitives.cc                \
test/TestSort.cc                      \
test/TestStream.cc                    \
test/TestMain.cc               

include_HEADERS = $(COMMONHEADERS)
lib_LTLIBRARIES = libnativetask.la
noinst_PROGRAMS = nttest
noinst_SCRIPTS = test.sh

libnativetask_la_SOURCES = $(COMMONSOURCES)
libnativetask_la_CPPFLAGS = -g -O2 -DNDEBUG -fPIC $(AM_CFLAGS)
libnativetask_la_LDFLAGS = -version-info 0:1:0 $(AM_LDFLAGS)


nttest_SOURCES = $(TESTSOURCES) gtest/gtest-all.cc
nttest_LDADD = libnativetask.la
nttest_LDFLAGS = $(AM_LDFLAGS)
nttest_CPPFLAGS = -g -O2 $(AM_CFLAGS) -I$(top_srcdir) -I$(top_srcdir)/test 
