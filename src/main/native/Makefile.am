export PLATFORM = $(shell uname | tr [A-Z] [a-z])

ACLOCAL_AMFLAGS = -I m4

SNAPPY_PREFIX ?= $(HOME)/local

if PLATFORM_IS_MACOS
# macos 
JNI_INCLUDE ?= -I/System/Library/Frameworks/JavaVM.framework/Versions/Current/Headers/
TESTLDFLAGS = -framework JavaVM
endif

if PLATFORM_IS_LINUX
# linux
JNI_INCLUDE ?= -I$(JAVA_HOME)/include -I$(JAVA_HOME)/include/$(PLATFORM)
TESTLDFLAGS = -lrt
endif

# primatives configs 
PRFLAGS=-DSIMPLE_MEMCPY        
#-DCHECKSUM_CRC32C             
#-DSPILLRECORD_CHECKSUM_UINT   

AM_LDFLAGS = -L$(SNAPPY_PREFIX)/lib -lsnappy -lpthread -ldl -lz
AM_CFLAGS = $(PRFLAGS) -I$(SNAPPY_PREFIX)/include -Ijavah -I$(top_srcdir)/src

COMMONSOURCES = \
lz4/lz4.c                              \
cityhash/city.cc                       \
src/commons.cc                         \
src/Hash.cc                            \
src/mempool.cc                         \
src/SyncUtils.cc                       \
src/jniutils.cc                        \
src/NativeRuntimeJniImpl.cc            \
src/Timer.cc                           \
src/StringUtil.cc                      \
src/WritableUtils.cc                   \
src/Random.cc                          \
src/Checksum.cc                        \
src/Streams.cc                         \
src/BufferStream.cc                    \
src/Buffers.cc                         \
src/FileSystem.cc                      \
src/Compressions.cc                    \
src/PartitionIndex.cc                  \
src/KVFile.cc                          \
src/IFile.cc                           \
src/MapOutputSpec.cc                   \
src/MapOutputCollector.cc              \
src/NativeObject.cc                    \
src/NativeObjectFactory.cc             \
src/NativeTask.cc                      \
src/NativeLibrary.cc                   \
src/BatchHandler.cc                    \
src/EchoBatchHandler.cc                \
src/MCollectorOutputHandler.cc         \
src/MMapperHandler.cc                  \
src/MMapTaskHandler.cc                 \
src/RReducerHandler.cc                 \
src/codec/GzipCodec.cc                 \
src/codec/BlockCodec.cc                \
src/codec/SnappyCodec.cc               \
src/codec/Lz4Codec.cc                  \
src/lib/FileSplit.cc                   \
src/lib/LineRecordWriter.cc            \            
src/lib/LineRecordReader.cc            \
src/lib/TeraSort.cc                    \
src/lib/WordCount.cc                   


COMMONHEADERS = \
src/NativeTask.h                      \
src/NativeObject.h                    \
src/Checksum.h                        \
src/Streams.h                         \
src/StringUtil.h                      \
src/FileSystem.h                      \
src/NativeObjectFactory.h             


TESTSOURCES = \
test/test_commons.cc                  \
test/TestPrimitives.cc                \
test/TestCompressions.cc              \
test/TestChecksum.cc                  \
test/TestSyncUtils.cc                 \
test/TestRReducerHandler.cc           \
test/TestMMapTaskHandler.cc           \
test/TestStringUtil.cc                \
test/TestBuffers.cc                   \
test/TestStream.cc                    \
test/TestFileSystem.cc                \
test/TestHash.cc                      \
test/TestIFile.cc                     \
test/TestSort.cc                      \
test/TestWritableUtils.cc             \
test/TestMapTask.cc                   \
test/lib/TestLineRecordReader.cc      \
test/TestMain.cc               

include_HEADERS = $(COMMONHEADERS)
lib_LTLIBRARIES = libnativetask.la
noinst_PROGRAMS = nttest
noinst_SCRIPTS = test.sh

libnativetask_la_SOURCES = $(COMMONSOURCES)
libnativetask_la_CPPFLAGS = -g -O2 -DNDEBUG -fPIC $(JNI_INCLUDE) $(AM_CFLAGS)
libnativetask_la_LDFLAGS = -version-info 0:1:0 $(AM_LDFLAGS)


nttest_SOURCES = $(TESTSOURCES) gtest/gtest-all.cc
nttest_LDADD = libnativetask.la
nttest_LDFLAGS = $(TESTLDFLAGS) $(AM_LDFLAGS)
nttest_CPPFLAGS = -g -O2 $(JNI_INCLUDE) $(AM_CFLAGS) -I$(top_srcdir) -I$(top_srcdir)/test
