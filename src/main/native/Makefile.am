export PLATFORM = $(shell uname | tr [A-Z] [a-z])

ACLOCAL_AMFLAGS = -I m4

SNAPPY_PREFIX ?= $(HOME)/local

if PLATFORM_IS_MACOS
# macos 
JNI_INCLUDE ?= -I/System/Library/Frameworks/JavaVM.framework/Versions/Current/Headers/
TESTLDFLAGS = 
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

AM_LDFLAGS = -L$(SNAPPY_PREFIX)/lib -lsnappy -lpthread -ldl
AM_CFLAGS = -I$(SNAPPY_PREFIX)/include $(PRFLAGS)

COMMONSOURCES = \
src/commons.cc                         \
src/mempool.cc                         \
src/MapOutputCollector.cc              \
src/jniutils.cc                        \
src/NativeRuntimeJniImpl.cc            \
src/Checksum.cc                        \
src/Streams.cc                         \
src/BufferedStream.cc                  \
src/Compressions.cc                    \
src/GzipCodec.cc                       \
src/SnappyCodec.cc                     \
src/FileSystem.cc                      \
src/BatchHandler.cc                    \
src/NativeObject.cc                    \
src/NativeObjectFactory.cc             \
src/NativeTask.cc                      \
src/NativeLibrary.cc                   \
src/EchoBatchHandler.cc                \
src/MCollectorOutputHandler.cc         \
src/MMapperHandler.cc                  \
src/RReducerHandler.cc



lib_LTLIBRARIES = libnativetask.la

libnativetask_la_SOURCES = $(COMMONSOURCES)
libnativetask_la_CPPFLAGS = -g -O2 -DNDEBUG -fPIC $(JNI_INCLUDE) $(AM_CFLAGS) -Ijavah
libnativetask_la_LDFLAGS = -version-info 1:0:0 $(AM_LDFLAGS)

noinst_PROGRAMS = ntperftest
# nttest_SOURCES = $(COMMONSOURCES) ./src/test.cc
# nttest_LDFLAGS = $(TESTLDFLAGS) $(AM_LDFLAGS)
# nttest_CPPFLAGS = -g $(AM_CFLAGS)

ntperftest_SOURCES = ./src/test.cc
ntperftest_LDADD = libnativetask.la
ntperftest_LDFLAGS = $(TESTLDFLAGS) $(AM_LDFLAGS)
ntperftest_CPPFLAGS = -g -O2 $(JNI_INCLUDE) $(AM_CFLAGS) -Ijavah
