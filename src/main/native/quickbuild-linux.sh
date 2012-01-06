JAVA_HOME=$HOME/hadoop-v2/java6
SNAPPY_PREFIX=$HOME/local

CPPFLAGS="-I. -Isrc -Isrc/util -I$JAVA_HOME/include -I $JAVA_HOME/include/linux/ -I$SNAPPY_PREFIX/include/ -DSIMPLE_MEMCPY -DNDEBUG -DQUICK_BUILD"

LDFLAGS="-L $SNAPPY_PREFIX/lib -L$JAVA_HOME/jre/lib/amd64/server/ -lpthread -lz -lrt -ljvm -lsnappy -ldl"

echo "Build flags:" $CPPFLAGS $LDFLAGS

g++ src/*.cc src/codec/*.cc src/util/*.cc src/lib/*.cc src/handler/*.cc lz4/lz4.c cityhash/city.cc \
    $CPPFLAGS $LDFLAGS \
    -fPIC -O2 --shared -o libnativetask.so

g++ test/*.cc test/lib/*.cc test/handler/*.cc test/util/*.cc gtest/gtest-all.cc \
    -Itest $CPPFLAGS $LDFLAGS \
    -L. -lnativetask -fPIC -O2 -o nttest

