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

package org.apache.hadoop.mapred.nativetask;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.nativetask.NativeUtils;

/**
 * A factory for native object, also manages native runtime library
 * loading & cleaning
 */
public class NativeRuntime {
  private static Log LOG = LogFactory.getLog(NativeRuntime.class);
  private static boolean nativeLibraryLoaded = false;
  private static JobConf conf = new JobConf();
  // All configs native side needed
  private static String[] usefulExternalConfigsKeys = {
      "mapred.map.tasks",
      "mapred.reduce.tasks",
      "mapred.task.partition",
      "mapred.mapoutput.key.class",
      "mapred.mapoutput.value.class",
      "mapred.output.key.class",
      "mapred.output.value.class",
      "mapred.input.format.class",
      "mapred.output.format.class",
      "mapred.work.output.dir",
      "mapred.textoutputformat.separator",
      "mapred.compress.map.output",
      "mapred.map.output.compression.codec",
      "mapred.output.compress",
      "mapred.output.compression.codec",
      "mapred.map.output.sort",
      "io.sort.mb",
      "io.file.buffer.size",
      "fs.default.name",
      "fs.defaultFS",
  };

  static {
    try {
      System.loadLibrary("nativetask");
      LOG.info("Nativetask JNI library loaded.");
      nativeLibraryLoaded = true;
    } catch (Throwable t) {
      // Ignore failures
      LOG.info("Failed to load nativetask JNI library with error: " + t);
      LOG.info("java.library.path=" + System.getProperty("java.library.path"));
      LOG.info("LD_LIBRARY_PATH=" + System.getenv("LD_LIBRARY_PATH"));
    }
  }

  private static void assertNativeLibraryLoaded() {
    if (!nativeLibraryLoaded) {
      throw new RuntimeException("Native runtime library not loaded");
    }
  }

  public static boolean isNativeLibraryLoaded() {
    return nativeLibraryLoaded;
  }

  /**
   * Open File to read, used by native side
   * @param pathUTF8 file path
   * @return
   * @throws IOException
   */
  public static FSDataInputStream openFile(byte[] pathUTF8)
      throws IOException {
    String pathStr = NativeUtils.fromBytes(pathUTF8);
    Path path = new Path(pathStr);
    FileSystem fs = path.getFileSystem(conf);
    return fs.open(path);
  }

  /**
   * Create file to write, use by native side
   * @param pathUTF8
   * @param overwrite
   * @return
   * @throws IOException
   */
  public static FSDataOutputStream createFile(byte [] pathUTF8, boolean overwrite)
      throws IOException {
    String pathStr = NativeUtils.fromBytes(pathUTF8);
    Path path = new Path(pathStr);
    FileSystem fs = path.getFileSystem(conf);
    return fs.create(path, overwrite);
  }

  public static long getFileLength(byte [] pathUTF8)
      throws IOException {
    String pathStr = NativeUtils.fromBytes(pathUTF8);
    Path path = new Path(pathStr);
    FileSystem fs = path.getFileSystem(conf);
    return fs.getLength(path);
  }
  
  public static boolean exists(byte [] pathUTF8) throws IOException {
    String pathStr = NativeUtils.fromBytes(pathUTF8);
    Path path = new Path(pathStr);
    FileSystem fs = path.getFileSystem(conf);
    return fs.exists(path);
  }
  
  public static boolean remove(byte [] pathUTF8) throws IOException {
    String pathStr = NativeUtils.fromBytes(pathUTF8);
    Path path = new Path(pathStr);
    FileSystem fs = path.getFileSystem(conf);
    return fs.delete(path, true);
  }
  
  public static boolean mkdirs(byte [] pathUTF8) throws IOException {
    String pathStr = NativeUtils.fromBytes(pathUTF8);
    Path path = new Path(pathStr);
    FileSystem fs = path.getFileSystem(conf);
    boolean ret = fs.mkdirs(path);
    return ret;
  }

  public static void configure(JobConf jobConf) {
    assertNativeLibraryLoaded();
    conf = jobConf;
    List<byte[]> nativeConfigs = new ArrayList<byte[]>();
    // add needed external configs
    for (int i = 0; i < usefulExternalConfigsKeys.length; i++) {
      String key = usefulExternalConfigsKeys[i];
      String value = conf.get(key);
      if (value != null) {
        nativeConfigs.add(NativeUtils.toBytes(key));
        nativeConfigs.add(NativeUtils.toBytes(value));
      }
    }
    // add native.* configs
    for (Map.Entry<String, String> e : conf) {
      if (e.getKey().startsWith("native.")) {
        nativeConfigs.add(NativeUtils.toBytes(e.getKey()));
        nativeConfigs.add(NativeUtils.toBytes(e.getValue()));
      }
    }
    JNIConfigure(nativeConfigs.toArray(new byte[nativeConfigs.size()][]));
  }

  public static void set(String key, String value) {
    set(key, NativeUtils.toBytes(value));
  }

  public static void set(String key, byte [] value) {
    assertNativeLibraryLoaded();
    byte [][] jniConfig = new byte [2][];
    jniConfig[0] = NativeUtils.toBytes(key);
    jniConfig[1] = value;
    JNIConfigure(jniConfig);
  }

  public static void set(String key, boolean value) {
    set(key, Boolean.toString(value));
  }

  public static void set(String key, int value) {
    set(key, Integer.toString(value));
  }

  public synchronized static long createNativeObject(String clazz) {
    assertNativeLibraryLoaded();
    long ret = JNICreateNativeObject(NativeUtils.toBytes(clazz));
    if (ret == 0) {
      LOG.warn("Can't create NativeObject for class " + clazz
          + ", prabobly not exist.");
    }
    return ret;
  }

  public enum NativeObjectType {
    UnknownObjectType,
    BatchHandlerType,
    MapperType,
    ReducerType,
    PartitionerType,
    CombinerType,
    FolderType,
    RecordReaderType,
    RecordWriterType
  }

  public synchronized static long createDefaultObject(NativeObjectType type) {
    assertNativeLibraryLoaded();
    return JNICreateDefaultNativeObject(NativeUtils.toBytes(type.toString()));
  }

  public synchronized static void releaseNativeObject(long addr) {
    assertNativeLibraryLoaded();
    JNIReleaseNativeObject(addr);
  }

  public synchronized static void updateCounters(Counters counters) {

  }

  private native static void JNIRelease();
  private native static void JNIConfigure(byte [][] configs);
  private native static long JNICreateNativeObject(byte [] clazz);
  private native static long JNICreateDefaultNativeObject(byte [] type);
  private native static void JNIReleaseNativeObject(long addr);
  private native static int  JNIRegisterModule(byte [] path, byte [] name);
  private native static byte [] JNIGetCounters();
}

