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

package org.apache.hadoop.mapred;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapred.nativetask.NativeBatchProcessor;
import org.apache.hadoop.mapred.nativetask.OutputPathUtil;
import org.apache.hadoop.mapred.nativetask.NativeRuntime;
import org.apache.hadoop.mapred.nativetask.NativeTaskConfig;
import org.apache.hadoop.mapred.nativetask.NativeUtils;

public class NativeMapOutputCollector
    <K extends BinaryComparable, V extends BinaryComparable>
    extends NativeBatchProcessor
    implements MapTask.MapOutputCollector<K, V> {
  private static Log LOG = LogFactory.getLog(NativeMapOutputCollector.class);

  private MapOutputFile mapOutputFile = null;
  private TaskAttemptID taskAttemptID = null;
  private int spillNumber = 0;

  public static boolean canEnable(JobConf job) {
    if (!job.getBoolean(NativeTaskConfig.NATIVE_MAPOUTPUT_COLLECTOR_ENABLED, false)) {
      return false;
    }
    if (job.getNumReduceTasks() == 0) {
      return false;
    }
    if (job.getCombinerClass() != null) {
      return false;
    }
    if (job.getClass("mapred.output.key.comparator.class", null,
        RawComparator.class) != null) {
      return false;
    }
    if (job.getBoolean("mapred.compress.map.output", false) == true) {
      if (!"org.apache.hadoop.io.compress.SnappyCodec".equals(job
          .get("mapred.map.output.compression.codec"))) {
        return false;
      }
    }
    Class<?> keyCls = job.getMapOutputKeyClass();
    Class<?> valCls = job.getMapOutputValueClass();
    if (!(BinaryComparable.class.isAssignableFrom(keyCls) && 
          BinaryComparable.class.isAssignableFrom(valCls))) {
      return false;
    }
    return NativeRuntime.isNativeLibraryLoaded();
  }

  public NativeMapOutputCollector(JobConf conf, TaskAttemptID taskAttemptID) {
    super("MCollectorOutputHandler", conf.getInt(
        NativeTaskConfig.NATIVE_PROCESSOR_BUFFER_KB, 1024) * 1024, 0);
    this.taskAttemptID = taskAttemptID;
    this.mapOutputFile = new MapOutputFile();
    this.mapOutputFile.setConf(conf);
  }

  @Override
  protected boolean onProcess(ByteBuffer outputbuffer) {
    return true;
  }

  @Override
  protected boolean onFinish() {
    return true;
  }

  @Override
  protected byte[] sendCommandToJava(byte[] data) throws IOException {
    String cmd = NativeUtils.fromBytes(data);
    Path p = null;
    if (cmd.equals("GetOutputPath")) {
      p = mapOutputFile.getOutputFileForWrite(-1);
    } else if (cmd.equals("GetOutputIndexPath")) {
      p = mapOutputFile.getOutputIndexFileForWrite(-1);
    } else if (cmd.equals("GetSpillPath")) {
      p = mapOutputFile.getSpillFileForWrite(spillNumber++, -1);
    } else {
      LOG.warn("Illegal command: " + cmd);
    }
    if (p != null) {
      return NativeUtils.toBytes(p.toUri().getPath());
    } else {
      throw new IOException("MapOutputFile can't allocate spill/output file");
    }
  }

  @Override
  public void collect(K key, V value, int partition) throws IOException,
      InterruptedException {
    byte [] keyBuff = key.getBytes();
    int keyLen = key.getLength();
    byte [] valBuff = value.getBytes();
    int valLen = value.getLength();
    int kvLen = keyLen + valLen + 8;
    putInt(partition, kvLen);
    putInt(keyLen);
    put(keyBuff, 0, keyLen);
    putInt(valLen);
    put(valBuff, 0, valLen);
  };

  @Override
  public void flush() throws IOException, InterruptedException {
    finish();
  }

  @Override
  public void close() throws IOException, InterruptedException {
    if (!isFinished()) {
      finish();
    }
    releaseNative();
  }
}
