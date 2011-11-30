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
import java.nio.ByteBuffer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RawKeyValueIterator;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapred.TaskDelegation;
import org.apache.hadoop.mapred.TaskUmbilicalProtocol;
import org.apache.hadoop.mapred.nativetask.NativeUtils.KVType;
import org.apache.hadoop.mapred.nativetask.NativeUtils.NativeDeserializer;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;

public class NativeReduceTaskDelegator<IK, IV, OK, OV> implements
    TaskDelegation.ReduceTaskDelegator {
  private static final Log LOG = LogFactory.getLog(NativeReduceTaskDelegator.class);

  public NativeReduceTaskDelegator() {
  }

  @Override
  @SuppressWarnings("unchecked")
  public void run(TaskAttemptID taskAttemptID, JobConf job,
      TaskUmbilicalProtocol umbilical, Reporter reporter, RawKeyValueIterator rIter)
      throws IOException, InterruptedException {
    NativeRuntime.configure(job);

    Class keyClass = job.getMapOutputKeyClass();
    Class valueClass = job.getMapOutputValueClass();

    String finalName = OutputPathUtil.getOutputName(taskAttemptID.getTaskID().getId());
    FileSystem fs = FileSystem.get(job);
    RecordWriter<OK, OV> writer = job.getOutputFormat().getRecordWriter(
        fs, job, finalName, reporter);
    Class<OK> okeyClass = (Class<OK>) job.getOutputKeyClass();
    Class<OV> ovalueClass = (Class<OV>) job.getOutputValueClass();
    int bufferCapacity = job.getInt(
        NativeTaskConfig.NATIVE_PROCESSOR_BUFFER_KB,
        NativeTaskConfig.NATIVE_PROCESSOR_BUFFER_KB_DEFAULT) * 1024;
    ReducerProcessor<IK, IV, OK, OV> processor = 
        new ReducerProcessor<IK, IV, OK, OV>(
        bufferCapacity, bufferCapacity, keyClass, valueClass, okeyClass,
        ovalueClass, job, writer, reporter);
    processor.process(rIter);
    processor.close();
  }

  public static class ReducerProcessor<IK, IV, OK, OV> extends
      NativeBatchProcessor {
    enum KVState {
      KEY, VALUE
    }

    final private JobConf conf;
    final private RecordWriter<OK, OV> writer;
    final private Progressable progress;
    final private KVType iKType;
    final private KVType iVType;
    OK tmpKey;
    OV tmpValue;
    NativeDeserializer<OK> keyDeserializer;
    NativeDeserializer<OV> valueDeserializer;
    KVState state = KVState.KEY;

    public ReducerProcessor(int inputBufferCapacity, int outputBufferCapacity,
        Class<IK> iKClass, Class<IV> iVClass, Class<OK> oKClass, Class<OV> oVClass,
        JobConf conf, RecordWriter<OK, OV> writer,
        Progressable progress) throws IOException {
      super("RReducerHandler", inputBufferCapacity, outputBufferCapacity);
      this.iKType = KVType.getType(iKClass);
      this.iVType = KVType.getType(iVClass);
      this.conf = conf;
      this.writer = writer;
      this.progress = progress;
      tmpKey = ReflectionUtils.newInstance(oKClass, conf);
      tmpValue = ReflectionUtils.newInstance(oVClass, conf);
      keyDeserializer = NativeUtils.createDeserializer(oKClass);
      valueDeserializer = NativeUtils.createDeserializer(oVClass);
    }

    @Override
    protected boolean onProcess(ByteBuffer outputbuffer) throws IOException {
      while (outputbuffer.remaining() > 0) {
        switch (state) {
        case KEY:
          if (keyDeserializer.deserialize(tmpKey, outputbuffer) != null) {
            state = KVState.VALUE;
          }
          break;
        case VALUE:
          if (valueDeserializer.deserialize(tmpValue, outputbuffer) != null) {
            writer.write(tmpKey, tmpValue);
            state = KVState.KEY;
          }
          break;
        }
      }
      return true;
    }

    public void process(RawKeyValueIterator rIter) throws IOException {
      while (rIter.next()) {
        DataInputBuffer keyBuffer = rIter.getKey();
        int keyStart = keyBuffer.getPosition();
        switch (iKType) {
        case TEXT:
          keyStart += WritableUtils.decodeVIntSize(keyBuffer.getData()[keyBuffer.getPosition()]);
          break;
        case BYTES:
          keyStart += 4;
          break;
        }
        int keyLen = keyBuffer.getLength() - keyStart;
        DataInputBuffer valueBuffer = rIter.getValue();
        int valueStart = valueBuffer.getPosition();
        switch (iVType) {
        case TEXT:
          valueStart += WritableUtils.decodeVIntSize(valueBuffer.getData()[valueBuffer.getPosition()]);
          break;
        case BYTES:
          valueStart += 4;
          break;
        }
        int valueLen = valueBuffer.getLength() - valueStart;
        putInt(keyLen, valueLen);
        put(keyBuffer.getData(), keyStart, keyLen);
        put(valueBuffer.getData(), valueStart, valueLen);
      }
      rIter.close();
    }

    public void close() throws IOException, InterruptedException {
      if (!isFinished()) {
        finish();
      }
      releaseNative();
    }
  }
}
