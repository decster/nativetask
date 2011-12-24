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
import org.apache.hadoop.hdfs.web.resources.RecursiveParam;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.Text;
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
    int bufferCapacity = job.getInt(
        NativeTaskConfig.NATIVE_PROCESSOR_BUFFER_KB,
        NativeTaskConfig.NATIVE_PROCESSOR_BUFFER_KB_DEFAULT) * 1024;
    String finalName = OutputPathUtil.getOutputName(taskAttemptID.getTaskID().getId());

    if (job.get("native.recordwriter.class") != null) {
      // delegate whole reduce task
      NativeRuntime.set("native.output.file.name", finalName);
      ReduceTaskProcessor<IK, IV> processor = new ReduceTaskProcessor<IK, IV>(
          bufferCapacity, 0, keyClass, valueClass, job, reporter, rIter);
      processor.run();
    } else {
      FileSystem fs = FileSystem.get(job);
      RecordWriter<OK, OV> writer = job.getOutputFormat().getRecordWriter(
          fs, job, finalName, reporter);
      Class<OK> okeyClass = (Class<OK>) job.getOutputKeyClass();
      Class<OV> ovalueClass = (Class<OV>) job.getOutputValueClass();
      ReducerProcessor<IK, IV, OK, OV> processor =
          new ReducerProcessor<IK, IV, OK, OV>(
          bufferCapacity, bufferCapacity, keyClass, valueClass, okeyClass,
          ovalueClass, job, writer, reporter, rIter);
      processor.run();
      writer.close(reporter);
    }
  }

  public static class ReduceTaskProcessor<IK, IV> extends NativeBatchProcessor {
    final private JobConf conf;
    final private KVType iKType;
    final private KVType iVType;
    final private byte [] refillRet = new byte[4];
    RawKeyValueIterator rIter;
    DataInputBuffer keyBuffer;
    DataInputBuffer valueBuffer;
    int keyStart;
    int valueStart;
    int keyLen;
    int valueLen;
    int copied;
    enum FillState {
      NEW,
      KVLENGTH,
      KV,
    }
    FillState fillState;

    public ReduceTaskProcessor(int inputBufferCapacity, Class<IK> iKClass,
        Class<IV> iVClass, JobConf conf, Progressable progress,
        RawKeyValueIterator rIter) {
      this(inputBufferCapacity, 0, iKClass, iVClass, conf, progress, rIter);
    }

    public ReduceTaskProcessor(int inputBufferCapacity, int outputBufferCapacity, Class<IK> iKClass,
        Class<IV> iVClass, JobConf conf, Progressable progress,
        RawKeyValueIterator rIter) {
      super("NativeTask.RReducerHandler", inputBufferCapacity, outputBufferCapacity);
      this.iKType = KVType.getType(iKClass);
      this.iVType = KVType.getType(iVClass);
      this.conf = conf;
      this.fillState = FillState.NEW;
      this.rIter = rIter;
    }

    private byte [] currentInputBufferPosition() {
      int count = inputBuffer.position();
      refillRet[0] = (byte)(count & 0xff);
      refillRet[1] = (byte)((count>>8) & 0xff);
      refillRet[2] = (byte)((count>>16) & 0xff);
      refillRet[3] = (byte)((count>>24) & 0xff);
      return refillRet;
    }

    @Override
    protected byte[] sendCommandToJava(byte[] data) throws IOException {
      // must be refill
      if (data[0] != (byte)'r') {
        throw new IOException("command not support");
      }
      inputBuffer.position(0);
      while (true) {
        switch (fillState) {
        case NEW:
          if (!rIter.next()) {
            return currentInputBufferPosition();
          }
          keyBuffer = rIter.getKey();
          keyStart = keyBuffer.getPosition();
          switch (iKType) {
          case TEXT:
            keyStart += WritableUtils.decodeVIntSize(keyBuffer.getData()[keyBuffer.getPosition()]);
            break;
          case BYTES:
            keyStart += 4;
            break;
          }
          keyLen = keyBuffer.getLength() - keyStart;
          valueBuffer = rIter.getValue();
          valueStart = valueBuffer.getPosition();
          switch (iVType) {
          case TEXT:
            valueStart += WritableUtils.decodeVIntSize(valueBuffer.getData()[valueBuffer.getPosition()]);
            break;
          case BYTES:
            valueStart += 4;
            break;
          }
          valueLen = valueBuffer.getLength() - valueStart;
          if (inputBuffer.remaining() >= keyLen+valueLen+8) {
            inputBuffer.putInt(keyLen);
            inputBuffer.putInt(valueLen);
            inputBuffer.put(keyBuffer.getData(), keyStart, keyLen);
            inputBuffer.put(valueBuffer.getData(), valueStart, valueLen);
          } else {
            fillState = FillState.KVLENGTH;
            if (inputBuffer.position()>0) {
              // if already have some data, return those
              return currentInputBufferPosition();
            }
          }
          break;
        case KVLENGTH:
          inputBuffer.putInt(keyLen);
          inputBuffer.putInt(valueLen);
          copied = 0;
          fillState = FillState.KV;
          break;
        case KV:
          if (copied<keyLen) {
            int cp = Math.min(keyLen - copied, inputBuffer.remaining());
            if (cp>0) {
              inputBuffer.put(keyBuffer.getData(), keyStart+copied, cp);
              copied += cp;
            }
          }
          if (copied>=keyLen) {
            int vcopied = copied - keyLen;
            int cp = Math.min(valueLen - vcopied, inputBuffer.remaining());
            if (cp>0) {
              inputBuffer.put(valueBuffer.getData(), valueStart+vcopied, cp);
              copied += cp;
            }
            if (copied==keyLen+valueLen) {
              fillState = FillState.NEW;
            }
          }
          return currentInputBufferPosition();
        }
      }
    }

    public void run() throws IOException {
      try {
        command(NativeUtils.toBytes("run"));
      } catch (Exception e) {
        throw new IOException(e);
      }
      releaseNative();
    }
  }

  public static class ReducerProcessor<IK, IV, OK, OV> extends
      ReduceTaskProcessor<IK, IV> {
    enum KVState {
      KEY, VALUE
    }

    final private RecordWriter<OK, OV> writer;
    OK tmpKey;
    OV tmpValue;
    NativeDeserializer<OK> keyDeserializer;
    NativeDeserializer<OV> valueDeserializer;
    KVState state = KVState.KEY;

    public ReducerProcessor(int inputBufferCapacity, int outputBufferCapacity,
        Class<IK> iKClass, Class<IV> iVClass, Class<OK> oKClass, Class<OV> oVClass,
        JobConf conf, RecordWriter<OK, OV> writer,
        Progressable progress, RawKeyValueIterator rIter) throws IOException {
      super(inputBufferCapacity, outputBufferCapacity, iKClass, iVClass, conf, progress, rIter);
      this.writer = writer;
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
  }
}
