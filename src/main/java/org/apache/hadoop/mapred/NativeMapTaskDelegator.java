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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapred.TaskUmbilicalProtocol;
import org.apache.hadoop.mapred.Task.TaskReporter;
import org.apache.hadoop.mapred.nativetask.KeyValueBatchProcessor;
import org.apache.hadoop.mapred.nativetask.NativeRuntime;
import org.apache.hadoop.mapred.nativetask.NativeTaskConfig;
import org.apache.hadoop.mapred.nativetask.NativeUtils;
import org.apache.hadoop.mapred.nativetask.NativeUtils.NativeDeserializer;
import org.apache.hadoop.util.ReflectionUtils;

public class NativeMapTaskDelegator<INKEY, INVALUE, OUTKEY, OUTVALUE> {
  private static final Log LOG = LogFactory.getLog(NativeMapTaskDelegator.class);

  public NativeMapTaskDelegator() {
  }
  
  @SuppressWarnings("unchecked")
  public void run(final TaskAttemptID taskAttemptID, final JobConf job,
      final TaskUmbilicalProtocol umbilical, TaskReporter reporter,
      final Object split) throws IOException,
      InterruptedException {
    NativeRuntime.configure(job);

    RecordReader<INKEY,INVALUE> rawIn =
      job.getInputFormat().getRecordReader((InputSplit)split, job, reporter);

    INKEY key = rawIn.createKey();
    INVALUE value = rawIn.createValue();
    Class<INKEY> ikeyClass = (Class<INKEY>)key.getClass();
    Class<INVALUE> ivalueClass = (Class<INVALUE>)value.getClass();

    int bufferCapacity = job.getInt(
        NativeTaskConfig.NATIVE_PROCESSOR_BUFFER_KB,
        NativeTaskConfig.NATIVE_PROCESSOR_BUFFER_KB_DEFAULT) * 1024;

    int numReduceTasks = job.getNumReduceTasks();
    LOG.info("numReduceTasks: " + numReduceTasks);
    if (numReduceTasks > 0) {
      MapperOutputProcessor<INKEY, INVALUE> processor = 
          new MapperOutputProcessor<INKEY, INVALUE>(
              bufferCapacity, ikeyClass, ivalueClass, job, taskAttemptID);
      try {
        while (rawIn.next(key, value)) {
          processor.process(key, value);
        }
      } finally {
        processor.close();
      }
    } else {
      String finalName = Task.getOutputName(taskAttemptID.getTaskID().getId());
      FileSystem fs = FileSystem.get(job);
      RecordWriter<OUTKEY, OUTVALUE> writer = job.getOutputFormat()
          .getRecordWriter(fs, job, finalName, reporter);
      Class<OUTKEY> okeyClass = (Class<OUTKEY>) job.getOutputKeyClass();
      Class<OUTVALUE> ovalueClass = (Class<OUTVALUE>) job.getOutputValueClass();
      MapperProcessor<INKEY, INVALUE, OUTKEY, OUTVALUE> processor = 
          new MapperProcessor<INKEY, INVALUE, OUTKEY, OUTVALUE>(
              bufferCapacity, bufferCapacity, ikeyClass, ivalueClass, okeyClass,
              ovalueClass, job, writer);
      try {
        while (rawIn.next(key, value)) {
          processor.process(key, value);
        }
        writer.close(reporter);
      } finally {
        processor.close();
      }
    }
  }

  /**
   * Mapper processor with partitioner, output collector, and maybe combiner 
   */
  static class MapperOutputProcessor<IK, IV>
      extends KeyValueBatchProcessor<IK, IV> {
    private MapOutputFile mapOutputFile;
    private TaskAttemptID taskAttemptID;
    private int spillNumber = 0;

    public MapperOutputProcessor(int bufferCapacity, Class<IK> keyClass,
        Class<IV> valueClass, JobConf conf, TaskAttemptID taskAttemptID)
        throws IOException {
      super("MMapperHandler", bufferCapacity, 0, keyClass, valueClass);
      this.mapOutputFile = new MapOutputFile();
      this.mapOutputFile.setConf(conf);
      this.taskAttemptID = taskAttemptID;
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
  }

  /**
   * Mapper only processor
   */
  static class MapperProcessor<IK, IV, OK, OV>
      extends KeyValueBatchProcessor<IK, IV> {
    enum KVState {
      KEY,
      VALUE
    }
    RecordWriter<OK, OV> writer;
    OK tmpKey;
    OV tmpValue;
    NativeDeserializer<OK> keyDeserializer;
    NativeDeserializer<OV> valueDeserializer;
    KVState state = KVState.KEY;

    public MapperProcessor(int inputBufferCapacity, int outputBufferCapacity,
        Class<IK> iKClass, Class<IV> iVClass, Class<OK> oKClass, Class<OV> oVClass,
        JobConf conf, RecordWriter<OK, OV> writer) throws IOException {
      super("MMapperHandler", inputBufferCapacity, outputBufferCapacity, iKClass, iVClass);
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
          if (keyDeserializer.deserialize(tmpKey, outputbuffer)!=null) {
            state = KVState.VALUE;
          }
          break;
        case VALUE:
          if (valueDeserializer.deserialize(tmpValue, outputbuffer)!=null) {
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
