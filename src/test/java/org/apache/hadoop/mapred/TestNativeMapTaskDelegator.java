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
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.NativeMapTaskDelegator.MapperOutputProcessor;
import org.apache.hadoop.mapred.NativeMapTaskDelegator.MapperProcessor;
import org.apache.hadoop.mapred.nativetask.NativeRuntime;
import org.apache.hadoop.mapred.nativetask.NativeTaskConfig;

import junit.framework.TestCase;

public class TestNativeMapTaskDelegator extends TestCase {
  private static Log LOG = LogFactory.getLog(TestNativeMapTaskDelegator.class);
  static int INPUT_SIZE = 500000;
  static int NUM_REDUCE = 100;
  List<Text> createData(int count, boolean random) {
    List<Text> ret = new ArrayList<Text>();
    for (int i = 0;i<count;i++) {
      ret.add(new Text(Integer.toString(i)));
    }
    if (random) {
      Collections.shuffle(ret,new Random(2));
    }
    return ret;
  }

  Text getValue(Text key) {
    return new Text(Integer.toString(Integer.parseInt(key.toString())*113));
  }

  RecordReader<Text, Text> createReader(final List<Text> data) {
    return new RecordReader<Text, Text>() {
      int pos;

      @Override
      public boolean next(Text key, Text value) throws IOException {
        if (pos<data.size()) {
          key.set(data.get(pos));
          value.set(getValue(key));
          pos++;
          return true;
        }
        return false;
      }

      @Override
      public float getProgress() throws IOException {
        if (data.size()==0) {
          return 1;
        }
        return pos/data.size();
      }

      @Override
      public long getPos() throws IOException {
        return pos;
      }

      @Override
      public Text createValue() {
        return new Text();
      }

      @Override
      public Text createKey() {
        return new Text();
      }

      @Override
      public void close() throws IOException {
      }
    };
  }

  public void testMapperOutputProcessor() throws IOException, InterruptedException {
    JobConf conf = new JobConf();
    conf.set("mapred.local.dir", "local");
    conf.setNumReduceTasks(NUM_REDUCE);
    conf.setBoolean(NativeTaskConfig.NATIVE_TASK_ENABLED, true);
    conf.setMapOutputKeyClass(Text.class);
    conf.setMapOutputValueClass(Text.class);
    conf.setInt("io.sort.mb", 8);

    assertTrue(NativeRuntime.isNativeLibraryLoaded());
    NativeRuntime.configure(conf);
    TaskAttemptID taskid = new TaskAttemptID("TestMapperOutputProcessor", 1, true, 0, 0);
    MapOutputFile mapOutputFile = new MapOutputFile(taskid.getJobID());
    mapOutputFile.setConf(conf);

    int bufferCapacity = conf.getInt(
        NativeTaskConfig.NATIVE_PROCESSOR_BUFFER_KB,
        NativeTaskConfig.NATIVE_PROCESSOR_BUFFER_KB_DEFAULT) * 1024;

    MapperOutputProcessor<Text, Text> processor = new MapperOutputProcessor<Text, Text>(
        bufferCapacity, Text.class, Text.class, conf, taskid);

    RecordReader<Text, Text> reader = createReader(createData(INPUT_SIZE, true));
    try {
      Text key = reader.createKey();
      Text value = reader.createValue();
      while (reader.next(key, value)) {
        processor.process(key, value);
      }
    } finally {
      processor.close();
    }

    // check output file correctness
    byte [] keyStore = new byte[INPUT_SIZE];
    Path outFile = mapOutputFile.getOutputFile(taskid);
    Path outIndex = mapOutputFile.getOutputIndexFile(taskid);
    SpillRecord spillRecord = new SpillRecord(outIndex, conf);
    FSDataInputStream dataIn = outFile.getFileSystem(conf).open(outFile);
    DataInputBuffer keyBuf = new DataInputBuffer();
    DataInputBuffer valueBuf = new DataInputBuffer();
    for (int i = 0; i < NUM_REDUCE; i++) {
      IndexRecord r = spillRecord.getIndex(i);
      IFile.Reader<Text, Text> ifileReader = new IFile.Reader<Text, Text>(conf, dataIn,
          r.partLength, null, null);
      Text lastKey = null;
      while (ifileReader.next(keyBuf, valueBuf)) {
        Text key = new Text();
        Text value = new Text();
        key.readFields(keyBuf);
        value.readFields(valueBuf);
        // check value correctness
        assertEquals(0, value.compareTo(getValue(key)));
        // check sorted
        if (lastKey != null) {
          assertTrue(lastKey.compareTo(key)<=0);
        }
        lastKey = key;
        // check partition
        int partition = (lastKey.hashCode() & 0x7fffffff) % NUM_REDUCE;
        assertEquals(i, partition);
        keyStore[Integer.parseInt(lastKey.toString())] = 1;
      }
    }
    // check doesn't miss any key
    for (int i=0;i<INPUT_SIZE;i++) {
      assertEquals(1, keyStore[i]);
    }
  }

  RecordWriter<Text, Text> createCheckDataWriter(final List<Text> data) {
    return new RecordWriter<Text, Text>() {
      List<Text> dest = new ArrayList<Text>();

      @Override
      public void write(Text key, Text value) throws IOException {
        assertEquals(data.get(dest.size()), key);
        assertEquals(getValue(data.get(dest.size())), value);
        dest.add(key);
      }

      @Override
      public void close(Reporter reporter) throws IOException {
        assertEquals(data.size(), dest.size());
      }
    };
  }

  public void testMapperProcessor() throws IOException, InterruptedException {
    JobConf conf = new JobConf();
    conf.setNumReduceTasks(0);
    conf.setBoolean(NativeTaskConfig.NATIVE_TASK_ENABLED, true);
    conf.setMapOutputKeyClass(Text.class);
    conf.setMapOutputValueClass(Text.class);

    assertTrue(NativeRuntime.isNativeLibraryLoaded());
    NativeRuntime.configure(conf);

    int bufferCapacity = conf.getInt(
        NativeTaskConfig.NATIVE_PROCESSOR_BUFFER_KB,
        NativeTaskConfig.NATIVE_PROCESSOR_BUFFER_KB_DEFAULT) * 1024;

    List<Text> data = createData(INPUT_SIZE, true);
    MapperProcessor<Text, Text, Text, Text> processor = new 
        MapperProcessor<Text, Text, Text, Text>(bufferCapacity, bufferCapacity,
            Text.class, Text.class, Text.class, Text.class, conf, 
            createCheckDataWriter(data));

    RecordReader<Text, Text> reader = createReader(data);
    try {
      Text key = reader.createKey();
      Text value = reader.createValue();
      while (reader.next(key, value)) {
        processor.process(key, value);
      }
    } finally {
      processor.close();
    }
  }
}
