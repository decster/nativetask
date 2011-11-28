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
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.NativeReduceTaskDelegator.ReducerProcessor;
import org.apache.hadoop.mapred.nativetask.NativeRuntime;
import org.apache.hadoop.mapred.nativetask.NativeTaskConfig;
import org.apache.hadoop.util.Progress;
import org.apache.hadoop.util.Progressable;

import junit.framework.TestCase;

public class TestNativeReduceTaskDelegator extends TestCase {
  private static Log LOG = LogFactory.getLog(TestNativeReduceTaskDelegator.class);
  static int INPUT_SIZE = 500000;
  static int GROUP_SIZE =  48697;

  List<Text[]> createData(int count) {
    List<Text[]> ret = new ArrayList<Text[]>();
    for (int i = 0;i<count;i++) {
      Text key = new Text(Integer.toString(i/GROUP_SIZE));
      Text value = new Text(Integer.toString(i)+":"+key.toString());
      ret.add(new Text[]{key,value});
    }
    return ret;
  }

  boolean checkValue(Text key, Text value) {
    return value.toString().endsWith(":"+key.toString());
  }

  static class NullProgress implements Progressable {
    public void progress() { }
  }

  RawKeyValueIterator createRawKeyValueIterator(final List<Text[]> data) {
    return new RawKeyValueIterator() {
      int index = -1;
      DataInputBuffer keyBuffer = new DataInputBuffer();
      DataInputBuffer valueBuffer = new DataInputBuffer();
      DataOutputBuffer keyOutputBuffer = new DataOutputBuffer();
      DataOutputBuffer valueOutputBuffer = new DataOutputBuffer();

      @Override
      public boolean next() throws IOException {
        if (index == data.size()) {
          return false;
        }
        index++;
        if (index < data.size()) {
          return true;
        }
        return false;
      }

      @Override
      public DataInputBuffer getValue() throws IOException {
        Text cur = data.get(index)[1];
        valueOutputBuffer.reset();
        cur.write(valueOutputBuffer);
        valueBuffer.reset(valueOutputBuffer.getData(), valueOutputBuffer.getLength());
        return valueBuffer;
      }

      @Override
      public Progress getProgress() {
        return null;
      }

      @Override
      public DataInputBuffer getKey() throws IOException {
        Text cur = data.get(index)[0];
        keyOutputBuffer.reset();
        cur.write(keyOutputBuffer);
        keyBuffer.reset(keyOutputBuffer.getData(), keyOutputBuffer.getLength());
        return keyBuffer;
      }

      @Override
      public void close() throws IOException {
      }
    };
  }

  RecordWriter<Text, Text> createCheckRecordWriter(final List<Text[]> data) {
    return new RecordWriter<Text, Text>() {
      int index=0;

      @Override
      public void write(Text key, Text value) throws IOException {
        assertEquals(data.get(index)[0], key);
        assertTrue(checkValue(key, value));
        index++;
      }

      @Override
      public void close(Reporter reporter) throws IOException {
        assertEquals(data.size(), index);
      }
    };
  }

  public void testPassiveReducerProcessor() throws Exception {
    JobConf conf = new JobConf();
    conf.set("mapred.local.dir", "local");
    conf.setBoolean(NativeTaskConfig.NATIVE_TASK_ENABLED, true);
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);
    // use passive reducer
    conf.set(NativeTaskConfig.NATIVE_REDUCER_CLASS, "Mapper");

    assertTrue(NativeRuntime.isNativeLibraryLoaded());
    NativeRuntime.configure(conf);

    int bufferCapacity = conf.getInt(
        NativeTaskConfig.NATIVE_PROCESSOR_BUFFER_KB,
        NativeTaskConfig.NATIVE_PROCESSOR_BUFFER_KB_DEFAULT) * 1024;

    List<Text[]> data = createData(INPUT_SIZE);
    RecordWriter<Text, Text> writer = createCheckRecordWriter(data);
    RawKeyValueIterator rIter = createRawKeyValueIterator(data);

    ReducerProcessor<Text, Text, Text, Text> processor =
        new ReducerProcessor<Text, Text, Text, Text>(bufferCapacity, bufferCapacity,
            Text.class, Text.class, Text.class, Text.class, conf, writer, new NullProgress());
    processor.process(rIter);
    processor.close();
  }

  public void testActiveReducerProcessor() throws Exception {
    JobConf conf = new JobConf();
    conf.setBoolean(NativeTaskConfig.NATIVE_TASK_ENABLED, true);
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);
    // use active reducer
    conf.set(NativeTaskConfig.NATIVE_REDUCER_CLASS, "Reducer");

    assertTrue(NativeRuntime.isNativeLibraryLoaded());
    NativeRuntime.configure(conf);

    int bufferCapacity = conf.getInt(
        NativeTaskConfig.NATIVE_PROCESSOR_BUFFER_KB,
        NativeTaskConfig.NATIVE_PROCESSOR_BUFFER_KB_DEFAULT) * 1024;

    List<Text[]> data = createData(INPUT_SIZE);
    RecordWriter<Text, Text> writer = createCheckRecordWriter(data);
    RawKeyValueIterator rIter = createRawKeyValueIterator(data);

    ReducerProcessor<Text, Text, Text, Text> processor =
        new ReducerProcessor<Text, Text, Text, Text>(bufferCapacity, bufferCapacity,
            Text.class, Text.class, Text.class, Text.class, conf, writer, new NullProgress());
    processor.process(rIter);
    processor.close();
  }
}
