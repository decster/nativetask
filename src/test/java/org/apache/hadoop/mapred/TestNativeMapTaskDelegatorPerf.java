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

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapOutputFile;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapred.nativetask.NativeRuntime;
import org.apache.hadoop.mapred.nativetask.NativeTaskConfig;
import org.apache.hadoop.mapred.nativetask.NativeMapTaskDelegator.MapperOutputProcessor;

import junit.framework.TestCase;

public class TestNativeMapTaskDelegatorPerf extends TestCase {
  final static int NUM_REDUCE = 100;
  final static int INPUTSIZE = 250000000;
  final static int KVSIZE = 64;

  public void perfMapperOutputProcessor(int numReduce, int kvSize, int inputSize)
      throws IOException, InterruptedException {
    JobConf conf = new JobConf();
    conf.set("mapred.local.dir", "local");
    conf.setNumReduceTasks(numReduce);
    conf.setBoolean(NativeTaskConfig.NATIVE_TASK_ENABLED, true);
    conf.setMapOutputKeyClass(Text.class);
    conf.setMapOutputValueClass(Text.class);
    conf.setCompressMapOutput(true);
    conf.set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.SnappyCodec");
    conf.setInt("io.sort.mb", 400);
    conf.setInt(NativeTaskConfig.NATIVE_PROCESSOR_BUFFER_KB, 128);
    

    assertTrue(NativeRuntime.isNativeLibraryLoaded());
    NativeRuntime.configure(conf);
    TaskAttemptID taskid = new TaskAttemptID("PerfMapperOutputProcessor", 1, true, 0, 0);
    MapOutputFile mapOutputFile = new MapOutputFile();
    mapOutputFile.setConf(conf);

    int bufferCapacity = conf.getInt(
        NativeTaskConfig.NATIVE_PROCESSOR_BUFFER_KB,
        NativeTaskConfig.NATIVE_PROCESSOR_BUFFER_KB_DEFAULT) * 1024;

    MapperOutputProcessor<Text, Text> processor = new MapperOutputProcessor<Text, Text>(
        bufferCapacity, Text.class, Text.class, conf, taskid);

    byte [] t = new byte[kvSize/2];
    for (int i=0;i<t.length;i++) {
      t[i] = 'K';
    }
    Text key = new Text(t);
    for (int i=0;i<t.length;i++) {
      t[i] = 'V';
    }
    Text value = new Text(t);
    byte [] d = key.getBytes();
    try {
      long s = System.currentTimeMillis();
      int aa = 1;
      while ((inputSize-=kvSize)>=0) {
        for (int i=0;i<kvSize/2;i++) {
          aa += 1;
          d[i] = (byte)(aa%26 + 'a');
        }
        processor.process(key, value);
      }
      long e = System.currentTimeMillis();
      System.out.printf("Fill time used: %.3f\n", (e-s)/1000.0f);
    } finally {
      processor.close();
    }
  }
  
  public void testMapperOutputProcessor() throws Exception {
    long s = System.currentTimeMillis();
    perfMapperOutputProcessor(NUM_REDUCE, KVSIZE, INPUTSIZE);
    long e = System.currentTimeMillis();
    System.out.printf("Total time used: %.3f\n", (e-s)/1000.0f);
  }
}
