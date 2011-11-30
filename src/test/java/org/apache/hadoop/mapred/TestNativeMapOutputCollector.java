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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.IFile;
import org.apache.hadoop.mapred.IndexRecord;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapOutputFile;
import org.apache.hadoop.mapred.SpillRecord;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapred.nativetask.NativeMapOutputCollector;
import org.apache.hadoop.mapred.nativetask.NativeRuntime;

import junit.framework.TestCase;

public class TestNativeMapOutputCollector extends TestCase {
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

  public void testCollect() throws Exception {
    JobConf conf = new JobConf();
    conf.set("mapred.local.dir", "local");
    conf.setNumReduceTasks(NUM_REDUCE);
    conf.setBoolean("mapred.native.map.output.collector", true);
    conf.setMapOutputKeyClass(Text.class);
    conf.setMapOutputValueClass(Text.class);
    conf.setInt("io.sort.mb", 8);

    assertTrue(NativeRuntime.isNativeLibraryLoaded());
    NativeRuntime.configure(conf);
    TaskAttemptID taskid = new TaskAttemptID("TestNativeMapOutpputCollector", 1, true, 0, 0);
    MapOutputFile mapOutputFile = new MapOutputFile();
    mapOutputFile.setConf(conf);
    NativeMapOutputCollector<Text, Text> collector = 
        new NativeMapOutputCollector<Text, Text>(conf, taskid);
    List<Text> inputData = createData(INPUT_SIZE, true);
    for (Text k: inputData) {
      int partition = (k.hashCode() & 0x7fffffff) % NUM_REDUCE;
      collector.collect(k, getValue(k), partition);
    }
    collector.flush();
    collector.close();
    
    // check output file correctness
    byte [] keyStore = new byte[INPUT_SIZE];
    Path outFile = mapOutputFile.getOutputFile();
    Path outIndex = mapOutputFile.getOutputIndexFile();
    SpillRecord spillRecord = new SpillRecord(outIndex, conf, null);
    FSDataInputStream dataIn = outFile.getFileSystem(conf).open(outFile);
    DataInputBuffer keyBuf = new DataInputBuffer();
    DataInputBuffer valueBuf = new DataInputBuffer();
    for (int i = 0; i < NUM_REDUCE; i++) {
      IndexRecord r = spillRecord.getIndex(i);
      IFile.Reader<Text, Text> reader = new IFile.Reader<Text, Text>(conf, dataIn,
          r.partLength, null, null);    
      Text lastKey = null;
      while (reader.next(keyBuf, valueBuf)) {
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
}

