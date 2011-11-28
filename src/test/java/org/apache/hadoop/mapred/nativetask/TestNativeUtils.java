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
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapred.nativetask.NativeUtils.NativeDeserializer;
import org.apache.hadoop.mapred.nativetask.NativeUtils.SerializeDest;
import org.apache.hadoop.mapred.nativetask.NativeUtils.NativeSerializer;
import org.apache.hadoop.util.ReflectionUtils;

import junit.framework.TestCase;

public class TestNativeUtils extends TestCase {
  public <T> void ClassSerDeTest(final Class<T> clazz, final List<T> data) throws IOException {
    final ByteBuffer buffer = ByteBuffer.allocateDirect(1024);
    final List<T> deserializedObjects = new ArrayList<T>();
    final NativeSerializer<T> serializer = NativeUtils.createSerializer(clazz);
    final NativeDeserializer<T> deserializer = NativeUtils.createDeserializer(clazz);
    SerializeDest dest = new SerializeDest() {
      public int flushTime;
      public int written;
      public T obj;
      @Override
      public ByteBuffer getBuffer() {
        return buffer;
      }
      
      @Override
      public void flush() throws IOException {
        written+=buffer.position();
        buffer.limit(buffer.position());
        buffer.position(0);
        while (buffer.remaining()>0) {
          if (obj==null) {
            obj = ReflectionUtils.newInstance(clazz, null);
          }
          if (null != deserializer.deserialize(obj, buffer)) {
            deserializedObjects.add(obj);
            obj = null;
          }
        }
        buffer.position(0);
        buffer.limit(buffer.capacity());
        flushTime++;
      }
    };
    serializer.setDest(dest);
    for (T o : data) {
      serializer.serialize(o);
    }
    dest.flush();
    assertEquals(0, buffer.position());
    assertEquals(data.size(), deserializedObjects.size());
    for (int i=0;i<data.size();i++) {
      assertEquals(data.get(i), deserializedObjects.get(i));
    }
  }
  
  static int LEN = 1000;

  List<BytesWritable> getBytesWritable(int len) {
    List<BytesWritable> ret = new ArrayList<BytesWritable>();
    for (int i = 0;i < len;i++) {
      ret.add(new BytesWritable(String.format("%037d", i*11234).getBytes()));
    }
    return ret;
  }

  List<Text> getText(int len) {
    List<Text> ret = new ArrayList<Text>();
    for (int i = 0;i < len;i++) {
      ret.add(new Text(String.format("%023d", i*11234).getBytes()));
    }
    return ret;
  }

  List<NullWritable> getNullWritable(int len) {
    List<NullWritable> ret = new ArrayList<NullWritable>();
    for (int i = 0;i < len;i++) {
      ret.add(NullWritable.get());
    }
    return ret;
  }
  
  List<LongWritable> getLongWritable(int len) {
    List<LongWritable> ret = new ArrayList<LongWritable>();
    for (int i = 0;i < len;i++) {
      ret.add(new LongWritable(i*13313914L));
    }
    return ret;
  }
  
  // used for generic Writable test
  List<VLongWritable> getVLongWritable(int len) {
    List<VLongWritable> ret = new ArrayList<VLongWritable>();
    for (int i = 0;i < len;i++) {
      ret.add(new VLongWritable(i*13313914L));
    }
    return ret;
  }  

  public void testNativeSerDeser() throws IOException {
    ClassSerDeTest(BytesWritable.class, getBytesWritable(LEN));
    ClassSerDeTest(Text.class, getText(LEN));
    ClassSerDeTest(NullWritable.class, getNullWritable(LEN));
    ClassSerDeTest(LongWritable.class, getLongWritable(LEN));
    ClassSerDeTest(VLongWritable.class, getVLongWritable(LEN));
  }
}
