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
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.mapred.JobConf;

public class NativeUtils {  
  public enum KVType {
    TEXT,
    BYTES,
    OTHER;
    public static KVType getType(Class<?> clazz) {
      if (clazz.equals(Text.class)) {
        return TEXT;
      } else if (clazz.equals(BytesWritable.class)) {
        return BYTES;
      } else {
        return OTHER;
      }
    }
  }

  public static boolean equals(byte [] a, byte [] b) {
    if (a.length != b.length) {
      return false;
    }
    for (int i = 0;i<a.length;i++) {
      if (a[i]!=b[i]) {
        return false;
      }
    }
    return true;
  }

  public static byte [] toBytes(String str) {
    if (str == null) {
      return null;
    }
    try {
      return str.getBytes("utf-8");
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e.getMessage());
    }
  }

  public static String fromBytes(byte [] data) {
    if (data == null) {
      return null;
    }
    try {
      return new String(data, "utf-8");
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e.getMessage());
    }
  }
  
  @SuppressWarnings("unchecked")
  public static <T> byte [] serialize(JobConf conf, Object obj) throws IOException {
    SerializationFactory factory = new SerializationFactory(conf);
    Serializer<T> serializer = (Serializer<T>) factory.getDeserializer(obj.getClass());
    DataOutputBuffer out = new DataOutputBuffer(1024);
    serializer.open(out);
    serializer.serialize((T)obj);
    byte [] ret = new byte[out.getLength()];
    System.arraycopy(out.getData(), 0, ret, 0, out.getLength());
    return ret;
  }

  @SuppressWarnings("unchecked")
  public static <T> NativeSerializer<T> createSerializer(Class<T> clazz)
      throws IOException {
    if (BinaryComparable.class.isAssignableFrom(clazz)) {
      return new BinarySerializer<T>();
    } else if (NullWritable.class.isAssignableFrom(clazz)) {
      return (NativeSerializer<T>)new NullWritableSerializer();
    } else if (LongWritable.class.isAssignableFrom(clazz)) {
      return (NativeSerializer<T>)new LongWritableSerializer();
    } else if (Writable.class.isAssignableFrom(clazz)) {
      return new WritableSerializer<T>();
    } else {
      throw new IOException("Type not supported for native serialization: "
          + clazz.getName());
    }
  }

  @SuppressWarnings("unchecked")
  public static <T> NativeDeserializer<T> createDeserializer(
      Class<T> clazz) throws IOException {
    if (Text.class.equals(clazz)) {
      return (NativeDeserializer<T>) (new TextDeserializer());
    } else if (BytesWritable.class.equals(clazz)) {
      return (NativeDeserializer<T>) (new BytesWritableDeserializer());
    } else if (NullWritable.class.equals(clazz)) {
      return (NativeDeserializer<T>) (new NullWritableDeserializer());
    } else if (LongWritable.class.equals(clazz)) {
      return (NativeDeserializer<T>) (new LongWritableDeserializer());
    } else if (Writable.class.isAssignableFrom(clazz)) {
      return new WritableDeserializer<T>();
    } else {
      throw new IOException("Type not supported for native deserialization: "
          + clazz.getName());
    }
  }

  public static abstract class NativeSerializer<T> {
    SerializeDest dest;
    ByteBuffer buffer;

    public void setDest(SerializeDest dest) {
      this.dest = dest;
      // reference not safe
      this.buffer = dest.getBuffer();
    }

    public abstract int getSerialLength(T obj) throws IOException;

    public abstract void serializeRaw(T obj, int len) throws IOException;

    public void serialize(T obj) throws IOException {
      int len = getSerialLength(obj);
      if (buffer.remaining()<4+len) {
        dest.flush();
      }
      buffer.putInt(len);
      serializeRaw(obj, len);
    }
  }

  public static interface SerializeDest {
    ByteBuffer getBuffer();
    void flush() throws IOException;
  }

  public static class NullWritableSerializer extends NativeSerializer<NullWritable> {
    @Override
    public int getSerialLength(NullWritable obj) throws IOException {
      return 0;
    };
    @Override
    public void serializeRaw(NullWritable obj, int len) throws IOException {
    };
  }

  public static class LongWritableSerializer extends NativeSerializer<LongWritable> {
    @Override
    public int getSerialLength(LongWritable obj) throws IOException {
      return 8;
    }
    @Override
    public void serializeRaw(LongWritable obj, int len) throws IOException {
      if (buffer.remaining()<8) {
        dest.flush();
      }
      buffer.putLong(Long.reverseBytes(obj.get()));
    }
  }

  public static class BinarySerializer<T> extends NativeSerializer<T> {
    @Override
    public int getSerialLength(T obj) throws IOException {
      return ((BinaryComparable)obj).getLength();
    };

    @Override
    public void serializeRaw(T obj, int len) throws IOException {
      BinaryComparable o = (BinaryComparable)obj;
      int rest = len;
      while (rest>0) {
        int remain = buffer.remaining();
        if (len <= remain) {
          buffer.put(o.getBytes(), len-rest, rest);
          return;
        }
        if (remain>0) {
          buffer.put(o.getBytes(), len-rest, remain);
          rest -= remain;
        }
        dest.flush();
      }
    }
  }

  public static class WritableSerializer<T> extends NativeSerializer<T> {
    private DataOutputBuffer tempOutput = new DataOutputBuffer();

    @Override
    public int getSerialLength(T robj) throws IOException {
      Writable obj = (Writable)robj;
      tempOutput.reset();
      obj.write(tempOutput);
      return tempOutput.getLength();
    };

    @Override
    public void serializeRaw(T obj, int len) throws IOException {
      int rest = len;
      while (rest>0) {
        int remain = buffer.remaining();
        if (len <= remain) {
          buffer.put(tempOutput.getData(), len-rest, rest);
          return;
        }
        if (remain>0) {
          buffer.put(tempOutput.getData(), len-rest, remain);
          rest -= remain;
        }
        dest.flush();
      }
    };
  }

  public static interface NativeDeserializer<T> {
    /**
     * Deserialize an object from buffer
     * @param src src object for reuse
     * @param buffer input buffer
     * @return object deserialized, null if deserialize incomplete
     */
    T deserialize(T src, ByteBuffer buffer) throws IOException;
  }

  public static class NullWritableDeserializer implements
      NativeDeserializer<NullWritable> {
    @Override
    public NullWritable deserialize(NullWritable src, ByteBuffer buffer)
        throws IOException {
      if (buffer.remaining()<4) {
        return null;
      }
      buffer.getInt();
      return src;
    }
  }

  public static class LongWritableDeserializer implements
      NativeDeserializer<LongWritable> {
    byte [] tempBuff = new byte[8];
    @Override
    public LongWritable deserialize(LongWritable src, ByteBuffer buffer)
        throws IOException {
      if (buffer.remaining()<12) {
        return null;
      }
      buffer.getInt();
      long v = buffer.getLong();
      src.set(Long.reverseBytes(v));
      return src;
    }
  }

  public static class TextDeserializer implements NativeDeserializer<Text> {
    int len=-1;
    int need;
    @Override
    public Text deserialize(Text src, ByteBuffer buffer) throws IOException {
      if (len == -1) {
        if (buffer.remaining()<4) {
          return null;
        }
        len = buffer.getInt();
        need = len;
        src.clear();
      }
      if (buffer.remaining() >= need) {
        src.append(buffer, need);
        len = -1;
        return src;
      }
      int cp = buffer.remaining();
      src.append(buffer, cp);
      need -= cp;
      return null;
    }
  }

  public static class BytesWritableDeserializer implements
      NativeDeserializer<BytesWritable> {
    int len=-1;
    int need;
    @Override
    public BytesWritable deserialize(BytesWritable src, ByteBuffer buffer) {
      if (len == -1) {
        if (buffer.remaining()<4) {
          return null;
        }
        len = buffer.getInt();
        need = len;
        src.setSize(len);
      }
      if (buffer.remaining() >= need) {
        if (need > 0) {
          buffer.get(src.getBytes(), len-need, need);
        }
        len = -1;
        return src;
      }
      int cp = buffer.remaining();
      buffer.get(src.getBytes(), len-need, cp);
      need -= cp;
      return null;
    }
  }

  public static class WritableDeserializer<T> implements NativeDeserializer<T> {
    int len=-1;
    int need;
    byte [] tempBuffer = new byte[256];
    DataInputBuffer serialInput = new DataInputBuffer();

    @Override
    public T deserialize(T src, ByteBuffer buffer) throws IOException {
      if (len == -1) {
        if (buffer.remaining()<4) {
          return null;
        }
        len = buffer.getInt();
        need = len;
        if (tempBuffer.length < len) {
          tempBuffer = new byte[len];
        }
      }
      if (buffer.remaining() >= need) {
        if (need>0) {
          buffer.get(tempBuffer, len-need, need);
        }
        serialInput.reset(tempBuffer, len);
        ((Writable)src).readFields(serialInput);
        len = -1;
        return src;
      }
      int cp = buffer.remaining();
      buffer.get(tempBuffer, len-need, cp);
      need -= cp;
      return null;
    }
  }
}
