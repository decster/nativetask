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

import org.apache.hadoop.mapred.nativetask.NativeUtils.SerializeDest;
import org.apache.hadoop.mapred.nativetask.NativeUtils.NativeSerializer;

public class KeyValueBatchProcessor<K, V>
    extends NativeBatchProcessor {
  private final NativeSerializer<K> keySerializer;
  private final NativeSerializer<V> valueSerializer;

  public KeyValueBatchProcessor(String nativeHandlerName, int inputBufferCapacity,
      int outputBufferCapacity, Class<K> keyClazz, Class<V> valueClazz) throws IOException {
    super(nativeHandlerName, inputBufferCapacity, outputBufferCapacity);
    keySerializer = NativeUtils.createSerializer(keyClazz);
    valueSerializer = NativeUtils.createSerializer(valueClazz);
    final SerializeDest dest = new SerializeDest() {
      @Override
      public ByteBuffer getBuffer() {
        return getInputBuffer();
      }

      @Override
      public void flush() throws IOException {
        flushInput();
      }
    };
    keySerializer.setDest(dest);
    valueSerializer.setDest(dest);
  }

  public void process(K key, V value) throws IOException, InterruptedException {
    int keyLen = keySerializer.getSerialLength(key);
    int valueLen = valueSerializer.getSerialLength(value);
    if (inputBuffer.position()>0 && 
        inputBuffer.remaining()<keyLen+valueLen+8) {
      flushInput();
    }
    inputBuffer.putInt(keyLen);
    inputBuffer.putInt(valueLen);
    keySerializer.serializeRaw(key, keyLen);
    valueSerializer.serializeRaw(value, valueLen);
  };

  public void close() throws IOException, InterruptedException {
    if (!isFinished()) {
      finish();
    }
    releaseNative();
  }
}
