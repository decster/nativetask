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

import org.apache.hadoop.io.WritableComparator;

import junit.framework.TestCase;

public class TestNativeBatchProcessor extends TestCase {
  static int BUFFER_SIZE = 128 * 1024;
  static class EchoBatchProcessor extends NativeBatchProcessor {
    public ArrayList<byte []> tempOutput = new ArrayList<byte []>();
    public boolean finished = false;

    public EchoBatchProcessor() {
      init("EchoBatchHandler", BUFFER_SIZE, BUFFER_SIZE);
    }

    @Override
    protected boolean onProcess(ByteBuffer outputbuffer) {
      byte [] temp = new byte[outputbuffer.remaining()];
      outputbuffer.get(temp);
      System.out.printf("java side got output of native side size %d\n", temp.length);
      tempOutput.add(temp);
      return true;
    }

    @Override
    protected byte[] sendCommandToJava(byte[] data) throws IOException {
      return NativeUtils.toBytes("Java:" + NativeUtils.fromBytes(data));
    }

    @Override
    protected boolean onFinish() {
      finished = true;
      return true;
    }
  }

  byte[] createData(byte b, int len) {
    byte[] ret = new byte[len];
    for (int i = 0; i < ret.length; i++) {
      ret[i] = b;
    }
    return ret;
  }

  byte [][] createDatas(int start, int end, int len) {
    byte [][] ret = new byte[end-start][];
    for (int i = start;i<end;i++) {
      ret[i-start] = createData((byte)(i%26 + 'a'), len);
    }
    return ret;
  }

  byte [] join(byte [][] data) {
    int len = 0;
    for (byte [] d:data) {
      len += d.length;
    }
    byte [] ret = new byte[len];
    int cur = 0;
    for (byte[] d:data) {
      System.arraycopy(d, 0, ret, cur, d.length);
      cur += d.length;
    }
    return ret;
  }

  byte [] join(List<byte[]> data) {
    return join((byte[][]) data.toArray(new byte[data.size()][]));
  }

  public void testNullOutputBuffer() throws Exception {
    // just check dummy BatchProcessor without output buffer won't have any problem
    assertTrue(NativeRuntime.isNativeLibraryLoaded());
    NativeBatchProcessor processor = new NativeBatchProcessor("BatchHandler", 64 * 1024, 0);
    byte [] ret = processor.command(NativeUtils.toBytes("asdfasdf"));
    byte [][] inputs = createDatas(0, 1023, 329);
    for (byte []input:inputs) {
      processor.put(input, 0, input.length);
    }
    processor.finish();
  }

  public void testEchoStream() throws Exception {
    assertTrue(NativeRuntime.isNativeLibraryLoaded());
    EchoBatchProcessor echo = new EchoBatchProcessor();
    // test command
    byte [] ret = echo.command(NativeUtils.toBytes("asdfasdf"));
    String retstr = NativeUtils.fromBytes(ret);
    assertEquals("Java:Echoed:asdfasdf", retstr);
    // test data
    byte [][] inputs = createDatas(0, 1023, 329);
    for (byte []input:inputs) {
      echo.put(input, 0, input.length);
    }
    echo.finish();
    ArrayList<byte []> outputs = echo.tempOutput;
    assertTrue(echo.finished);
    byte [] inputBytes = join(inputs);
    byte [] outputBytes = join(outputs);
    int result = WritableComparator.compareBytes(inputBytes, 0, inputBytes.length,
        outputBytes, 0, outputBytes.length);
    assertEquals(0, result);
  }
}
