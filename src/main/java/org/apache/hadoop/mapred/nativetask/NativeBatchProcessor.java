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
import java.nio.ByteOrder;

/**
 * Provide a shared memory, synchronized, batch data processing framework,
 * also including interfaces for passing control messages. These interfaces
 * should be used to implement varies of java/native interactions, so JNI
 * can be isolated in some core classes, namely <code>NativeRuntime</code>
 * and this class.
 */
public class NativeBatchProcessor {
  protected ByteBuffer inputBuffer;
  protected ByteBuffer outputBuffer;
  protected String nativeHandlerName;
  protected long nativeHandlerAddr;
  protected boolean finished = false;

  public NativeBatchProcessor() {
  }

  public NativeBatchProcessor(String nativeHandlerName, int inputBufferCapacity,
      int outputBufferCapacity) {
    init(nativeHandlerName, inputBufferCapacity, outputBufferCapacity);
  }

  void init(String nativeHandlerName, int inputBufferCapacity,
      int outputBufferCapacity) {
    if (inputBufferCapacity > 0) {
      this.inputBuffer = ByteBuffer.allocateDirect(inputBufferCapacity);
      this.inputBuffer.order(ByteOrder.LITTLE_ENDIAN);
    }
    if (outputBufferCapacity > 0) {
      this.outputBuffer = ByteBuffer.allocateDirect(outputBufferCapacity);
      this.outputBuffer.order(ByteOrder.LITTLE_ENDIAN);
    }
    this.nativeHandlerAddr = NativeRuntime.createNativeObject(nativeHandlerName);
    if (this.nativeHandlerAddr == 0) {
      throw new RuntimeException("Native object create failed, class: "
          + nativeHandlerName);
    }
    setupHandler(nativeHandlerAddr);    
  }

  protected synchronized void releaseNative() {
    if (nativeHandlerAddr != 0) {
      NativeRuntime.releaseNativeObject(nativeHandlerAddr);
      nativeHandlerAddr = 0;
    }
  }

  public boolean isFinished() {
    return finished;
  }

  @Override
  protected void finalize() throws Throwable {
    releaseNative();
    super.finalize();
  }

  public ByteBuffer getInputBuffer() {
    return inputBuffer;
  }

  public ByteBuffer getOutputBuffer() {
    return outputBuffer;
  }

  // /////////////////////////////////////////////////////////
  // methods for user(upstream)
  // /////////////////////////////////////////////////////////
  /**
   * utility method to stream data into input buffer
   *
   * @param buffer
   * @param offset
   * @param length
   */
  public void put(byte[] buffer, int offset, int length) throws IOException {
    while (length > 0) {
      if (inputBuffer.remaining() < length && inputBuffer.position() > 0) {
        flushInput();
      }
      int cp = Math.min(inputBuffer.remaining(), length);
      inputBuffer.put(buffer, offset, cp);
      offset += cp;
      length -= cp;
    }
  }

  /**
   * Put a little-endian 4 byte integer to input buffer
   * @param val
   */
  public void putInt(int val) throws IOException {
    if (inputBuffer.remaining()<4) {
      flushInput();
    }
    inputBuffer.putInt(val);
  }

  /**
   * Put two little-endian 4 byte integer to input buffer
   * @param val1
   * @param val2
   */
  public void putInt(int val1, int val2) throws IOException {
    if (inputBuffer.remaining()<8) {
      flushInput();
    }
    inputBuffer.putInt(val1);
    inputBuffer.putInt(val2);
  }
  
  /**
   * Called by user, finish input
   */
  public void finish() throws IOException {
    if (inputBuffer.position()>0) {
      flushInput();
    }
    nativeFinish(nativeHandlerAddr);
    finished = true;
  }

  /**
   * Called by user, flush input
   */
  public void flushInput() throws IOException {
    nativeProcessInput(nativeHandlerAddr, inputBuffer.position());
    inputBuffer.position(0);
  }

  public byte[] command(byte [] cmd) throws Exception {
    return nativeCommand(nativeHandlerAddr, cmd);
  }

  // /////////////////////////////////////////////////////////
  // methods that subclass should override
  // /////////////////////////////////////////////////////////
  /**
   * Provided by subclass, process native side output
   *
   * @param outputbuffer
   * @return true if succeed
   */
  protected boolean onProcess(ByteBuffer outputbuffer) throws IOException {
    return true;
  }

  /**
   * Provided by subclass, on native side finished
   *
   * @return true if succeed
   */
  protected boolean onFinish() {
    return true;
  }

  // /////////////////////////////////////////////////////////
  // methods for setup native side
  // /////////////////////////////////////////////////////////
  /**
   * Setup native side BatchHandler
   */
  private native void setupHandler(long nativeHandlerAddr);

  // /////////////////////////////////////////////////////////
  // methods for data transfer
  // /////////////////////////////////////////////////////////
  /**
   * Called by native side, clean output buffer so native side can continue
   * processing
   */
  public void flushOutput(int length) throws IOException {
    outputBuffer.position(0);
    outputBuffer.limit(length);
    onProcess(outputBuffer);
  }

  /**
   * Called by native side, indicates native side has finished.
   */
  public void finishOutput() {
    onFinish();
  }

  /**
   * Let native side to process data in inputBuffer
   *
   * @param handler
   * @param length
   */
  private native void nativeProcessInput(long handler, int length);

  /**
   * Notice native side input is finished
   *
   * @param handler
   */
  private native void nativeFinish(long handler);

  /**
   * Send control message to native side
   *
   * @param cmd
   *          command data
   * @return return value
   */
  private native byte[] nativeCommand(long handler, byte[] cmd);

  /**
   * Called by native side, send control message to java side,
   * Subclass can override it if needed
   *
   * @param cmd
   *          command id
   * @param data
   *          command data
   * @return 0 if succeed
   */
  protected byte[] sendCommandToJava(byte[] data) throws IOException {
    return null;
  }

  /**
   * Cache JNI field & method ids
   */
  private static native void InitIDs();

  static {
    InitIDs();
  }
}
