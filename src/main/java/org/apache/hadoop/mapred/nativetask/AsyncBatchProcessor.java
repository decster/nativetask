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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class AsyncBatchProcessor extends NativeBatchProcessor {
  private static Log LOG = LogFactory.getLog(AsyncBatchProcessor.class);
  static byte [] RUN = NativeUtils.toBytes("RUN");

  final Thread javaSideThread;
  volatile boolean javaSide;

  public AsyncBatchProcessor(String handlerName, int inputBufferCapacity,
      int outputBufferCapacity) {
    super(handlerName, inputBufferCapacity, outputBufferCapacity);
    javaSide = true;
    javaSideThread = Thread.currentThread();
  }

  protected synchronized void getBuffer(boolean isJavaSide) throws IOException {
    try {
      while (javaSide != isJavaSide) {
        this.wait();
      }
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }
  
  protected synchronized void releaseBuffer(boolean isJavaSide) throws IOException {
    javaSide = !isJavaSide;
    this.notify();
  }

  @Override
  protected byte[] sendCommandToJava(byte[] data) throws IOException {
    releaseBuffer(false);
    getBuffer(false);
    return null;
  }

  @Override
  public void flushInput() throws IOException {
    super.flushInput();
    releaseBuffer(true);
    getBuffer(true);
  }

  protected void runNativeSide() throws Exception {
    command(RUN);
  }

  protected void runJavaSide() throws Exception {
  }

  public void process() throws Exception {
    Thread nativeSideThread = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          getBuffer(false);
          try {
            runNativeSide();
          } finally {
            releaseBuffer(false);
          }
        } catch (InterruptedException e) {
          // java side error, ignore
        } catch (Exception e) {
          // native side error, interrupt java side
          LOG.warn("Native side error");
          LOG.warn(e);
          LOG.warn("Interrupt java side thread");
          javaSideThread.interrupt();
        }
      }
    });
    try {
      nativeSideThread.start();
      getBuffer(true);
      runJavaSide();
      if (!isFinished()) {
        finish();
      }
      releaseBuffer(true);
      nativeSideThread.join();
    } catch (InterruptedException e) {
      // native side error
      throw new IOException(e);
    } catch (Exception e) {
      // java side error
      if (nativeSideThread.isAlive()) {
        nativeSideThread.interrupt();
        try {
          LOG.warn("Wait for native side to finish");
          nativeSideThread.join();
        } catch (InterruptedException e1) {
          // should not be here
          LOG.warn("Should not be interrupted");
        }
      }
      throw e;
    } finally {
      releaseNative();
    }
  }
}
