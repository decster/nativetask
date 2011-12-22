package org.apache.hadoop.mapred.nativetask;

import java.io.IOException;

import junit.framework.TestCase;

public class TestJavaFileSystem extends TestCase {
  public void testMkdir() throws IOException {
    NativeRuntime.mkdirs(NativeUtils.toBytes("aaaaaaaaaaaaaaaaaaaa"));
  }
}
