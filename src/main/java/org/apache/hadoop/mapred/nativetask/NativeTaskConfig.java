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

public interface NativeTaskConfig {
  public static String MAPRED_MAPTASK_DELEGATOR_CLASS = "mapreduce.map.task.delegator.class";
  public static String MAPRED_REDUCETASK_DELEGATOR_CLASS = "mapreduce.reduce.task.delegator.class";
  public static String NATIVE_TASK_ENABLED = "native.task.enabled";
  public static String NATIVE_MAPOUTPUT_COLLECTOR_ENABLED = "native.mapoutput.collector.enabled";
  public static String NATIVE_LOG_DEVICE = "native.log.device";
  
  public static String NATIVE_MAPPER_CLASS = "native.mapper.class";
  public static String NATIVE_REDUCER_CLASS = "native.reducer.class";
  public static String NATIVE_PARTITIONER_CLASS = "native.partitioner.class";
  public static String NATIVE_COMBINER_CLASS = "native.combiner.class";
  
  public static String NATIVE_RECORDREADER_CLASS = "native.recordreader.class";
  public static String NATIVE_RECORDWRITER_CLASS = "native.recordwriter.class";

  public static String NATIVE_PROCESSOR_BUFFER_KB = "native.processor.buffer.kb";
  public static int NATIVE_PROCESSOR_BUFFER_KB_DEFAULT = 64;
  public static int NATIVE_ASYNC_PROCESSOR_BUFFER_KB_DEFAULT = 1024;

  enum RecordOrderType {
    FULLSORT,
    GROUPBY,
    NOSORT
  };

  public static String NATIVE_RECORD_ORDER = "native.record.order.type";

  enum SortType {
    CQSORT,
    CPPSORT,
  };

  public static String NATIVE_SORT_TYPE = "native.sort.type";

  enum CompressionType {
    PLAIN,
    SNAPPY
  };

  public static String NATIVE_COMPRESSION_TYPE = "native.compression.type";

  enum OutputFileType {
    INTERMEDIATE,
    IFILE
  };

  public static String NATIVE_MAPOUTPUT_FILE_TYPE = "native.mapoutput.file.type";
}
