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
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;

public class NativeTeraSort implements Tool {
  protected static final Log LOG = LogFactory.getLog(NativeTeraSort.class);

  static final String PARTITION_FILENAME = "_nativepartition.lst";
  Configuration conf;

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public int run(String[] args) throws Exception {
    JobConf job = getConf() == null ? new JobConf() : new JobConf(getConf());

    GenericOptionsParser genericParser = new GenericOptionsParser(job, args);

    String [] remains = genericParser.getRemainingArgs();
    setConf(job);

    Path inputpath = new Path(remains[0]);
    Path outputpath = new Path(remains[1]);
    Path partitionFile = new Path(inputpath, PARTITION_FILENAME);
    URI partitionUri = new URI(partitionFile.toString() +
        "#" + PARTITION_FILENAME);
    FileInputFormat.setInputPaths(job, inputpath);
    FileOutputFormat.setOutputPath(job, outputpath);
    job.setJobName("NativeTeraSort");
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.set("native.recordreader.class", "NativeTask.TeraRecordReader");
    job.set("native.recordwriter.class", "NativeTask.TeraRecordWriter");
    job.set("native.partitioner.class", "NativeTask.TeraPartitioner");
    writePartitionFile(job, partitionFile);
    DistributedCache.addCacheFile(partitionUri, job);
    DistributedCache.createSymlink(job);
    job.setInt("dfs.replication", 1);
    Submitter.setupNativeJob(job);
    JobClient.runJob(job);
    LOG.info("done");
    return 0;
  }
  
  static final String SAMPLE_SIZE = "terasort.partitions.sample";

  private void writePartitionFile(JobConf conf, 
      Path partFile) throws IOException {
    TextInputFormat inFormat = new TextInputFormat();
    LongWritable key = new LongWritable();
    Text value = new Text();
    int partitions = conf.getNumReduceTasks();
    long sampleSize = conf.getLong(SAMPLE_SIZE, 100000);
    InputSplit[] splits = inFormat.getSplits(conf, conf.getNumMapTasks());
    int samples = Math.min(10, splits.length);
    long recordsPerSample = sampleSize / samples;
    int sampleStep = splits.length / samples;
    long records = 0;
    ArrayList<Text> sampleArray = new ArrayList<Text>();
    // take N samples from different parts of the input
    for(int i=0; i < samples; ++i) {
      RecordReader<LongWritable,Text> reader = 
        inFormat.getRecordReader(splits[sampleStep * i], conf, null);
      while (reader.next(key, value)) {
        sampleArray.add(new Text(value.toString().substring(0, 10)));
        records += 1;
        if ((i+1) * recordsPerSample <= records) {
          break;
        }
      }
    }
    Collections.sort(sampleArray);
    FileSystem outFs = partFile.getFileSystem(conf);
    if (outFs.exists(partFile)) {
      outFs.delete(partFile, false);
    }
    int numRecords = sampleArray.size();
    System.out.println("Making " + partitions + " from " + numRecords + 
                       " records");
    if (partitions > numRecords) {
      throw new IllegalArgumentException
        ("Requested more partitions than input keys (" + partitions +
         " > " + numRecords + ")");
    }
    float stepSize = numRecords / (float) partitions;
    System.out.println("Step size is " + stepSize);
    FSDataOutputStream fout = outFs.create(partFile);
    new IntWritable(partitions).write(fout);
    for(int i=1; i < partitions; ++i) {
      int current = Math.round(stepSize * i);
      Text w = sampleArray.get(current);
      w.write(fout);
    }
    fout.close();
  }

  void printUsage() {
    System.out.println("Usage: NativeTeraSort <input> <output>");
    GenericOptionsParser.printGenericCommandUsage(System.out);
  }

  /**
   * Submit a pipes job based on the command line arguments.
   * @param args
   */
  public static void main(String[] args) throws Exception {
    int exitCode =  new NativeTeraSort().run(args);
    System.exit(exitCode);
  }
}
