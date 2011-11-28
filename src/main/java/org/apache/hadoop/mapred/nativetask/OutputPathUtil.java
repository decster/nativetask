package org.apache.hadoop.mapred.nativetask;

import java.io.IOException;

import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapred.TaskTracker;

public class OutputPathUtil {
  final JobID jobId;
  final JobConf conf;
  private LocalDirAllocator lDirAlloc = 
    new LocalDirAllocator("mapred.local.dir");
  
  public OutputPathUtil(JobID jobId, JobConf conf) {
    this.jobId = jobId;
    this.conf = conf;
  }
  
  public String getIntermediateOutputDir(TaskAttemptID mapTaskId) {
    return String.format("taskTracker/jobcache/%s/%s/output", jobId.toString(),
        mapTaskId.toString());
  }
  
  /** Create a local map output file name.
   * @param mapTaskId a map task id
   * @param size the size of the file
   */
  public Path getOutputFileForWrite(TaskAttemptID mapTaskId, long size)
    throws IOException {
    return lDirAlloc.getLocalPathForWrite(getIntermediateOutputDir(mapTaskId)
        + "/file.out", size, conf);
  }

  /** Create a local map output index file name.
   * @param mapTaskId a map task id
   * @param size the size of the file
   */
  public Path getOutputIndexFileForWrite(TaskAttemptID mapTaskId, long size)
    throws IOException {
    return lDirAlloc.getLocalPathForWrite(getIntermediateOutputDir(mapTaskId)
        + "/file.out.index", size, conf);
  }


  /** Create a local map spill file name.
   * @param mapTaskId a map task id
   * @param spillNumber the number
   * @param size the size of the file
   */
  public Path getSpillFileForWrite(TaskAttemptID mapTaskId, int spillNumber, 
         long size) throws IOException {
    return lDirAlloc.getLocalPathForWrite(getIntermediateOutputDir(mapTaskId)
        + "/spill" + spillNumber + ".out", size, conf);
  }
  
  
  public static String getOutputName(int partition) {
    return String.format("part-%05d", partition);
  }
}
