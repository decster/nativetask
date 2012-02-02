Introduction
------------

NativeTask is a high performance C++ API & runtime for Hadoop MapReduce. Why
it is called *NativeTask* is that it is a *native* computing unit only focus
on data processing, which is exactly what *Task* do in the Hadoop MapReduce 
context. 
In other word, NativeTask is not responsible for resource management, job
Scheduling and fault-tolerance. Those are all managed by original Hadoop
components as before, unchanged. But the actual data processing and computation, 
which consumes most of cluster resources, are delegated to this highly 
efficient data processing unit.

NativeTask is designed to be very fast, with native C++ API. So more 
efficient data analysis applications can build upon it, like LLVM based 
query execution engine mentioned in Google's 
[Tenzing](http://research.google.com/pubs/pub37200.html). 
Actually this is the main objective of NativeTask, to provide a efficient 
native Hadoop framework, so much more efficient data analyze tools can 
be built upon it: 

  * Data warehousing tool using state of the art query execution techniques 
    existing in parallel DBMSs, such as compression, vectorization, dynamic 
    compilation, etc. These techniques are more easy to implement in 
    native code, as we can see that most of these techniques are implemented 
    using C/C++: Vectorwise, Vertica.

  * High performance data mining/machine learning libraries, most of these 
    algorithms are CPU intensive, involving lot of numerical computation, 
    or have been implemented using native languages already, a native runtime 
    permits better performance, or easy porting these algorithms to Hadoop. 

From user's perspective, NativeTask is a lot like Hadoop Pipes: using header 
files and dynamic libraries provided in NativeTask library, you compile 
your application or class library to a dynamic library rather than executable 
program(because we use JNI), then using a Submitter tool to submit you 
job to Hadoop cluster like streaming or pipes do. For more information, 
please read the design document and examples in src/main/native/examples.

Features
--------
1. High performance, more cost effective for your Hadoop cluster;
2. C++ API, so user can develop native applications or apply more 
   aggressive optimizations not available or convenient for java, 
   like SSE/AVX instruction, LLVM, GPU computing, coprocessor etc.
3. Support no sort, by removing sort, the shuffle stage barrier can be 
   eliminated, yielding better data processing throughput;
4. Support foldl style API, much faster for aggregation queries;
5. Binary based MapReduce API, no serialization/deserialization overhead;
6. Compatible with Hadoop 0.20-0.23(need task-delegation patch)

Notice
------
This project is in very early stages currently, and is not well documented. 
If you are familiar with Hadoop MapReduce, you can hack into the source code. 

Demo and some early test results in Hadoop JIRA:
https://issues.apache.org/jira/browse/MAPREDUCE-2841
