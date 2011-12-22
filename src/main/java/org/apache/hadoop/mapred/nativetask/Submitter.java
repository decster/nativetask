package org.apache.hadoop.mapred.nativetask;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.StringTokenizer;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.Parser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;


public class Submitter extends Configured implements Tool  {
  protected static final Log LOG = LogFactory.getLog(Submitter.class);

  public Submitter() {
    this(new Configuration());
  }

  public Submitter(Configuration conf) {
    setConf(conf);
  }

  /**
   * Set the configuration, if it doesn't already have a value for the given
   * key.
   * @param conf the configuration to modify
   * @param key the key to set
   * @param value the new "default" value to set
   */
  private static void setIfUnset(JobConf conf, String key, String value) {
    if (conf.get(key) == null) {
      conf.set(key, value);
    }
  }

  public static void setupNativeJob(JobConf conf) throws IOException {
    String textClassname = Text.class.getName();
    setIfUnset(conf, "mapred.mapoutput.key.class", textClassname);
    setIfUnset(conf, "mapred.mapoutput.value.class", textClassname);
    setIfUnset(conf, "mapred.output.key.class", textClassname);
    setIfUnset(conf, "mapred.output.value.class", textClassname);
    conf.setBoolean(NativeTaskConfig.NATIVE_TASK_ENABLED, true);
    conf.set(NativeTaskConfig.MAPRED_MAPTASK_DELEGATOR_CLASS,
        NativeMapTaskDelegator.class.getCanonicalName());
    conf.set(NativeTaskConfig.MAPRED_REDUCETASK_DELEGATOR_CLASS,
        NativeReduceTaskDelegator.class.getCanonicalName());
  }

  /**
   * Submit a job to the map/reduce cluster. All of the necessary modifications
   * to the job to run under pipes are made to the configuration.
   * @param conf the job to submit to the cluster (MODIFIED)
   * @throws IOException
   */
  public static RunningJob runJob(JobConf conf) throws IOException {
    setupNativeJob(conf);
    return JobClient.runJob(conf);
  }

  /**
   * A command line parser for the CLI-based Pipes job submitter.
   */
  static class CommandLineParser {
    private Options options = new Options();

    @SuppressWarnings("static-access")
    void addOption(String longName, boolean required, String description,
        String paramName) {
      Option option = OptionBuilder.withArgName(paramName).hasArgs(1)
          .withDescription(description).isRequired(required).create(longName);
      options.addOption(option);
    }

    @SuppressWarnings("static-access")
    void addArgument(String name, boolean required, String description) {
      Option option = OptionBuilder.withArgName(name).hasArgs(1)
          .withDescription(description).isRequired(required).create();
      options.addOption(option);

    }

    Parser createParser() {
      Parser result = new BasicParser();
      return result;
    }

    void printUsage() {
      // The CLI package should do this for us, but I can't figure out how
      // to make it print something reasonable.
      System.out.println("bin/hadoop -jar nativetask.jar");
      System.out.println("  [-input <path>] // Input directory");
      System.out.println("  [-output <path>] // Output directory");
      System.out.println("  [-jar <jar file> // jar filename");
      System.out.println("  [-inputformat <class>] // InputFormat class");
      System.out.println("  [-outputformat <class>] // OutputFormat class");
      System.out.println("  [-reduces <num>] // number of reduces");
      System.out.println();
      GenericOptionsParser.printGenericCommandUsage(System.out);
    }
  }

  private static <InterfaceType>
  Class<? extends InterfaceType> getClass(CommandLine cl, String key,
                                          JobConf conf,
                                          Class<InterfaceType> cls
                                         ) throws ClassNotFoundException {
    return conf.getClassByName((String) cl.getOptionValue(key)).asSubclass(cls);
  }

  @SuppressWarnings("deprecation")
  @Override
  public int run(String[] args) throws Exception {
    CommandLineParser cli = new CommandLineParser();
    if (args.length == 0) {
      cli.printUsage();
      return 1;
    }
    cli.addOption("input", false, "input path to the maps", "path");
    cli.addOption("output", false, "output path from the reduces", "path");

    cli.addOption("jar", false, "job jar file", "path");

    cli.addOption("inputformat", false, "java classname of InputFormat", "class");
    cli.addOption("outputformat", false, "java classname of OutputFormat", "class");
    cli.addOption("reduces", false, "number of reduces", "num");
    cli.addOption("jobconf", false,
        "\"n1=v1,n2=v2,..\" (Deprecated) Optional. Add or override a JobConf property.",
        "key=val");
    Parser parser = cli.createParser();
    try {

      GenericOptionsParser genericParser = new GenericOptionsParser(getConf(), args);
      CommandLine results =
        parser.parse(cli.options, genericParser.getRemainingArgs());

      JobConf job = new JobConf(getConf());

      if (results.hasOption("input")) {
        FileInputFormat.setInputPaths(job,
                          (String) results.getOptionValue("input"));
      }
      if (results.hasOption("output")) {
        FileOutputFormat.setOutputPath(job,
          new Path((String) results.getOptionValue("output")));
      }
      if (results.hasOption("jar")) {
        job.setJar((String) results.getOptionValue("jar"));
      }
      if (results.hasOption("inputformat")) {
        job.setInputFormat(getClass(results, "inputformat", job,
                                     InputFormat.class));
      }
      if (results.hasOption("outputformat")) {
        job.setOutputFormat(getClass(results, "putputformat", job,
                                     OutputFormat.class));
      }
      if (results.hasOption("jobconf")) {
        LOG.warn("-jobconf option is deprecated, please use -D instead.");
        String options = (String)results.getOptionValue("jobconf");
        StringTokenizer tokenizer = new StringTokenizer(options, ",");
        while (tokenizer.hasMoreTokens()) {
          String keyVal = tokenizer.nextToken().trim();
          String[] keyValSplit = keyVal.split("=", 2);
          job.set(keyValSplit[0], keyValSplit[1]);
        }
      }
      // if they gave us a jar file, include it into the class path
      String jarFile = job.getJar();
      if (jarFile != null) {
        final URL[] urls = new URL[]{ FileSystem.getLocal(job).
            pathToFile(new Path(jarFile)).toURL()};
        //FindBugs complains that creating a URLClassLoader should be
        //in a doPrivileged() block.
        ClassLoader loader =
          AccessController.doPrivileged(
              new PrivilegedAction<ClassLoader>() {
                public ClassLoader run() {
                  return new URLClassLoader(urls);
                }
              }
            );
        job.setClassLoader(loader);
      }

      runJob(job);
      return 0;
    } catch (ParseException pe) {
      LOG.info("Error : " + pe);
      cli.printUsage();
      return 1;
    }
  }

  /**
   * Submit a pipes job based on the command line arguments.
   * @param args
   */
  public static void main(String[] args) throws Exception {
    int exitCode =  new Submitter().run(args);
    System.exit(exitCode);
  }
}
