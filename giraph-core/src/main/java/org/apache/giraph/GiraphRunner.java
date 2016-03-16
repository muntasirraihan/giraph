/*
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
package org.apache.giraph;

import org.apache.commons.cli.CommandLine;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.job.GiraphJob;
import org.apache.giraph.utils.ConfigurationUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/*if[PURE_YARN]
import org.apache.giraph.yarn.GiraphYarnClient;
end[PURE_YARN]*/

/**
 * Helper class to run Giraph applications by specifying the actual class name
 * to use (i.e. vertex, vertex input/output format, combiner, etc.).
 *
 * This is the default entry point for Giraph jobs running on any Hadoop
 * cluster, MRv1 or v2, including Hadoop-specific configuration and setup.
 */
public class GiraphRunner implements Tool {
  static {
    Configuration.addDefaultResource("giraph-site.xml");
  }

  /** Class logger */
  private static final Logger LOG = Logger.getLogger(GiraphRunner.class);
  /** Writable conf */
  private Configuration conf;

  /** Console Constants */
  private static final String CONSOLE_PROMPT = "DPRG-Giraph >> ";
  private static final String CONSOLE_COMMAND_EXIT = "exit";
  private static final String CONSOLE_COMMAND_START = "start";
  private static final String CONSOLE_COMMAND_STOP = "stop";
  private static final String CONSOLE_COMMAND_STATUS = "status";
  private static final String CONSOLE_ARGUMENT = "console";

  /** Executors (ThreadPool) for running new jobs  */
  ExecutorService executor = Executors.newCachedThreadPool();

  /** Collection of Futures to keep track of scheduled jobs. */
  Set<Future> scheduledJobsFutures = new HashSet<>();

  /** Collection of references to submitted GiraphJobs*/
  // Set<GiraphJob> scheduledJobCallables = new HashSet<>();

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  /**
   * Drives a job run configured for "Giraph on Hadoop MR cluster" or runs the console.
   * @param args the command line arguments
   * @return job run exit code
   */
  public int run(String[] args) throws Exception {
    LOG.info("Args:" + Arrays.toString(args));
    if(args[0].toLowerCase().equals(CONSOLE_ARGUMENT)) {
      runShell();
    } else {
      return runNewJob(args);
    }

    // TODO: Change this. The return value should reflect the jobs return values.
    return 0;
  }

  /**
   * Starts the shell.
   * @return
   * @throws Exception
   */
  private int runShell() throws Exception {
    boolean keepRunning = true;
    while(keepRunning) {
      System.out.print(CONSOLE_PROMPT);
      BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
      String command = br.readLine();
      if(processCommand(command) == 0) {
        keepRunning = false;
      }
    }

    // TODO: Change this. The return value should reflect the jobs return values.
    return 0;
  }

  private int processCommand(String command) throws Exception {
    String[] args = command.split("\\s+");

    switch (args[0].toLowerCase()) {
      case CONSOLE_COMMAND_EXIT:
        System.out.println("Exiting.");
        return 0;
      case CONSOLE_COMMAND_START: // TODO: Add features.
        System.out.println("Start - NOT IMPLEMENTED.");
        String[] jobArgs = Arrays.copyOfRange(args, 1, args.length);
        /*
         * If there is an error / exception, system would crash.
         */
        try {
          runNewJob(jobArgs);
        } catch (Exception e) {
          LOG.info(e);
        }

        break;
      case CONSOLE_COMMAND_STATUS: // TODO: Add features.
        System.out.println("Status - NOT IMPLEMENTED.");
        break;
      case CONSOLE_COMMAND_STOP: // TODO: Add features.
        System.out.println("Stop - NOT IMPLEMENTED.");
        break;
    }

    return 1;
  }

  /**
   * TODO: Add concurrency.
   * Creates & Drives a job run configured for "Giraph on Hadoop MR cluster"
   * @param args the command line arguments
   * @return job run exit code
   */
  private int runNewJob(String[] args) throws Exception {
    if (null == getConf()) { // for YARN profile
      conf = new Configuration();
    }
    GiraphConfiguration giraphConf = new GiraphConfiguration(getConf());
    CommandLine cmd = ConfigurationUtils.parseArgs(giraphConf, args);
    if (null == cmd) {
      return 0; // user requested help/info printout, don't run a job.
    }

    // print the progress of previously submitted jobs
    // for (GiraphJob job: scheduledJobCallables) {
    //   LOG.info(job.getJobName() + ": " + job.getInternalJob().mapProgress());
    // }

    // set up job for various platforms
    final String vertexClassName = args[0];
    final String jobName = "Giraph: " + vertexClassName;
    /*if[PURE_YARN]
    GiraphYarnClient job = new GiraphYarnClient(giraphConf, jobName);
    else[PURE_YARN]*/
    GiraphJob job = new GiraphJob(giraphConf, jobName);
    prepareHadoopMRJob(job, cmd);
    /*end[PURE_YARN]*/

    // run the job, collect results
    if (LOG.isDebugEnabled()) {
      LOG.debug("Attempting to run Vertex: " + vertexClassName);
    }
    job.setVerbosity(!cmd.hasOption('q'));
    Future jobFuture = executor.submit(job);
    scheduledJobsFutures.add(jobFuture);
    // scheduledJobCallables.add(job);

    return 0;
    //return (boolean)job.call() ? 0 : -1;
  }

  /**
   * Populate internal Hadoop Job (and Giraph IO Formats) with Hadoop-specific
   * configuration/setup metadata, propagating exceptions to calling code.
   * @param job the GiraphJob object to help populate Giraph IO Format data.
   * @param cmd the CommandLine for parsing Hadoop MR-specific args.
   */
  private void prepareHadoopMRJob(final GiraphJob job, final CommandLine cmd)
    throws Exception {
    if (cmd.hasOption("vof") || cmd.hasOption("eof")) {
      if (cmd.hasOption("op")) {
        FileOutputFormat.setOutputPath(job.getInternalJob(),
          new Path(cmd.getOptionValue("op")));
      }
    }
    if (cmd.hasOption("cf")) {
      DistributedCache.addCacheFile(new URI(cmd.getOptionValue("cf")),
          job.getConfiguration());
    }
  }

  /**
   * Execute GiraphRunner.
   *
   * @param args Typically command line arguments.
   * @throws Exception Any exceptions thrown.
   */
  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new GiraphRunner(), args));
  }
}
