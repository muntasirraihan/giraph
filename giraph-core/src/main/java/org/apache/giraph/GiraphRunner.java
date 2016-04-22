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
import java.io.FileReader;
import java.io.IOException;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.BufferedWriter;

import java.net.URI;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import java.util.List;
import java.util.ArrayList;


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
  private static final String CONSOLE_COMMAND_TEST = "test";
  private static final String CONSOLE_COMMAND_BASE = "base";  
  private static final String CONSOLE_COMMAND_BUSY = "busy";

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
        // System.out.println("Start - NOT IMPLEMENTED.");
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
      case CONSOLE_COMMAND_TEST:
        try {
          testScheduling();
        } catch (Exception e) {
          LOG.info(e);
        }
        break;
      case CONSOLE_COMMAND_BASE:
        try {
          testSchedulingBase();
        } catch (Exception e) {
          LOG.info(e);
        }
        break;
      case CONSOLE_COMMAND_BUSY:
        try {
          busy();
        } catch (Exception e) {
          LOG.info(e);
        }
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
    * Every interval of time, the console will start a small job to keep the cluster
    */
  private void busy() {
    int jobSleepingInterval = 30 * 60 * 1000; // 30 minutes
    String inputPath = "/user/input/inputGraph2.txt";
    String outputPath = "/user/output/busy/";

    int jobCounter = 1;
    while (true) {

      try {
        
        processCommand(formatSSSPJobCommand(inputPath, outputPath+jobCounter, 14));
        jobCounter++;
        Thread.sleep(jobSleepingInterval);

      } catch (Exception e) {
        LOG.info(e);
      }
    }
  }

  private String formatSSSPJobCommand(String inputPath, String outputPath, int numWorkers) {
    return "start org.apache.giraph.examples.SimpleShortestPathsComputation -vif org.apache.giraph.io.formats.JsonLongDoubleFloatDoubleVertexInputFormat -vip " + inputPath + " -vof org.apache.giraph.io.formats.IdWithValueTextOutputFormat -op "+ outputPath + " -w " + numWorkers + " -yj giraph-examples-1.1.0-for-hadoop-2.7.0-jar-with-dependencies.jar";
  }

  /*
   *  Fetch the currently running applications using 'yarn application -list'
   */
  private List<String> yarnApplicationFetchAccepted() throws IOException {
    List<String> ids = new ArrayList<String>();
    ProcessBuilder pb = new ProcessBuilder("bash", "-c", "$HADOOP_HOME/bin/yarn application -list 2>/dev/null | grep -E \'application_.*(SUBMITTED|ACCEPTED)\' | awk \'{ print $1 }\'");
    Process process = pb.start();
   
    // File f = new File("/users/mrahman2/yarnApplicationFetchAcceptedOutput"+System.currentTimeMillis());
    // try {
    //   f.createNewFile();
    // } catch (IOException e) {
    // }

   try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))
    // ; PrintWriter pw = new PrintWriter(new BufferedWriter(new FileWriter(f, true)))
    )
   {  
      String line = null;
      while ( (line = reader.readLine()) != null) {
         ids.add(line);
          // pw.println(line);
      }
      return ids; 
   } catch (IOException e){
      LOG.debug("Error occurred when reading yarn application fetching accepted.");
   }
   return ids;
  }

  /*
   *  Fetch the currently running applications using 'yarn application -list'
   */
  private List<String> yarnApplicationFetchRunning() throws IOException {
    List<String> ids = new ArrayList<String>();
    ProcessBuilder pb = new ProcessBuilder("bash", "-c", "$HADOOP_HOME/bin/yarn application -list 2>/dev/null | grep -E \'application_.*(RUNNING)\' | awk \'{ print $1 }\'");
    Process process = pb.start();

    // File f = new File("/users/mrahman2/yarnApplicationFetchRunningOutput"+System.currentTimeMillis());
    // try {
    //   f.createNewFile();
    // } catch (IOException e) {
    // }

    try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))
        // ; PrintWriter pw = new PrintWriter(new BufferedWriter(new FileWriter(f, true)))
      ) {
      String line = null;
      while ( (line = reader.readLine()) != null) {
         ids.add(line);
         // pw.println(line);
      }
    } catch (IOException e) {
      LOG.debug("Error occurred when reading yarn application fetching running.");
    }
    return ids; 
  }

  private void yarnApplicationKillJob(String applicationId) throws IOException {
    ProcessBuilder pb = new ProcessBuilder("bash","-c", "$HADOOP_HOME/bin/yarn application -kill " + applicationId);
    pb.start();
  }

  /*
   *  Fetch the progress for an application given the log path
   */
  private int readProgressOfApp(String progressFilePath) {
    String lastLine = null;
    try (BufferedReader br = new BufferedReader(new FileReader(progressFilePath)))
    {
      String currentLine = null;

      while ((currentLine = br.readLine()) != null)
      {
          lastLine = currentLine;
      }

    } catch (IOException e) {
      LOG.debug("Error occurred when reading progress log at "+progressFilePath);
    } 
    if (lastLine != null) {
      String[] lastLineData = lastLine.split("\\s+");
      return Integer.parseInt(lastLineData[0]);
    } else {
      return Integer.MAX_VALUE;
    }
  }

  /*
   * Fetch the list of containers for an application given the log path
   */
  private List<String> readContainersOfApp(String containerFilePath) {
    List<String> containers = new ArrayList<String>();
    try (BufferedReader br = new BufferedReader(new FileReader(containerFilePath)))
    {
      String currentLine = null;
      while ((currentLine = br.readLine()) != null)
      {
          containers.add(currentLine);
      }

    } catch (IOException e) {
      LOG.debug("Error occurred when reading container log at " + containerFilePath);
    }

    return containers;
  }

  private void sshCopyCommand(String hostname, String fileToCopy) throws IOException {
    String hdfsCommand = "$HADOOP_HOME/bin/hdfs dfs -cp " + fileToCopy + " " + fileToCopy+".copy.txt";
    String sshCommand = "ssh -oStrictHostKeyChecking=no " + hostname + " " + hdfsCommand;
    ProcessBuilder pb = new ProcessBuilder("bash", "-c", sshCommand);
    pb.start();
  }

  private String formatInputPath(int i) {
    return "/user/input/inputGraph"+ i+".txt"; 
  }

  private String formatOutputPath(int i) {
    return "/user/output/output"+i;
  }

  /**
    * Function to monitor baseline performance without opportunistic scheduling
    * Yarn polling, log reading are still enabled to perform a fair comparison with opportunistic scheduling
    */
  private void testSchedulingBase() throws Exception {

   int numSSHCommands = 1;
   int jobNumber = 3;
   int numWorkers = 3;
   String issProgressLogPrefix = "/users/mrahman2/iss_progress_giraph_yarn_"; // + applicationId
   String issContainerLogPrefix = "/users/mrahman2/iss_container_"; // + applicationId

   for (int i=0; i<jobNumber; i++) {
    // step 1 
    processCommand(formatSSSPJobCommand(formatInputPath(i), formatOutputPath(i), numWorkers));
    
    // step 2
    Thread.sleep(10000);
    List<String> waitingJobs = yarnApplicationFetchAccepted();

    // step 3 and 4
    if (!waitingJobs.isEmpty()) {
      String appId = waitingJobs.get(0);
      
      // step 5
      // yarnApplicationKillJob(appId);

      // step 6
      List<String> runningJobs = yarnApplicationFetchRunning();
      
      // step 7
      int minOngoingMessage = Integer.MAX_VALUE;
      String maximumProgressJob = null;
      for (String runningJobId: runningJobs) {
        int thisOngoingMessage = readProgressOfApp(issProgressLogPrefix + runningJobId);
        if (minOngoingMessage > thisOngoingMessage) {
          minOngoingMessage = thisOngoingMessage;
          maximumProgressJob = runningJobId;
        }
      }
      // step 8
      List<String> listOfContainers = readContainersOfApp(issContainerLogPrefix + maximumProgressJob);

      // step 10
      Thread.sleep(7500);

      // step 11
      // processCommand(formatSSSPJobCommand(formatInputPath(i)+".copy.txt", formatOutputPath(i)opy", numWorkers));
    }
   }

  }

  /**
   * Script for test ISS scheduling project
   * Run three SSSP jobs with inputgraph1, inputgraph2, inputgraph3, all with worker = numWorkers
   * For each job, 
   * 1. submit
   * 2. sleep for a while, then pull "yarn application -list" and check the status of 'latest' job
   * 3. if status = running, ok
   * 4. else if status = accepted, 
   * 5    kill it
   * 6.   pull "yarn application -list" and check all running job
   * 7.   pull all iss_progress_log, find job_k with maximum progress
   * 8.   pull job_k's iss_container_log
   * 9.   for each container c in the log, ssh into host of c and run hdfs copy command
   * 10.  sleep and wait for copy to finish
   * 11.  resubmit the job with new input
   */
  private void testScheduling() throws Exception {

   int numSSHCommands = 1;
   int jobNumber = 3;
   int numWorkers = 3;
   String issProgressLogPrefix = "/users/mrahman2/iss_progress_giraph_yarn_"; // + applicationId
   String issContainerLogPrefix = "/users/mrahman2/iss_container_"; // + applicationId

   for (int i=0; i<jobNumber; i++) {
    // step 1 
    processCommand(formatSSSPJobCommand(formatInputPath(i), formatOutputPath(i), numWorkers));
    
    // step 2
    Thread.sleep(10000);
    List<String> waitingJobs = yarnApplicationFetchAccepted();

    // step 3 and 4
    if (!waitingJobs.isEmpty()) {
      String appId = waitingJobs.get(0);
      
      // step 5
      yarnApplicationKillJob(appId);

      // step 6
      List<String> runningJobs = yarnApplicationFetchRunning();
      
      // step 7
      int minOngoingMessage = Integer.MAX_VALUE;
      String maximumProgressJob = null;
      for (String runningJobId: runningJobs) {
        int thisOngoingMessage = readProgressOfApp(issProgressLogPrefix + runningJobId);
        if (minOngoingMessage > thisOngoingMessage) {
          minOngoingMessage = thisOngoingMessage;
          maximumProgressJob = runningJobId;
        }
      }

      // File f = new File("/users/mrahman2/" + System.currentTimeMillis() + "_mpj=" + maximumProgressJob);
      // f.createNewFile();
      LOG.info("Find maximum progress job " + maximumProgressJob);
      
      // step 8
      List<String> listOfContainers = readContainersOfApp(issContainerLogPrefix + maximumProgressJob);
      // try ( PrintWriter pw = new PrintWriter(new BufferedWriter(new FileWriter(f, true))) ) 
      // {
      //   for (String container: listOfContainers)
      //     pw.println(container);
      // } catch (IOException e) {
      //   LOG.info("Error occurred in logging containers for " + maximumProgressJob);
      // }

      // step 9
      for (String container: listOfContainers) {
        // for (int ii = 0; ii < 10; ii++)
        LOG.info("ssh into container " + container.split("\\.")[0]);
        sshCopyCommand(container.split("\\.")[0], formatInputPath(i));
        numSSHCommands--;
        if (numSSHCommands == 0)
          break;
      }

      // step 10
      Thread.sleep(7500);

      // step 11
      processCommand(formatSSSPJobCommand(formatInputPath(i)+".copy.txt", formatOutputPath(i)+"copy", numWorkers));
    }
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
