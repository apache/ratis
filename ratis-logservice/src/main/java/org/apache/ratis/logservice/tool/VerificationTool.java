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
package org.apache.ratis.logservice.tool;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.ratis.logservice.api.LogInfo;
import org.apache.ratis.logservice.api.LogName;
import org.apache.ratis.logservice.api.LogReader;
import org.apache.ratis.logservice.api.LogStream;
import org.apache.ratis.logservice.api.LogWriter;
import org.apache.ratis.logservice.client.LogServiceClient;
import org.apache.ratis.logservice.server.LogStateMachine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

public class VerificationTool {

    public static final Logger LOG = LoggerFactory.getLogger(LogStateMachine.class);

    @Parameter(names = {"-q", "--metaQuorum"}, description = "Metadata Service Quorum", required = true)
    private String metaQuorum;
    @Parameter(names = {"-nl", "--numLogs"}, description = "Number of logs")
    private int numLogs = 10;
    @Parameter(names = {"-nr", "--numRecords"}, description = "Number of records per log")
    private int numRecords = 1000;
    @Parameter(names = {"-w", "--write"}, description = "Write to the logs")
    private boolean write = true;
    @Parameter(names = {"-r", "--read"}, description = "Read the logs")
    private boolean read = true;
    @Parameter(names = {"-l", "--logFrequency"}, description = "Print update every N operations")
    private int logFrequency = 50;
    @Parameter(names = {"-h", "--help"}, description = "Help", help = true)
    private boolean help = false;

    public static final String LOG_NAME_PREFIX = "testlog";
    public static final String MESSAGE_PREFIX = "message";

    public static void main(String[] args) throws IOException {
        VerificationTool tool = new VerificationTool();
        JCommander jc = JCommander.newBuilder()
                .addObject(tool)
                .build();
        
        jc.parse(args);
        if (tool.help) {
          jc.usage();
          return;
        }
        System.out.println(tool.metaQuorum);
        LogServiceClient client = new LogServiceClient(tool.metaQuorum);
        ExecutorService executor = Executors.newCachedThreadPool();
        List<Future<?>> futures = new ArrayList<Future<?>>(tool.numLogs);

        if (tool.write) {
          LOG.info("Executing parallel writes");
          // Delete any logs that already exist first
          final Set<LogName> logsInSystem = new HashSet<>();
          List<LogInfo> listOfLogs = client.listLogs();
          for (LogInfo logInfo : listOfLogs) {
            logsInSystem.add(logInfo.getLogName());
          }

          LOG.info("Observed logs already in system: {}", logsInSystem);
          for (int i = 0; i < tool.numLogs; i++) {
              LogName logName = getLogName(i);
              if (logsInSystem.contains(logName)) {
                LOG.info("Deleting {}", logName);
                client.deleteLog(logName);
              }
              BulkWriter writer = new BulkWriter(getLogName(i), client, tool.numRecords, tool.logFrequency);
              futures.add(executor.submit(writer));
          }
          waitForCompletion(futures);
        }

        if (tool.read) {
          LOG.info("Executing parallel reads");
          futures = new ArrayList<Future<?>>(tool.numLogs);
          for (int i = 0; i < tool.numLogs; i++) {
              BulkReader reader = new BulkReader(getLogName(i), client, tool.numRecords, tool.logFrequency);
              futures.add(executor.submit(reader));
          }
          waitForCompletion(futures);
        }
        executor.shutdownNow();
    }

    private static LogName getLogName(int id) {
      return LogName.of(LOG_NAME_PREFIX + id);
    }

    private static void waitForCompletion(List<Future<?>> futures) {
        for (Future<?> future : futures) {
            try {
                Object object = future.get();
                if (object != null) {
                    LOG.error("Operation failed with error ", object);
                    System.exit(-1);
                }
            } catch (Exception e) {
                LOG.error("Got exception ", e);
                System.exit(-1);
            }
        }
        LOG.info("All operations finished");
    }

    static abstract class Operation implements Runnable {
      final LogName logName;
      final LogServiceClient client;
      final int numRecords;
      final int logFreq;

      Operation(LogName logName, LogServiceClient client, int numRecords, int logFreq) {
        this.logName = logName;
        this.client = client;
        this.numRecords = numRecords;
        this.logFreq = logFreq;
      }
    }

    static class BulkWriter extends Operation {
        BulkWriter(LogName logName, LogServiceClient client, int numRecords, int logFreq) {
            super(logName, client, numRecords, logFreq);
        }

        public void run() {
            try {
                LOG.info("Creating {}", logName);
                LogStream logStream = this.client.createLog(logName);
                LogWriter writer = logStream.createWriter();
                for (int i = 0; i < this.numRecords; i++) {
                    String message = MESSAGE_PREFIX + i;
                    if (i % logFreq == 0) {
                      LOG.info(logName + " Writing " + message);
                    }
                    writer.write(ByteBuffer.wrap(message.getBytes(StandardCharsets.UTF_8)));
                }
                writer.close();
                LOG.info("{} log entries written to log {} successfully.", numRecords, logName);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    static class BulkReader extends Operation {
        BulkReader(LogName logName, LogServiceClient client, int numRecords, int logFreq) {
          super(logName, client, numRecords, logFreq);
      }

        public void run() {
            try {
                LogStream logStream = this.client.getLog(logName);
                LogReader reader = logStream.createReader();
                long size = logStream.getLength();
                if(size != this.numRecords) {
                    LOG.error("There is mismatch is number of records. Expected Records: "+
                            this.numRecords +", Actual Records: " + size);
                    System.exit(-1);
                }
                for (int i = 0; i < size; i++) {
                    ByteBuffer buffer = reader.readNext();
                    String message = new String(buffer.array(), buffer.arrayOffset(),
                            buffer.remaining(), StandardCharsets.UTF_8);
                    if (i % logFreq == 0) {
                      LOG.info(logName + " Read " + message);
                    }
                    if(!message.equals(MESSAGE_PREFIX + i)) {
                        LOG.error("Message is not correct. Expected: "+(MESSAGE_PREFIX + i)
                                +". Actual:" +message);
                        System.exit(-1);
                    }
                }
                LOG.info("{} log entries read from log {} successfully.", numRecords, logName);
                reader.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
