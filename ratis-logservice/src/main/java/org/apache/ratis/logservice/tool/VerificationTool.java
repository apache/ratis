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
import java.util.List;
import java.util.concurrent.*;

import org.apache.ratis.logservice.api.LogName;
import org.apache.ratis.logservice.api.LogReader;
import org.apache.ratis.logservice.api.LogStream;
import org.apache.ratis.logservice.api.LogWriter;
import org.apache.ratis.logservice.client.LogServiceClient;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.ratis.logservice.server.LogStateMachine;
import org.jline.utils.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VerificationTool {

    public static final Logger LOG = LoggerFactory.getLogger(LogStateMachine.class);

    @Parameter(names = {"-q", "--metaQuorum"}, description = "Metadata Service Quorum", required = true)
    private String metaQuorum;
    public static String LOG_NAME_PREFIX = "testlog";
    public static String MESSAGE_PREFIX = "message";

    private int numLogs = 10;
    private int numRecords = 1000;

    public static void main(String[] args) {
        VerificationTool tool = new VerificationTool();
        JCommander.newBuilder()
                .addObject(tool)
                .build()
                .parse(args);
        System.out.println(tool.metaQuorum);
        LogServiceClient client = new LogServiceClient(tool.metaQuorum);
        ExecutorService executor = Executors.newCachedThreadPool();
        List<Future<?>> futures = new ArrayList<Future<?>>(tool.numLogs);
        for (int i = 0; i < tool.numLogs; i++) {
            BulkWriter writer = new BulkWriter(LOG_NAME_PREFIX + i, client, tool.numRecords);
            futures.add(executor.submit(writer));
        }
        waitForCompletion(futures);
        futures = new ArrayList<Future<?>>(tool.numLogs);
        for (int i = 0; i < tool.numLogs; i++) {
            BulkReader reader = new BulkReader(LOG_NAME_PREFIX + i, client, tool.numRecords);
            futures.add(executor.submit(reader));
        }
        waitForCompletion(futures);
        executor.shutdownNow();
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
    }

    static class BulkWriter implements Runnable {
        private String logName;
        private LogServiceClient logServiceClient;
        private int numRecords;

        BulkWriter(String logName, LogServiceClient logServiceClient, int numRecords) {
            this.logName = logName;
            this.logServiceClient = logServiceClient;
            this.numRecords = numRecords;
        }

        public void run() {
            try {
                LogStream logStream = this.logServiceClient.createLog(LogName.of(logName));
                LogWriter writer = logStream.createWriter();
                for (int i = 0; i < this.numRecords; i++) {
                    String message = MESSAGE_PREFIX + i;
                    System.out.println(logName + " Writing " + message);
                    writer.write(ByteBuffer.wrap(message.getBytes(StandardCharsets.UTF_8)));
                }
                writer.close();
                Log.info("" + numRecords + "log entries written to log "+ this.logName + " successfully.");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    static class BulkReader implements Runnable {
        private String logName;
        private LogServiceClient logServiceClient;
        private int numRecords;

        BulkReader(String logName, LogServiceClient logServiceClient, int numRecords) {
            this.logName = logName;
            this.logServiceClient = logServiceClient;
            this.numRecords = numRecords;
        }

        public void run() {
            try {
                LogStream logStream = this.logServiceClient.getLog(LogName.of(logName));
                LogReader reader = logStream.createReader();
                long size = logStream.getLength();
                if(size != this.numRecords) {
                    Log.error("There is mismatch is number of records. Expected Records: "+
                            this.numRecords +", Actual Records: " + size);
                    System.exit(-1);
                }
                for (int i = 0; i < size; i++) {
                    ByteBuffer buffer = reader.readNext();
                    String message = new String(buffer.array(), buffer.arrayOffset(),
                            buffer.remaining(), StandardCharsets.UTF_8);
                    System.out.println(logName + " Read " + message);
                    if(!message.equals(MESSAGE_PREFIX + i)) {
                        Log.error("Message is not correct. Expected: "+(MESSAGE_PREFIX + i)
                                +". Actual:" +message);
                        System.exit(-1);
                    }
                }
                Log.info("" + numRecords + " log entries read from log "+ this.logName + " successfully.");
                reader.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
