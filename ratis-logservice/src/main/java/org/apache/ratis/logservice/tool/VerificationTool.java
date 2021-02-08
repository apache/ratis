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
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.ratis.logservice.api.LogInfo;
import org.apache.ratis.logservice.api.LogName;
import org.apache.ratis.logservice.api.LogReader;
import org.apache.ratis.logservice.api.LogStream;
import org.apache.ratis.logservice.api.LogWriter;
import org.apache.ratis.logservice.api.LogServiceClient;
import org.apache.ratis.logservice.common.LogNotFoundException;
import org.apache.ratis.logservice.server.LogStateMachine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.validators.PositiveInteger;

/**
 * LogService utility to write data and validate its presence.
 */
public class VerificationTool {
    /**
     * Validator to check that the provided value is positive and non-zero.
     */
    public static class NonZeroPositiveInteger implements IParameterValidator {
      @Override
      public void validate(String name, String value) throws ParameterException {
        int i = Integer.parseInt(value);
        if (i < 1) {
          throw new ParameterException("Parameter '" + name + "' must be positive and non-zero"
              + ", but was " + value);
        }
      }
    }

    public static final Logger LOG = LoggerFactory.getLogger(LogStateMachine.class);

    @Parameter(names = {"-q", "--metaQuorum"},
        description = "Metadata Service Quorum",
        required = true)
    private String metaQuorum;

    @Parameter(names = {"-nl", "--numLogs"},
        description = "Number of logs",
        validateWith = NonZeroPositiveInteger.class)
    private int numLogs = 10;

    @Parameter(names = {"-nr", "--numRecords"},
        description = "Number of records to write per log",
        validateWith = NonZeroPositiveInteger.class)
    private int numRecords = 1000;

    @Parameter(names = {"-w", "--write"},
        description = "Write to the logs",
        arity = 1)
    private boolean write = true;

    @Parameter(names = {"-r", "--read"},
        description = "Read the logs",
        arity = 1)
    private boolean read = true;

    @Parameter(names = {"-l", "--logFrequency"},
        description = "Print update every N operations",
        validateWith = NonZeroPositiveInteger.class)
    private int logFrequency = 50;

    @Parameter(names = {"-h", "--help"},
        description = "Help",
        help = true)
    private boolean help = false;

    @Parameter(names = {"-s", "--size"},
        description = "Size in bytes of each value")
    private int recordSize = -1;

    @Parameter(names = {"-bs", "--batchSize"},
        description = "Number of records in a batch, a value of 0 disables batching",
        validateWith = PositiveInteger.class)
    private int batchSize = 0;

    public static final String LOG_NAME_PREFIX = "testlog";
    public static final String MESSAGE_PREFIX = "message";

    public static void main(String[] args) throws Exception {
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
        try (LogServiceClient client = new LogServiceClient(tool.metaQuorum)) {
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
            }

            // First write batch entries to log.
            if(tool.batchSize > 0) {
              // Compute the number of batches to write given the batch size.
              int numBatches = tool.numRecords / tool.batchSize;
              for (int i = 0; i < tool.numLogs; i++) {
                BatchWriter writer = new BatchWriter(getLogName(i), client, tool.numRecords,
                    tool.logFrequency, tool.recordSize, tool.batchSize, numBatches);
                futures.add(executor.submit(writer));
              }
            } else {
              // Write single entries to log.
              for (int i = 0; i < tool.numLogs; i++) {
                BulkWriter writer = new BulkWriter(getLogName(i), client, tool.numRecords,
                    tool.logFrequency, tool.recordSize);
                futures.add(executor.submit(writer));
              }
            }
            waitForCompletion(futures);
          }

          if (tool.read) {
            LOG.info("Executing parallel reads");
            futures = new ArrayList<Future<?>>(tool.numLogs);
            for (int i = 0; i < tool.numLogs; i++) {
              BulkReader reader = new BulkReader(getLogName(i), client, tool.numRecords, tool.logFrequency,
                  tool.recordSize);
              futures.add(executor.submit(reader));
            }
            waitForCompletion(futures);
          }
          executor.shutdownNow();
        }

    }

    private static LogName getLogName(int id) {
      return LogName.of(LOG_NAME_PREFIX + id);
    }

  @SuppressFBWarnings("DM_EXIT")
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

    abstract static class Operation implements Runnable {
      static final byte DIVIDER_BYTE = '_';
      private final LogName logName;
      private final LogServiceClient client;
      private final int numRecords;
      private final int logFreq;
      private final int valueSize;

      public LogName getLogName() {
        return logName;
      }

      public int getNumRecords() {
        return numRecords;
      }

      public int getLogFreq() {
        return logFreq;
      }

      public LogServiceClient getClient() {
        return client;
      }

      Operation(LogName logName, LogServiceClient client, int numRecords, int logFreq, int valueSize) {
        this.logName = logName;
        this.client = client;
        this.numRecords = numRecords;
        this.logFreq = logFreq;
        this.valueSize = valueSize;
      }

      ByteBuffer createValue(String prefix) {
        if (valueSize == -1) {
          return ByteBuffer.wrap(prefix.getBytes(StandardCharsets.UTF_8));
        }
        byte[] value = new byte[valueSize];
        byte[] prefixBytes = prefix.getBytes(StandardCharsets.UTF_8);
        // Write as much of the prefix as possible
        if (prefixBytes.length > valueSize) {
          System.arraycopy(prefixBytes, 0, value, 0, valueSize);
          return ByteBuffer.wrap(value);
        }

        // Write the full prefix
        System.arraycopy(prefixBytes, 0, value, 0, prefixBytes.length);
        int bytesWritten = prefixBytes.length;

        // Write the divider (but only if we can write the whole thing)
        if (bytesWritten + 1 > valueSize) {
          return ByteBuffer.wrap(value);
        }

        value[bytesWritten] = DIVIDER_BYTE;
        bytesWritten += 1;

        // Generate random data to pad the value
        int bytesToGenerate = valueSize - bytesWritten;
        Random r = new Random();
        byte[] suffix = new byte[bytesToGenerate];
        r.nextBytes(suffix);
        System.arraycopy(suffix, 0, value, bytesWritten, suffix.length);

        return ByteBuffer.wrap(value);
      }

      String parseValue(ByteBuffer buff) {
        if (!buff.hasArray()) {
          throw new IllegalArgumentException("Require a ByteBuffer with a backing array");
        }
        if (valueSize == -1) {
          return new String(buff.array(), buff.arrayOffset(), buff.remaining(), StandardCharsets.UTF_8);
        }
        int length = buff.limit() - buff.arrayOffset();
        byte[] value = new byte[length];
        System.arraycopy(buff.array(), buff.arrayOffset(), value, 0, length);

        int dividerOffset = -1;
        for (int i = 0; i < value.length; i++) {
          if (value[i] == DIVIDER_BYTE) {
            dividerOffset = i;
            break;
          }
        }
        // We didn't have enough space to write the divider, return all of the bytes
        if (dividerOffset < 0) {
          return new String(value, StandardCharsets.UTF_8);
        }

        return new String(value, 0, dividerOffset, StandardCharsets.UTF_8);
      }

      LogWriter getLogWriter() throws IOException {
          LogStream logStream = null;
          try {
              logStream = this.client.getLog(logName);
          } catch (LogNotFoundException e) {
              LOG.info("Creating {}", logName);
              logStream = this.client.createLog(logName);
          }
          return logStream.createWriter();
      }
    }

    static class BulkWriter extends Operation {
        BulkWriter(LogName logName, LogServiceClient client, int numRecords, int logFreq,
                int valueSize) {
            super(logName, client, numRecords, logFreq, valueSize);
        }

        public void run() {
            try {
                LogWriter writer = getLogWriter();
                for (int i = 0; i < getNumRecords(); i++) {
                    String message = MESSAGE_PREFIX + i;
                    if (i % getLogFreq() == 0) {
                      LOG.info(getLogName() + " Writing " + message);
                    }
                    writer.write(createValue(message));
                }
                writer.close();
                LOG.info("{} entries written to {} successfully.", getNumRecords(), getLogName());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    static class BatchWriter extends Operation {
        private int batchSize;
        private int numBatches;
        BatchWriter(LogName logName, LogServiceClient client, int numRecords, int logFreq,
                int valueSize, int batchSize, int numBatches) {
            super(logName, client, numRecords, logFreq, valueSize);
            this.batchSize = batchSize;
            this.numBatches = numBatches;
        }

        public void run() {
            try {
                LogWriter writer = getLogWriter();
                for(int i = 0; i < numBatches; i++) {
                    List<ByteBuffer> messages = new ArrayList<ByteBuffer>(batchSize);
                    for(int j = 0; j < batchSize; j++) {
                        String message = MESSAGE_PREFIX + (i * batchSize + j);
                        messages.add(createValue(message));
                        if((i * batchSize + j) % getLogFreq() == 0) {
                            LOG.info(getLogName() + " batching write " + message);
                        }
                    }
                    try {
                        writer.write(messages);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }

                // Catch the last bit that didn't evenly fit into the batch sizes
                if (getNumRecords() % batchSize != 0) {
                  List<ByteBuffer> lastBatch = new ArrayList<>();
                  for (int i = numBatches * batchSize; i < getNumRecords(); i++) {
                    String message = MESSAGE_PREFIX + i;
                    lastBatch.add(createValue(message));
                  }
                  LOG.info(getLogName() + " writing last mini-batch of " + lastBatch.size() + " records");
                  try {
                    writer.write(lastBatch);
                  } catch (IOException e) {
                    throw new RuntimeException(e);
                  }
                }
                LOG.info("{} entries written in batches to {} successfully.",
                        getNumRecords(), getLogName());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    static class BulkReader extends Operation {
        BulkReader(LogName logName, LogServiceClient client, int numRecords, int logFreq, int valueSize) {
          super(logName, client, numRecords, logFreq, valueSize);
      }

        @SuppressFBWarnings("DM_EXIT")
        public void run() {
            try {
                LogStream logStream = getClient().getLog(getLogName());
                LogReader reader = logStream.createReader();
                long size = logStream.getLength();
                if(size != getNumRecords()) {
                    LOG.error("There is mismatch is number of records. Expected Records: "+
                        getNumRecords() +", Actual Records: " + size);
                    System.exit(-1);
                }
                for (int i = 0; i < size; i++) {
                    ByteBuffer buffer = reader.readNext();
                    String message = parseValue(buffer);
                    if (i % getLogFreq() == 0) {
                      LOG.info(getLogName() + " Read " + message);
                    }
                    if(!message.equals(MESSAGE_PREFIX + i)) {
                        LOG.error("Message is not correct. Expected: "+(MESSAGE_PREFIX + i)
                                +". Actual:" +message);
                        System.exit(-1);
                    }
                }
                LOG.info("{} log entries read from log {} successfully.", getNumRecords(), getLogName());
                reader.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
