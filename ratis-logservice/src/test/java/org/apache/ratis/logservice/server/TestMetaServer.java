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

package org.apache.ratis.logservice.server;

import org.apache.ratis.client.RaftClientConfigKeys;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.logservice.api.*;
import org.apache.ratis.logservice.api.LogStream.State;
import org.apache.ratis.logservice.api.LogServiceClient;
import org.apache.ratis.logservice.common.LogAlreadyExistException;
import org.apache.ratis.logservice.common.LogNotFoundException;
import org.apache.ratis.logservice.metrics.LogServiceMetaDataMetrics;
import org.apache.ratis.logservice.metrics.LogServiceMetrics;
import org.apache.ratis.logservice.proto.MetaServiceProtos;
import org.apache.ratis.logservice.util.LogServiceCluster;
import org.apache.ratis.logservice.util.TestUtils;
import org.apache.ratis.metrics.JVMMetrics;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.TimeDuration;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import javax.management.InstanceNotFoundException;
import javax.management.ObjectName;

import static org.junit.Assert.*;

public class TestMetaServer {
    private static final Logger LOG = LoggerFactory.getLogger(TestMetaServer.class);

    static {
        JVMMetrics.initJvmMetrics(TimeDuration.valueOf(10, TimeUnit.SECONDS));
    }

    static LogServiceCluster cluster = null;
    static List<LogServer> workers = null;
    static AtomicInteger createCount = new AtomicInteger();
    static AtomicInteger deleteCount = new AtomicInteger();
    static AtomicInteger listCount = new AtomicInteger();
    static LogServiceClient client = null;

    @BeforeClass
    public static void beforeClass() {
        cluster = new LogServiceCluster(3);
        cluster.createWorkers(3);
        workers = cluster.getWorkers();
        assert(workers.size() == 3);

        RaftProperties properties = new RaftProperties();
        RaftClientConfigKeys.Rpc.setRequestTimeout(properties, TimeDuration.valueOf(15, TimeUnit.SECONDS));

        cluster.getMasters().parallelStream().forEach(master ->
            ((MetaStateMachine)master.getMetaStateMachine()).setProperties(properties));

        client = new LogServiceClient(cluster.getMetaIdentity(), properties) {
          @Override
          public LogStream createLog(LogName logName) throws IOException {
            createCount.incrementAndGet();
            return super.createLog(logName);
          }

          @Override
          public void deleteLog(LogName logName) throws IOException {
            deleteCount.incrementAndGet();
            super.deleteLog(logName);
          }

          @Override
          public List<LogInfo> listLogs() throws IOException {
            listCount.incrementAndGet();
            return super.listLogs();
          }
        };
    }

    @AfterClass
    public static void afterClass() {
        if (cluster != null) {
          cluster.close();
        }

        if (client != null) {
          try {
            client.close();
          } catch (Exception ignored) {
          }
        }
    }

    @Before
    public void before() {
        // ensure workers before each test
        if (workers.size() < 3) {
            cluster.createWorkers(3 - workers.size());
        }
    }

    /**
     * Simple test for create a new log and get it.
     * @throws IOException
     */
    @Test
    public void testCreateAndGetLog() throws Exception {
        // This should be LogServiceStream ?
        try (LogStream logStream1 = client.createLog(LogName.of("testCreateLog"))) {
            assertNotNull(logStream1);
        }
        try (LogStream logStream2 = client.getLog(LogName.of("testCreateLog"))) {
            assertNotNull(logStream2);
        }
    }

    /**
     * Test closing log any of the peer in .
     * @throws IOException
     */
    @Test
    public void testCloseLogOnNodeFailure() throws Exception {
        boolean peerClosed = false;
        try {
            for(int i = 0; i < 5; i++) {
                try (LogStream logStream1 = client.createLog(LogName.of("testCloseLogOnNodeFailure"+i))) {
                    assertNotNull(logStream1);
                }
            }
            assertTrue(((MetaStateMachine)cluster.getMasters().get(0).getMetaStateMachine()).checkPeersAreSame());
            workers.get(0).close();
            peerClosed = true;
            Thread.sleep(90000);
            assertTrue(((MetaStateMachine)cluster.getMasters().get(0).getMetaStateMachine()).checkPeersAreSame());
            for(int i = 0; i < 5; i++) {
                try (LogStream logStream2 = client.getLog(LogName.of("testCloseLogOnNodeFailure"+i))) {
                    assertNotNull(logStream2);
                    assertEquals(State.CLOSED, logStream2.getState());
                }
            }
        } finally {
            if(peerClosed) {
                // recreate the worker closed in the test.
                cluster.createWorkers(1);
            }
        }
    }

    @Test
    public void testReadWritetoLog() throws Exception {
        try (LogStream stream = client.createLog(LogName.of("testReadWrite"))) {
            LogWriter writer = stream.createWriter();
            ByteBuffer testMessage = ByteBuffer.wrap("Hello world!".getBytes());
            List<LogInfo> listLogs = client.listLogs();
            assert (listLogs.stream().filter(log -> log.getLogName().getName().startsWith("testReadWrite")).count() == 1);
            List<LogServer> workers = cluster.getWorkers();
            for (LogServer worker : workers) {
                worker.getServer().getDivision(listLogs.get(0).getRaftGroup().getGroupId());
                // TODO: perform all additional checks on state machine level
            }
            writer.write(testMessage);
            for (LogServer worker : workers) {
                worker.getServer().getDivision(listLogs.get(0).getRaftGroup().getGroupId());
            }
//        assert(stream.getSize() > 0); //TODO: Doesn't work
            LogReader reader = stream.createReader();
            ByteBuffer res = reader.readNext();
            assert (res.array().length > 0);
        }
    }

    @Test
    public void testLogArchival() throws Exception {
        LogName logName = LogName.of("testArchivalLog");
        try (LogStream logStream = client.createLog(logName)) {
            LogWriter writer = logStream.createWriter();
            List<LogInfo> listLogs = client.listLogs();
            assert (listLogs.stream()
                    .filter(log -> log.getLogName().getName().startsWith(logName.getName())).count() == 1);
            List<LogServer> workers = cluster.getWorkers();
            List<ByteBuffer> records = TestUtils.getRandomData(100, 10);
            writer.write(records);
            client.closeLog(logName);
            assertEquals(logStream.getState(), State.CLOSED);
            client.archiveLog(logName);
            int retry = 0;
            while (logStream.getState() != State.ARCHIVED && retry <= 40) {
                Thread.sleep(1000);
                retry++;
            }
            assertEquals(logStream.getState(), State.ARCHIVED);
            LogReader reader = logStream.createReader();
            List<ByteBuffer> data = reader.readBulk(records.size());
            assertEquals(records.size(), data.size());
            reader.seek(1);
            data = reader.readBulk(records.size());
            assertEquals(records.size() - 1, data.size());

            //Test ArchiveLogStream
            LogServiceConfiguration config = LogServiceConfiguration.create();
            LogStream archiveLogStream = client.getArchivedLog(logName);
            reader = archiveLogStream.createReader();
            data = reader.readBulk(records.size());
            assertEquals(records.size(), data.size());
        }
    }

    @Test
    public void testLogExport() throws Exception {
        LogName logName = LogName.of("testLogExport");
        try (LogStream logStream = client.createLog(logName)) {
            LogWriter writer = logStream.createWriter();
            List<LogInfo> listLogs = client.listLogs();
            assert (listLogs.stream()
                    .filter(log -> log.getLogName().getName().startsWith(logName.getName())).count() == 1);
            List<LogServer> workers = cluster.getWorkers();
            List<ByteBuffer> records = TestUtils.getRandomData(100, 10);
            writer.write(records);
            String location1 = "target/tmp/export_1/";
            String location2 = "target/tmp/export_2/";
            deleteLocalDirectory(new File(location1));
            deleteLocalDirectory(new File(location2));
            int startPosition1 = 3;
            int startPosition2 = 5;
            client.exportLog(logName, location1, startPosition1);
            client.exportLog(logName, location2, startPosition2);
            List<ArchivalInfo> infos = client.getExportStatus(logName);
            int count = 0;
            while (infos.size() > 1 && (
                    infos.get(0).getStatus() != ArchivalInfo.ArchivalStatus.COMPLETED
                            || infos.get(1).getStatus() != ArchivalInfo.ArchivalStatus.COMPLETED)
                    && count < 10) {
                infos = client.getExportStatus(logName);
                ;
                Thread.sleep(1000);
                count++;

            }

            //Test ExportLogStream
            LogStream exportLogStream = client.getExportLog(logName, location1);
            LogReader reader = exportLogStream.createReader();
            List<ByteBuffer> data = reader.readBulk(records.size());
            assertEquals(records.size() - startPosition1, data.size());
            reader.close();
            exportLogStream = client.getExportLog(logName, location2);
            reader = exportLogStream.createReader();
            data = reader.readBulk(records.size());
            assertEquals(records.size() - startPosition2, data.size());
            reader.close();
            writer.close();
        }
    }

    boolean deleteLocalDirectory(File dir) {
        File[] allFiles = dir.listFiles();
        if (allFiles != null) {
            for (File file : allFiles) {
                deleteLocalDirectory(file);
            }
        }
        return dir.delete();
    }


    /**
     * Test for Delete operation
     * @throws IOException
     */

    @Test
    public void testDeleteLog() throws Exception {
        // This should be LogServiceStream ?
        try (LogStream logStream1 = client.createLog(LogName.of("testDeleteLog"))) {
            assertNotNull(logStream1);
            client.deleteLog(LogName.of("testDeleteLog"));
            testJMXCount(MetaServiceProtos.MetaServiceRequestProto.TypeCase.DELETELOG.name(),
                    (long) deleteCount.get());
            LogStream logStream2 = null;
            try {
                logStream2 = client.getLog(LogName.of("testDeleteLog"));
                fail("Failed to throw LogNotFoundException");
            } catch (Exception e) {
                assert (e instanceof LogNotFoundException);
            } finally {
                if (logStream2 != null) {
                    logStream2.close();
                }
            }
        }
    }
    /**
     * Test for getting not existing log. Should throw an exception
     * @throws IOException
     */
    @Test
    public void testGetNotExistingLog() throws Exception {
        LogStream logStream = null;
        try {
            logStream = client.getLog(LogName.of("no_such_log"));
            fail("LogNotFoundException was not thrown");
        } catch (IOException e) {
            assert(e instanceof LogNotFoundException);
        } finally {
            if (logStream != null) {
               logStream.close();
            }
        }
    }

    /**
     * Test for an exception during log creation if a log with the same name already exist.
     * @throws IOException
     */
    @Test
    public void testAlreadyExistLog() throws Exception {
        try (LogStream logStream1 = client.createLog(LogName.of("test1"))) {
            assertNotNull(logStream1);
            LogStream logStream2 = null;
            try {
                logStream2 = client.createLog(LogName.of("test1"));
                fail("Didn't fail with LogAlreadyExistException");
            } catch (IOException e) {
                assert (e instanceof LogAlreadyExistException);
            } finally {
                if (logStream2 != null) {
                    logStream2.close();
                }
            }
        }
    }

    private void createLogAndClose(String name) throws Exception {
        LogStream logStream = null;
        try {
            logStream = client.createLog(LogName.of(name));
        } finally {
            if (logStream != null) {
                logStream.close();
            }
        }
    }

    /**
     * Test list operation. 7 logs are created with follow up check that all are listed
     * @throws IOException
     */
    @Test
    public void testListLogs() throws Exception {
        createLogAndClose("listLogTest1");
        createLogAndClose("listLogTest2");
        createLogAndClose("listLogTest3");
        createLogAndClose("listLogTest4");
        createLogAndClose("listLogTest5");
        createLogAndClose("listLogTest6");
        createLogAndClose("listLogTest7");
        // Test jmx

        List<LogInfo> list = client.listLogs();
        testJMXCount(MetaServiceProtos.MetaServiceRequestProto.TypeCase.CREATELOG.name(),
            (long) createCount.get() );
        testJMXCount(MetaServiceProtos.MetaServiceRequestProto.TypeCase.LISTLOGS.name(),listCount.longValue());
        assert(list.stream().filter(log -> log.getLogName().getName().startsWith("listLogTest")).count() == 7);

    }

    private void testJMXCount(String metricName, Long expectedCount) throws Exception {
        assertEquals(expectedCount, getJMXCount(metricName));
    }

    private Long getJMXCount(String metricName) throws Exception {
        for (MetadataServer master : cluster.getMasters()) {
            ObjectName oname =
                new ObjectName(LogServiceMetrics.RATIS_LOG_SERVICE_METRICS, "name",
                    new LogServiceMetaDataMetrics(master.getId()).getRegistry()
                        .getMetricRegistryInfo().getName() + "." + metricName);
            try {
                return (Long) ManagementFactory.getPlatformMBeanServer()
                    .getAttribute(oname, "Count");
            } catch (InstanceNotFoundException e) {

            }
        }
        throw new InstanceNotFoundException();
    }

    @Ignore ("Too heavy for the current implementation")
    @Test
    public void testFinalClieanUp() throws Exception {
        IntStream.range(0, 10).forEach(i -> {
            LogStream logStream = null;
            try {
                logStream = client.createLog(LogName.of("CleanTest" + i));
            } catch (IOException e) {
                throw new RuntimeException(e);
            } finally {
                if (logStream != null) {
                    try {
                        logStream.close();
                    } catch (Exception ignored) {
                        LOG.warn("{} is ignored", JavaUtils.getClassSimpleName(ignored.getClass()), ignored);
                    }
                }
            }
        });
        List<LogInfo> list = client.listLogs();
        list.parallelStream().forEach(loginfo -> {
            try {
                client.deleteLog(loginfo.getLogName());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        list = client.listLogs();
        assert(list.size() == 0);

    }
}
