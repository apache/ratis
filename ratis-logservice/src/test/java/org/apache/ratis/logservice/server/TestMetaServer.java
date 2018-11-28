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

package org.apache.ratis.logservice.server;

import org.apache.ratis.logservice.api.*;
import org.apache.ratis.logservice.client.LogServiceClient;
import org.apache.ratis.logservice.common.LogAlreadyExistException;
import org.apache.ratis.logservice.common.LogNotFoundException;
import org.apache.ratis.logservice.server.LogServer;
import org.apache.ratis.logservice.util.LogServiceCluster;
import org.apache.ratis.server.impl.RaftServerImpl;
import org.apache.ratis.server.impl.RaftServerProxy;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.IntStream;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;


public class TestMetaServer {

    static LogServiceCluster cluster = null;

    @BeforeClass
    public static void beforeClass() {
        cluster = new LogServiceCluster(3);
        cluster.createWorkers(3);
        List<LogServer> workers = cluster.getWorkers();
        assert(workers.size() == 3);
    }

    @AfterClass
    public static void afterClass() {
        if (cluster != null) {
          cluster.close();
        }
    }

    /**
     * Simple test for create a new log and get it.
     * @throws IOException
     */
    @Test
    public void testCreateAndGetLog() throws IOException {
        LogServiceClient client = new LogServiceClient(cluster.getMetaIdentity());
        // This should be LogServiceStream ?
        LogStream logStream1 = client.createLog(LogName.of("testCreateLog"));
        assertNotNull(logStream1);
        LogStream logStream2 = client.getLog(LogName.of("testCreateLog"));
        assertNotNull(logStream2);
    }


    @Test
    public void testReadWritetoLog() throws IOException, InterruptedException {
        LogServiceClient client = new LogServiceClient(cluster.getMetaIdentity());
        LogStream stream = client.createLog(LogName.of("testReadWrite"));
        LogWriter writer = stream.createWriter();
        ByteBuffer testMessage =  ByteBuffer.wrap("Hello world!".getBytes());
        List<LogInfo> listLogs = client.listLogs();
        assert(listLogs.stream().filter(log -> log.getLogName().getName().startsWith("testReadWrite")).count() == 1);
        List<LogServer> workers = cluster.getWorkers();
        for(LogServer worker : workers) {
             RaftServerImpl server = ((RaftServerProxy)worker.getServer())
                     .getImpl(listLogs.get(0).getRaftGroup().getGroupId());
        // TODO: perform all additional checks on state machine level
        }
        writer.write(testMessage);
        for(LogServer worker : workers) {
            RaftServerImpl server = ((RaftServerProxy)worker.getServer())
                    .getImpl(listLogs.get(0).getRaftGroup().getGroupId());
        }
//        assert(stream.getSize() > 0); //TODO: Doesn't work
        LogReader reader = stream.createReader();
        ByteBuffer res = reader.readNext(); //TODO: first is conf log entry
        res = reader.readNext();
        assert(res.array().length > 0);
    }

    /**
     * Test for Delete operation
     * @throws IOException
     */

    @Test
    public void testDeleteLog() throws IOException {
        LogServiceClient client = new LogServiceClient(cluster.getMetaIdentity());
        // This should be LogServiceStream ?
        LogStream logStream1 = client.createLog(LogName.of("testDeleteLog"));
        assertNotNull(logStream1);
        client.deleteLog(LogName.of("testDeleteLog"));
        try {
          logStream1 = client.getLog(LogName.of("testDeleteLog"));
            fail("Failed to throw LogNotFoundException");
        } catch(Exception e) {
            assert(e instanceof LogNotFoundException);
        }


    }
    /**
     * Test for getting not existing log. Should throw an exception
     * @throws IOException
     */
    @Test
    public void testGetNotExistingLog() {
        LogServiceClient client = new LogServiceClient(cluster.getMetaIdentity());
        try {
            LogStream log = client.getLog(LogName.of("no_such_log"));
            fail("LogNotFoundException was not thrown");
        } catch (IOException e) {
            assert(e instanceof LogNotFoundException);
        }
    }

    /**
     * Test for an exception during log creation if a log with the same name already exist.
     * @throws IOException
     */
    @Test
    public void testAlreadyExistLog() throws IOException {
        LogServiceClient client = new LogServiceClient(cluster.getMetaIdentity());
        LogStream logStream1 = client.createLog(LogName.of("test1"));
        assertNotNull(logStream1);
        try {
            logStream1 = client.createLog(LogName.of("test1"));
            fail("Didn't fail with LogAlreadyExistException");
        } catch (IOException e) {
            assert(e instanceof LogAlreadyExistException);
        }
    }

    /**
     * Test list operation. 7 logs are created with follow up check that all are listed
     * @throws IOException
     */
    @Test
    public void testListLogs() throws IOException {
        LogServiceClient client = new LogServiceClient(cluster.getMetaIdentity());
        client.createLog(LogName.of("listLogTest1"));
        client.createLog(LogName.of("listLogTest2"));
        client.createLog(LogName.of("listLogTest3"));
        client.createLog(LogName.of("listLogTest4"));
        client.createLog(LogName.of("listLogTest5"));
        client.createLog(LogName.of("listLogTest6"));
        client.createLog(LogName.of("listLogTest7"));
        List<LogInfo> list = client.listLogs();
        assert(list.stream().filter(log -> log.getLogName().getName().startsWith("listLogTest")).count() == 7);

    }

    @Ignore ("Too heavy for the current implementation")
    @Test
    public void testFinalClieanUp() throws IOException {
        LogServiceClient client = new LogServiceClient(cluster.getMetaIdentity());
        IntStream.range(0, 10).forEach(i -> {
            try {
                client.createLog(LogName.of("CleanTest" + i));
            } catch (IOException e) {
                throw new RuntimeException(e);
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
