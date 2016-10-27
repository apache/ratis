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
package org.apache.raft.grpc;

import org.apache.raft.conf.RaftProperties;
import org.apache.raft.grpc.client.RaftOutputStream;
import org.apache.raft.grpc.client.RaftWriter;
import org.apache.raft.grpc.server.PipelinedLogAppenderFactory;
import org.apache.raft.proto.RaftProtos.LogEntryProto;
import org.apache.raft.server.LogAppenderFactory;
import org.apache.raft.server.RaftServer;
import org.apache.raft.server.storage.RaftLog;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.raft.RaftTestUtil.waitForLeader;
import static org.apache.raft.grpc.RaftGrpcConfigKeys.RAFT_OUTPUTSTREAM_BUFFER_SIZE_KEY;
import static org.apache.raft.server.RaftServerConfigKeys.RAFT_SERVER_LOG_APPENDER_FACTORY_CLASS_KEY;

public class TestRaftStream {
  static final Logger LOG = LoggerFactory.getLogger(TestRaftStream.class);

  private static final RaftProperties prop = new RaftProperties();
  private static final int NUM_SERVERS = 3;

  private MiniRaftClusterWithGRpc cluster;
  private int content;

  @BeforeClass
  public static void setProp() {
    prop.setClass(RAFT_SERVER_LOG_APPENDER_FACTORY_CLASS_KEY,
        PipelinedLogAppenderFactory.class, LogAppenderFactory.class);
    // default 64K is too large for a test
    prop.setInt(RAFT_OUTPUTSTREAM_BUFFER_SIZE_KEY, 4);
  }

  @Before
  public void setup() throws Exception {
    content = 1;
    cluster = new MiniRaftClusterWithGRpc(NUM_SERVERS, prop);
    cluster.start();
  }

  @After
  public void tearDown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  private byte[] genContent() {
    return toBytes(content++);
  }

  private byte[] toBytes(int i) {
    byte[] b = new byte[4];
    b[0] = (byte) ((i >>> 24) & 0xFF);
    b[1] = (byte) ((i >>> 16) & 0xFF);
    b[2] = (byte) ((i >>> 8) & 0xFF);
    b[3] = (byte) (i & 0xFF);
    return b;
  }

  private int toInt(byte[] b) {
    int result = 0;
    for (int i = 0; i < 4; i++) {
      result = (result << 8) + (b[i] & 0xFF);
    }
    return result;
  }

  @Test
  public void testSimpleWrite() throws Exception {
    LOG.info("Running testSimpleWrite");
    RaftServer leader = waitForLeader(cluster);

    RaftWriter writer = new RaftWriter("writer-1", cluster.getPeers(),
        leader.getId(), prop);
    try (RaftOutputStream out = writer.write()) {
      for (int i = 0; i < 500; i++) { // generate 500 requests
        out.write(genContent());
      }
    }

    // check the leader's raft log
    final RaftLog raftLog = leader.getState().getLog();
    long committedIndex = raftLog.getLastCommittedIndex();
    // leader's placeholder + 500 entries from client
    Assert.assertEquals(500, committedIndex);
    // check the log content
    LogEntryProto[] entries = raftLog.getEntries(1, 501);
    int currentNum = 1;
    for (LogEntryProto entry : entries) {
      byte[] logData = entry.getSmLogEntry().getData().toByteArray();
      Assert.assertEquals(4, logData.length);
      Assert.assertEquals(currentNum++, toInt(logData));
    }
  }
}
