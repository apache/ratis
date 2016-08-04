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
package org.apache.hadoop.raft;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.raft.proto.RaftProtos.LogEntryProto;
import org.apache.hadoop.raft.protocol.Message;
import org.apache.hadoop.raft.server.RaftConstants;
import org.apache.hadoop.raft.server.RaftServer;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;

import static org.junit.Assert.assertTrue;

public class RaftTestUtil {
  static final Logger LOG = LoggerFactory.getLogger(RaftTestUtil.class);

  public static RaftServer waitForLeader(MiniRaftCluster cluster)
      throws InterruptedException {
    final long sleepTime = (RaftConstants.ELECTION_TIMEOUT_MAX_MS * 3) >> 1;
    LOG.info(cluster.printServers());
    RaftServer leader = null;
    for(int i = 0; leader == null && i < 10; i++) {
      Thread.sleep(sleepTime);
      leader = cluster.getLeader();
    }
    LOG.info(cluster.printServers());
    return leader;
  }

  public static RaftServer waitForLeader(MiniRaftCluster cluster,
      final String leaderId) throws InterruptedException {
    LOG.info(cluster.printServers());
    for(int i = 0; !cluster.tryEnforceLeader(leaderId) && i < 10; i++) {
      RaftServer currLeader = cluster.getLeader();
      if (LOG.isDebugEnabled()) {
        LOG.debug("try enforcing leader to " + leaderId + " but "
            + (currLeader == null? "no leader for this round"
                : "new leader is " + currLeader.getId()));
      }
    }
    LOG.info(cluster.printServers());

    final RaftServer leader = cluster.getLeader();
    Assert.assertEquals(leaderId, leader.getId());
    return leader;
  }

  public static String waitAndKillLeader(MiniRaftCluster cluster,
      boolean expectLeader) throws InterruptedException {
    final RaftServer leader = waitForLeader(cluster);
    if (!expectLeader) {
      Assert.assertNull(leader);
    } else {
      Assert.assertNotNull(leader);
      LOG.info("killing leader = " + leader);
      cluster.killServer(leader.getId());
    }
    return leader != null ? leader.getId() : null;
  }

  public static void assertLogEntriesContains(LogEntryProto[] entries,
      SimpleMessage... expectedMessages) {
    int idxEntries = 0;
    int idxExpected = 0;
    while (idxEntries < entries.length
        && idxExpected < expectedMessages.length) {
      if (Arrays.equals(expectedMessages[idxExpected].getContent(),
          entries[idxEntries].getClientMessageEntry().getContent().toByteArray())) {
        ++idxExpected;
      }
      ++idxEntries;
    }
    assertTrue("Total number of matched records: " + idxExpected,
        expectedMessages.length == idxExpected);
  }

  public static void assertLogEntries(LogEntryProto[] entries, long startIndex,
      long expertedTerm, SimpleMessage... expectedMessages) {
    Assert.assertEquals(expectedMessages.length, entries.length);
    for(int i = 0; i < entries.length; i++) {
      final LogEntryProto e = entries[i];
      Assert.assertEquals(expertedTerm, e.getTerm());
      Assert.assertEquals(startIndex + i, e.getIndex());
      Assert.assertArrayEquals(expectedMessages[i].getContent(),
          e.getClientMessageEntry().getContent().toByteArray());
    }
  }

  public static class SimpleMessage implements Message {
    public static SimpleMessage[] create(int numMessages) {
      final SimpleMessage[] messages = new SimpleMessage[numMessages];
      for (int i = 0; i < messages.length; i++) {
        messages[i] = new SimpleMessage("m" + i);
      }
      return messages;
    }

    final String messageId;

    public SimpleMessage(final String messageId) {
      this.messageId = messageId;
    }

    @Override
    public String toString() {
      return messageId;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == this) {
        return true;
      } else if (obj == null || !(obj instanceof SimpleMessage)) {
        return false;
      } else {
        final SimpleMessage that = (SimpleMessage)obj;
        return this.messageId.equals(that.messageId);
      }
    }

    @Override
    public int hashCode() {
      return messageId.hashCode();
    }

    @Override
    public byte[] getContent() {
      return messageId.getBytes(Charset.forName("UTF-8"));
    }
  }

  public static File getTestDir(Class<?> caller) throws IOException {
    File dir = new File(System.getProperty("test.build.data", "target/test/data")
            + "/" + RandomStringUtils.randomAlphanumeric(10),
            caller.getSimpleName());
    if (dir.exists() && !dir.isDirectory()) {
      throw new IOException(dir + " already exists and is not a directory");
    } else if (!dir.exists() && !dir.mkdirs()) {
      throw new IOException("Cannot create directory " + dir);
    }
    return dir;
  }

  public interface IsBlocked {
    boolean isBlocked();
  }

  public static void block(IsBlocked impl) throws InterruptedException {
    for(; impl.isBlocked(); ) {
      Thread.sleep(RaftConstants.ELECTION_TIMEOUT_MAX_MS);
    }
  }

  public interface GetDelay {
    int getDelayMs();
  }

  public static void delay(GetDelay impl) throws InterruptedException {
    final int t = impl.getDelayMs();
    if (t > 0) {
      Thread.sleep(t);
    }
  }
}
