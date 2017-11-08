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
package org.apache.ratis;

import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.impl.BlockRequestHandlingInjection;
import org.apache.ratis.server.impl.DelayLocalExecutionInjection;
import org.apache.ratis.server.impl.RaftServerImpl;
import org.apache.ratis.server.impl.RaftServerProxy;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.storage.RaftLog;
import org.apache.ratis.shaded.com.google.protobuf.ByteString;
import org.apache.ratis.shaded.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.shaded.proto.RaftProtos.SMLogEntryProto;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.ProtoUtils;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;
import java.util.function.BooleanSupplier;
import java.util.function.IntSupplier;

public interface RaftTestUtil {

  Logger LOG = LoggerFactory.getLogger(RaftTestUtil.class);

  static RaftServerImpl getImplAsUnchecked(RaftServerProxy proxy) {
    return JavaUtils.callAsUnchecked(proxy::getImpl);
  }

  static RaftServerImpl waitForLeader(MiniRaftCluster cluster)
      throws InterruptedException {
    return waitForLeader(cluster, false);
  }

  static RaftServerImpl waitForLeader(
      MiniRaftCluster cluster, boolean tolerateMultipleLeaders) throws InterruptedException {
    return waitForLeader(cluster, tolerateMultipleLeaders, null);
  }

  static RaftServerImpl waitForLeader(
      MiniRaftCluster cluster, boolean tolerateMultipleLeaders, RaftGroupId groupId)
      throws InterruptedException {
    final long sleepTime = (cluster.getMaxTimeout() * 3) >> 1;
    LOG.info(cluster.printServers(groupId));
    RaftServerImpl leader = null;
    for(int i = 0; leader == null && i < 10; i++) {
      Thread.sleep(sleepTime);
      try {
        leader = cluster.getLeader(groupId);
      } catch(IllegalStateException e) {
        if (!tolerateMultipleLeaders) {
          throw e;
        }
      }
    }
    LOG.info(cluster.printServers(groupId));
    return leader;
  }

  static RaftServerImpl waitForLeader(
      MiniRaftCluster cluster, final String leaderId) throws InterruptedException {
    LOG.info(cluster.printServers());
    for(int i = 0; !cluster.tryEnforceLeader(leaderId) && i < 10; i++) {
      RaftServerImpl currLeader = cluster.getLeader();
      LOG.info("try enforcing leader to " + leaderId + " but " +
          (currLeader == null ? "no leader for this round" : "new leader is " + currLeader.getId()));
    }
    LOG.info(cluster.printServers());

    final RaftServerImpl leader = cluster.getLeader();
    Assert.assertEquals(leaderId, leader.getId().toString());
    return leader;
  }

  static String waitAndKillLeader(MiniRaftCluster cluster,
      boolean expectLeader) throws InterruptedException {
    final RaftServerImpl leader = waitForLeader(cluster);
    if (!expectLeader) {
      Assert.assertNull(leader);
    } else {
      Assert.assertNotNull(leader);
      LOG.info("killing leader = " + leader);
      cluster.killServer(leader.getId());
    }
    return leader != null ? leader.getId().toString() : null;
  }

  static boolean logEntriesContains(RaftLog log,
      SimpleMessage... expectedMessages) {
    int idxEntries = 0;
    int idxExpected = 0;
    TermIndex[] termIndices = log.getEntries(0, Long.MAX_VALUE);
    while (idxEntries < termIndices.length
        && idxExpected < expectedMessages.length) {
      try {
        if (Arrays.equals(expectedMessages[idxExpected].getContent().toByteArray(),
            log.get(termIndices[idxEntries].getIndex()).getSmLogEntry().getData().toByteArray())) {
          ++idxExpected;
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      ++idxEntries;
    }
    return idxExpected == expectedMessages.length;
  }

  static void assertLogEntries(Collection<RaftServerProxy> servers,
      SimpleMessage... expectedMessages) {
    final int size = servers.size();
    final long count = MiniRaftCluster.getServerStream(servers)
        .filter(RaftServerImpl::isAlive)
        .map(s -> s.getState().getLog())
        .filter(log -> logEntriesContains(log, expectedMessages))
        .count();
    if (2*count <= size) {
      throw new AssertionError("Not in majority: size=" + size
          + " but count=" + count);
    }
  }

  static void assertLogEntries(RaftLog log, TermIndex[] entries,
      long startIndex, long expertedTerm, SimpleMessage... expectedMessages) {
    Assert.assertEquals(expectedMessages.length, entries.length);
    for(int i = 0; i < entries.length; i++) {
      final LogEntryProto e;
      try {
        e = log.get(entries[i].getIndex());
      } catch (IOException exception) {
        throw new RuntimeException(exception);
      }
      Assert.assertEquals(expertedTerm, e.getTerm());
      Assert.assertEquals(startIndex + i, e.getIndex());
      Assert.assertArrayEquals(expectedMessages[i].getContent().toByteArray(),
          e.getSmLogEntry().getData().toByteArray());
    }
  }

  class SimpleMessage implements Message {
    public static SimpleMessage[] create(int numMessages) {
      return create(numMessages, "m");
    }

    public static SimpleMessage[] create(int numMessages, String prefix) {
      final SimpleMessage[] messages = new SimpleMessage[numMessages];
      for (int i = 0; i < messages.length; i++) {
        messages[i] = new SimpleMessage(prefix + i);
      }
      return messages;
    }

    final String messageId;
    final ByteString bytes;

    public SimpleMessage(final String messageId) {
      this.messageId = messageId;
      this.bytes = ProtoUtils.toByteString(messageId);
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
    public ByteString getContent() {
      return bytes;
    }
  }

  class SimpleOperation {
    private final String op;
    private final SMLogEntryProto smLogEntryProto;

    public SimpleOperation(String op) {
      this.op = Objects.requireNonNull(op);
      this.smLogEntryProto = SMLogEntryProto.newBuilder()
          .setData(ProtoUtils.toByteString(op)).build();
    }

    @Override
    public String toString() {
      return op;
    }

    @Override
    public boolean equals(Object obj) {
      return obj == this ||
          (obj instanceof SimpleOperation &&
              ((SimpleOperation) obj).op.equals(op));
    }

    @Override
    public int hashCode() {
      return op.hashCode();
    }

    public SMLogEntryProto getLogEntryContent() {
      return smLogEntryProto;
    }
  }

  static void block(BooleanSupplier isBlocked) throws InterruptedException {
    for(; isBlocked.getAsBoolean(); ) {
      RaftServerConfigKeys.Rpc.TIMEOUT_MAX_DEFAULT.sleep();
    }
  }

  static void delay(IntSupplier getDelayMs) throws InterruptedException {
    final int t = getDelayMs.getAsInt();
    if (t > 0) {
      Thread.sleep(t);
    }
  }

  static RaftPeerId changeLeader(MiniRaftCluster cluster, RaftPeerId oldLeader)
      throws InterruptedException {
    cluster.setBlockRequestsFrom(oldLeader.toString(), true);
    try {
      return JavaUtils.attempt(() -> {
        final RaftPeerId newLeader = waitForLeader(cluster).getId();
        Preconditions.assertTrue(!newLeader.equals(oldLeader),
            () -> "Failed to change leader: newLeader=" + newLeader + " equals oldLeader=" + oldLeader);
        LOG.info("Changed leader from " + oldLeader + " to " + newLeader);
        return newLeader;
      }, 10, 100L, "changeLeader", LOG);
    } finally {
      cluster.setBlockRequestsFrom(oldLeader.toString(), false);
    }
  }

  static <SERVER extends RaftServer> void blockQueueAndSetDelay(
      Collection<SERVER> servers,
      DelayLocalExecutionInjection injection, String leaderId, int delayMs,
      long maxTimeout) throws InterruptedException {
    // block reqeusts sent to leader if delayMs > 0
    final boolean block = delayMs > 0;
    LOG.debug("{} requests sent to leader {} and set {}ms delay for the others",
        block? "Block": "Unblock", leaderId, delayMs);
    if (block) {
      BlockRequestHandlingInjection.getInstance().blockReplier(leaderId);
    } else {
      BlockRequestHandlingInjection.getInstance().unblockReplier(leaderId);
    }

    // delay RaftServerRequest for other servers
    servers.stream().filter(s -> !s.getId().toString().equals(leaderId))
        .forEach(s -> {
          if (block) {
            injection.setDelayMs(s.getId().toString(), delayMs);
          } else {
            injection.removeDelay(s.getId().toString());
          }
        });

    Thread.sleep(3 * maxTimeout);
  }
}
