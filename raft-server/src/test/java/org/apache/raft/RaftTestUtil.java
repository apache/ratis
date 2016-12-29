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
package org.apache.raft;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.raft.protocol.Message;
import org.apache.raft.server.BlockRequestHandlingInjection;
import org.apache.raft.server.DelayLocalExecutionInjection;
import org.apache.raft.server.RaftServer;
import org.apache.raft.server.RaftServerConfigKeys;
import org.apache.raft.shaded.com.google.protobuf.ByteString;
import org.apache.raft.shaded.proto.RaftProtos.LogEntryProto;
import org.apache.raft.shaded.proto.RaftProtos.SMLogEntryProto;
import org.apache.raft.util.CheckedRunnable;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collection;
import java.util.function.BooleanSupplier;
import java.util.function.IntSupplier;

import static org.apache.raft.util.ProtoUtils.toByteString;

public class RaftTestUtil {
  static final Logger LOG = LoggerFactory.getLogger(RaftTestUtil.class);

  public static RaftServer waitForLeader(MiniRaftCluster cluster)
      throws InterruptedException {
    final long sleepTime = (cluster.getMaxTimeout() * 3) >> 1;
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

  public static boolean logEntriesContains(LogEntryProto[] entries,
      SimpleMessage... expectedMessages) {
    int idxEntries = 0;
    int idxExpected = 0;
    while (idxEntries < entries.length
        && idxExpected < expectedMessages.length) {
      if (Arrays.equals(expectedMessages[idxExpected].getContent().toByteArray(),
          entries[idxEntries].getSmLogEntry().getData().toByteArray())) {
        ++idxExpected;
      }
      ++idxEntries;
    }
    return idxExpected == expectedMessages.length;
  }

  public static void assertLogEntries(Collection<RaftServer> servers,
                                      SimpleMessage... expectedMessages) {
    final int size = servers.size();
    final long count = servers.stream()
        .filter(RaftServer::isAlive)
        .map(s -> s.getState().getLog().getEntries(0, Long.MAX_VALUE))
        .filter(e -> logEntriesContains(e, expectedMessages))
        .count();
    if (2*count <= size) {
      throw new AssertionError("Not in majority: size=" + size
          + " but count=" + count);
    }
  }

  public static void assertLogEntries(LogEntryProto[] entries, long startIndex,
      long expertedTerm, SimpleMessage... expectedMessages) {
    Assert.assertEquals(expectedMessages.length, entries.length);
    for(int i = 0; i < entries.length; i++) {
      final LogEntryProto e = entries[i];
      Assert.assertEquals(expertedTerm, e.getTerm());
      Assert.assertEquals(startIndex + i, e.getIndex());
      Assert.assertArrayEquals(expectedMessages[i].getContent().toByteArray(),
          e.getSmLogEntry().getData().toByteArray());
    }
  }

  public static class SimpleMessage implements Message {
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
    public ByteString getContent() {
      return toByteString(messageId.getBytes(Charset.forName("UTF-8")));
    }
  }

  public static class SimpleOperation {
    private final String op;

    public SimpleOperation(String op) {
      Preconditions.checkArgument(op != null);
      this.op = op;
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
      try {
        return SMLogEntryProto.newBuilder()
            .setData(toByteString(op.getBytes("UTF-8"))).build();
      } catch (UnsupportedEncodingException e) {
        throw new RuntimeException(e);
      }
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

  public static void block(BooleanSupplier isBlocked) throws InterruptedException {
    for(; isBlocked.getAsBoolean(); ) {
      Thread.sleep(RaftServerConfigKeys.RAFT_SERVER_RPC_TIMEOUT_MAX_MS_DEFAULT);
    }
  }

  public static void delay(IntSupplier getDelayMs) throws InterruptedException {
    final int t = getDelayMs.getAsInt();
    if (t > 0) {
      Thread.sleep(t);
    }
  }

  public static <T extends Throwable> void attempt(
      int n, long sleepMs, CheckedRunnable<T> runnable)
      throws T, InterruptedException {
    for(int i = 1; i <= n; i++) {
      LOG.info("Attempt #" + i + "/" + n +  ": sleep " + sleepMs + "ms");
      if (sleepMs > 0) {
        Thread.sleep(sleepMs);
      }
      try {
        runnable.run();
        return;
      } catch (Throwable t) {
        if (i == n) {
          throw t;
        }
        LOG.warn("Attempt #" + i + "/" + n + ": Ignoring " + t + " and retry.");
      }
    }
  }

  public static String changeLeader(MiniRaftCluster cluster, String oldLeader)
      throws InterruptedException {
    cluster.setBlockRequestsFrom(oldLeader, true);
    String newLeader = oldLeader;
    for(int i = 0; i < 10 && newLeader.equals(oldLeader); i++) {
      newLeader = RaftTestUtil.waitForLeader(cluster).getId();
    }
    cluster.setBlockRequestsFrom(oldLeader, false);
    return newLeader;
  }

  public static void blockQueueAndSetDelay(Collection<RaftServer> servers,
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
    servers.stream().filter(s -> !s.getId().equals(leaderId))
        .forEach(s -> {
          if (block) {
            injection.setDelayMs(s.getId(), delayMs);
          } else {
            injection.removeDelay(s.getId());
          }
        });

    Thread.sleep(3 * maxTimeout);
  }

  public static void setBlockRequestsFrom(String src, boolean block) {
    if (block) {
      BlockRequestHandlingInjection.getInstance().blockRequestor(src);
    } else {
      BlockRequestHandlingInjection.getInstance().unblockRequestor(src);
    }
  }
}
