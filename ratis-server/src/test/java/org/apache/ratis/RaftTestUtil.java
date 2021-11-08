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
package org.apache.ratis;

import org.apache.ratis.client.RaftClient;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.proto.RaftProtos.LogEntryProto.LogEntryBodyCase;
import org.apache.ratis.proto.RaftProtos.StateMachineLogEntryProto;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.impl.BlockRequestHandlingInjection;
import org.apache.ratis.server.impl.DelayLocalExecutionInjection;
import org.apache.ratis.server.impl.MiniRaftCluster;
import org.apache.ratis.server.raftlog.LogEntryHeader;
import org.apache.ratis.server.raftlog.LogProtoUtils;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.server.raftlog.RaftLogBase;
import org.apache.ratis.thirdparty.com.google.common.base.Preconditions;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.AutoCloseableLock;
import org.apache.ratis.util.CollectionUtils;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.ProtoUtils;
import org.apache.ratis.util.TimeDuration;
import org.junit.Assert;
import org.junit.AssumptionViolatedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntSupplier;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.StreamSupport;

public interface RaftTestUtil {
  Logger LOG = LoggerFactory.getLogger(RaftTestUtil.class);

  static Object getDeclaredField(Object obj, String fieldName) {
    final Class<?> clazz = obj.getClass();
    try {
      final Field f = clazz.getDeclaredField(fieldName);
      f.setAccessible(true);
      return f.get(obj);
    } catch (Exception e) {
      throw new IllegalStateException("Failed to get '" + fieldName + "' from " + clazz, e);
    }
  }

  static RaftServer.Division waitForLeader(MiniRaftCluster cluster)
      throws InterruptedException {
    return waitForLeader(cluster, null);
  }

  static RaftServer.Division waitForLeader(MiniRaftCluster cluster, RaftGroupId groupId)
      throws InterruptedException {
    return waitForLeader(cluster, groupId, true);
  }

  static RaftServer.Division waitForLeader(MiniRaftCluster cluster, RaftGroupId groupId, boolean expectLeader)
      throws InterruptedException {
    final String name = "waitForLeader-" + groupId + "-(expectLeader? " + expectLeader + ")";
    final int numAttempts = expectLeader? 100: 10;
    final TimeDuration sleepTime = cluster.getTimeoutMax().apply(d -> (d * 3) >> 1);
    LOG.info(cluster.printServers(groupId));

    final AtomicReference<IllegalStateException> exception = new AtomicReference<>();
    final Runnable handleNoLeaders = () -> {
      throw cluster.newIllegalStateExceptionForNoLeaders(groupId);
    };
    final Consumer<List<RaftServer.Division>> handleMultipleLeaders = leaders -> {
      final IllegalStateException ise = cluster.newIllegalStateExceptionForMultipleLeaders(groupId, leaders);
      exception.set(ise);
    };

    final RaftServer.Division leader = JavaUtils.attemptRepeatedly(() -> {
      final RaftServer.Division l = cluster.getLeader(groupId, handleNoLeaders, handleMultipleLeaders);
      if (l != null && !l.getInfo().isLeaderReady()) {
        throw new IllegalStateException("Leader: "+ l.getMemberId() +  " not ready");
      }
      return l;
    }, numAttempts, sleepTime, name, LOG);

    LOG.info(cluster.printServers(groupId));
    if (expectLeader) {
      return Optional.ofNullable(leader).orElseThrow(exception::get);
    } else {
      if (leader == null) {
        return null;
      } else {
        throw new IllegalStateException("expectLeader = " + expectLeader + " but leader = " + leader);
      }
    }
  }

  static RaftPeerId waitAndKillLeader(MiniRaftCluster cluster) throws InterruptedException {
    final RaftServer.Division leader = waitForLeader(cluster);
    Assert.assertNotNull(leader);

    LOG.info("killing leader = " + leader);
    cluster.killServer(leader.getId());
    return leader.getId();
  }

  static void waitFor(Supplier<Boolean> check, int checkEveryMillis,
      int waitForMillis) throws TimeoutException, InterruptedException {
    Preconditions.checkNotNull(check);
    Preconditions.checkArgument(waitForMillis >= checkEveryMillis);

    long st = System.currentTimeMillis();
    boolean result = check.get();

    while (!result && (System.currentTimeMillis() - st < waitForMillis)) {
      Thread.sleep(checkEveryMillis);
      result = check.get();
    }

    if (!result) {
      throw new TimeoutException("Timed out waiting for condition.");
    }
  }


  static boolean logEntriesContains(RaftLog log, SimpleMessage... expectedMessages) {
    return logEntriesContains(log, 0L, Long.MAX_VALUE, expectedMessages);
  }

  static boolean logEntriesNotContains(RaftLog log, SimpleMessage... expectedMessages) {
    return logEntriesNotContains(log, 0L, Long.MAX_VALUE, expectedMessages);
  }

  static boolean logEntriesContains(RaftLog log, long startIndex, long endIndex, SimpleMessage... expectedMessages) {
    int idxEntries = 0;
    int idxExpected = 0;
    final LogEntryHeader[] termIndices = log.getEntries(startIndex, endIndex);
    while (idxEntries < termIndices.length
        && idxExpected < expectedMessages.length) {
      try {
        if (Arrays.equals(expectedMessages[idxExpected].getContent().toByteArray(),
            log.get(termIndices[idxEntries].getIndex()).getStateMachineLogEntry().getLogData().toByteArray())) {
          ++idxExpected;
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      ++idxEntries;
    }
    return idxExpected == expectedMessages.length;
  }

  // Check whether raftlog contains any expected message between startIndex and endIndex.
  // Return true if raftlog does not contain any expected message, returns false otherwise.
  static boolean logEntriesNotContains(RaftLog log, long startIndex, long endIndex, SimpleMessage... expectedMessages) {
    int idxEntries = 0;
    int idxExpected = 0;
    final LogEntryHeader[] termIndices = log.getEntries(startIndex, endIndex);
    while (idxEntries < termIndices.length
        && idxExpected < expectedMessages.length) {
      try {
        if (Arrays.equals(expectedMessages[idxExpected].getContent().toByteArray(),
            log.get(termIndices[idxEntries].getIndex()).getStateMachineLogEntry().getLogData().toByteArray())) {
          return false;
        } else {
          ++idxExpected;
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      ++idxEntries;
    }
    return true;
  }

  static void checkLogEntries(RaftLog log, SimpleMessage[] expectedMessages,
      Predicate<LogEntryProto> predicate) {
    final LogEntryHeader[] termIndices = log.getEntries(0, Long.MAX_VALUE);
    for (int i = 0; i < termIndices.length; i++) {
      for (int j = 0; j < expectedMessages.length; j++) {
        final LogEntryProto e;
        try {
          e = log.get(termIndices[i].getIndex());
          if (Arrays.equals(expectedMessages[j].getContent().toByteArray(),
              e.getStateMachineLogEntry().getLogData().toByteArray())) {
            Assert.assertTrue(predicate.test(e));
          }
        } catch (IOException exception) {
          exception.printStackTrace();
        }
      }
    }
  }

  static void assertLogEntries(MiniRaftCluster cluster, SimpleMessage[] expectedMessages) {
    for(SimpleMessage m : expectedMessages) {
      assertLogEntries(cluster, m);
    }
  }

  static void assertLogEntries(MiniRaftCluster cluster, SimpleMessage expectedMessage) {
    final int size = cluster.getNumServers();
    final long count = cluster.getServerAliveStream()
        .map(RaftServer.Division::getRaftLog)
        .filter(log -> logEntriesContains(log, expectedMessage))
        .count();
    if (2*count <= size) {
      throw new AssertionError("Not in majority: size=" + size
          + " but count=" + count);
    }
  }

  static void assertLogEntries(RaftServer.Division server, long expectedTerm, SimpleMessage... expectedMessages) {
    LOG.info("checking raft log for {}", server.getMemberId());
    final RaftLog log = server.getRaftLog();
    try {
      RaftTestUtil.assertLogEntries(log, expectedTerm, expectedMessages);
    } catch (AssertionError e) {
      LOG.error("Unexpected raft log in {}", server.getMemberId(), e);
      throw e;
    }
  }

  static Iterable<LogEntryProto> getLogEntryProtos(RaftLog log) {
    return CollectionUtils.as(log.getEntries(0, Long.MAX_VALUE), ti -> {
      try {
        return log.get(ti.getIndex());
      } catch (IOException exception) {
        throw new AssertionError("Failed to get log at " + ti, exception);
      }
    });
  }

  static List<LogEntryProto> getStateMachineLogEntries(RaftLog log) {
    final List<LogEntryProto> entries = new ArrayList<>();
    for (LogEntryProto e : getLogEntryProtos(log)) {
      final String s = LogProtoUtils.toLogEntryString(e);
      if (e.hasStateMachineLogEntry()) {
        LOG.info(s + ", " + e.getStateMachineLogEntry().toString().trim().replace("\n", ", "));
        entries.add(e);
      } else if (e.hasConfigurationEntry()) {
        LOG.info("Found {}, ignoring it.", s);
      } else if (e.hasMetadataEntry()) {
        LOG.info("Found {}, ignoring it.", s);
      } else {
        throw new AssertionError("Unexpected LogEntryBodyCase " + e.getLogEntryBodyCase() + " at " + s);
      }
    }
    return entries;
  }

  static void assertLogEntries(RaftLog log, long expectedTerm, SimpleMessage... expectedMessages) {
    final List<LogEntryProto> entries = getStateMachineLogEntries(log);
    try {
      assertLogEntries(entries, expectedTerm, expectedMessages);
    } catch(Exception t) {
      throw new AssertionError("entries: " + entries, t);
    }
  }

  static void assertLogEntries(List<LogEntryProto> entries, long expectedTerm, SimpleMessage... expectedMessages) {
    long logIndex = 0;
    Assert.assertEquals(expectedMessages.length, entries.size());
    for (int i = 0; i < expectedMessages.length; i++) {
      final LogEntryProto e = entries.get(i);
      Assert.assertTrue(e.getTerm() >= expectedTerm);
      if (e.getTerm() > expectedTerm) {
        expectedTerm = e.getTerm();
      }
      Assert.assertTrue(e.getIndex() > logIndex);
      logIndex = e.getIndex();
      Assert.assertArrayEquals(expectedMessages[i].getContent().toByteArray(),
          e.getStateMachineLogEntry().getLogData().toByteArray());
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
      this(messageId, ProtoUtils.toByteString(messageId));
    }

    public SimpleMessage(final String messageId, ByteString bytes) {
      this.messageId = messageId;
      this.bytes = bytes;
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
    private static final ClientId clientId = ClientId.randomId();
    private static final AtomicLong callId = new AtomicLong();

    private final String op;
    private final StateMachineLogEntryProto smLogEntryProto;

    public SimpleOperation(String op) {
      this(op, false);
    }

    public SimpleOperation(String op, boolean hasStateMachineData) {
      this(clientId, callId.incrementAndGet(), op, hasStateMachineData);
    }

    private SimpleOperation(ClientId clientId, long callId, String op, boolean hasStateMachineData) {
      this.op = Objects.requireNonNull(op);
      final ByteString bytes = ProtoUtils.toByteString(op);
      this.smLogEntryProto = LogProtoUtils.toStateMachineLogEntryProto(
          clientId, callId, StateMachineLogEntryProto.Type.WRITE, bytes, hasStateMachineData? bytes: null);
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

    public StateMachineLogEntryProto getLogEntryContent() {
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
      throws Exception {
    return changeLeader(cluster, oldLeader, AssumptionViolatedException::new);
  }

  static RaftPeerId changeLeader(MiniRaftCluster cluster, RaftPeerId oldLeader, Function<String, Exception> constructor)
      throws Exception {
    final String name = JavaUtils.getCallerStackTraceElement().getMethodName() + "-changeLeader";
    cluster.setBlockRequestsFrom(oldLeader.toString(), true);
    try {
      return JavaUtils.attemptRepeatedly(() -> {
        final RaftPeerId newLeader = waitForLeader(cluster).getId();
        if (newLeader.equals(oldLeader)) {
          throw constructor.apply("Failed to change leader: newLeader == oldLeader == " + oldLeader);
        }
        LOG.info("Changed leader from " + oldLeader + " to " + newLeader);
        return newLeader;
      }, 20, BaseTest.HUNDRED_MILLIS, name, LOG);
    } finally {
      cluster.setBlockRequestsFrom(oldLeader.toString(), false);
    }
  }

  static void blockQueueAndSetDelay(Iterable<RaftServer> servers,
      DelayLocalExecutionInjection injection, String leaderId, int delayMs,
      TimeDuration maxTimeout) throws InterruptedException {
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
    StreamSupport.stream(servers.spliterator(), false)
        .filter(s -> !s.getId().toString().equals(leaderId))
        .forEach(s -> {
          if (block) {
            injection.setDelayMs(s.getId().toString(), delayMs);
          } else {
            injection.removeDelay(s.getId().toString());
          }
        });

    Thread.sleep(3 * maxTimeout.toLong(TimeUnit.MILLISECONDS));
  }

  static Thread sendMessageInNewThread(MiniRaftCluster cluster, RaftPeerId leaderId, SimpleMessage... messages) {
    Thread t = new Thread(() -> {
      try (final RaftClient client = cluster.createClient(leaderId)) {
        for (SimpleMessage mssg: messages) {
          client.io().send(mssg);
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    });
    t.start();
    return t;
  }

  static void assertSameLog(RaftLog expected, RaftLog computed) throws Exception {
    Assert.assertEquals(expected.getLastEntryTermIndex(), computed.getLastEntryTermIndex());
    final long lastIndex = expected.getNextIndex() - 1;
    Assert.assertEquals(expected.getLastEntryTermIndex().getIndex(), lastIndex);
    for(long i = 0; i < lastIndex; i++) {
      Assert.assertEquals(expected.get(i), computed.get(i));
    }
  }

  static EnumMap<LogEntryBodyCase, AtomicLong> countEntries(RaftLog raftLog) throws Exception {
    final EnumMap<LogEntryBodyCase, AtomicLong> counts = new EnumMap<>(LogEntryBodyCase.class);
    for(long i = 0; i < raftLog.getNextIndex(); i++) {
      final LogEntryProto e = raftLog.get(i);
      counts.computeIfAbsent(e.getLogEntryBodyCase(), c -> new AtomicLong()).incrementAndGet();
    }
    return counts;
  }

  static LogEntryProto getLastEntry(LogEntryBodyCase targetCase, RaftLog raftLog) throws Exception {
    try(AutoCloseableLock readLock = ((RaftLogBase)raftLog).readLock()) {
      long i = raftLog.getNextIndex() - 1;
      for(; i >= 0; i--) {
        final LogEntryProto entry = raftLog.get(i);
        if (entry.getLogEntryBodyCase() == targetCase) {
          return entry;
        }
      }
    }
    return null;
  }

  static void assertSuccessReply(CompletableFuture<RaftClientReply> reply) throws Exception {
    assertSuccessReply(reply.get(10, TimeUnit.SECONDS));
  }

  static void assertSuccessReply(RaftClientReply reply) {
    Assert.assertNotNull("reply == null", reply);
    Assert.assertTrue("reply is not success: " + reply, reply.isSuccess());
  }
}
