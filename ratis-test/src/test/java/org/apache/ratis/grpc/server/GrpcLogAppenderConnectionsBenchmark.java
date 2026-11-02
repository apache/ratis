/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ratis.grpc.server;

import org.apache.ratis.BaseTest;
import org.apache.ratis.RaftTestUtil;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.grpc.GrpcTlsConfig;
import org.apache.ratis.grpc.MiniRaftClusterWithGrpc;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.security.SecurityTestUtils;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.impl.MiniRaftCluster;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.io.netty.util.ResourceLeakDetector;
import org.apache.ratis.util.SizeInBytes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * RATIS-2787 benchmark: replication throughput between servers that share many groups, against the number of
 * appendEntries connections per peer (raft.grpc.server.log-appender.connections). Not run by the build (the name
 * doesn't match the surefire patterns); run with
 * <pre>
 *   mvn -pl ratis-test -am test -Dtest=GrpcLogAppenderConnectionsBenchmark -Dsurefire.failIfNoSpecifiedTests=false \
 *     -Djacoco.skip=true -DextraJavaTestArgs=-Xmx12g [-Dbench.connections=1,4 -Dbench.groups=12 \
 *     -Dbench.messageKB=1024 -Dbench.messagesPerGroup=50 -Dbench.warmupPerGroup=20 -Dbench.window=8 \
 *     -Dbench.rounds=1 -Dbench.tls=true -Dbench.workers=8 -Dbench.leaderOnOne=true -Dbench.flowControlWindowKB=0]
 * </pre>
 * JaCoCo (on by default in ratis-test) roughly halves the throughput, so skip it. Netty leak detection is disabled.
 * <p>
 * Each arm starts a new 3-server gRPC cluster (mutual TLS by default, in-memory Raft log, so no disk), adds the groups
 * on all three servers and, by default, moves every group's leader to the first server, so that all replication goes
 * over its connections to the two others: that is the case where a single connection per peer is the limit. By
 * default each server also gets its own event-loop groups (bench.workers threads each), as if in its own process;
 * with bench.workers=0 all servers share gRPC's default event loops, as many as cores. It writes a warm-up, then the
 * timed messages, to every group at once, with a window of outstanding async requests per group, until all are
 * committed. All three servers run in this JVM and keep the log in memory, so the heap needs about
 * 3 x groups x (messagesPerGroup + warmupPerGroup) x messageKB, in a fresh JVM per arm.
 * <p>
 * It reports the replication rate, the hottest event-loop threads during the timed writes (CPU time / wall time;
 * cwI is a client worker, i.e. outgoing connections, swI a server worker), how many were over half busy, and the
 * cores used by all event loops and by the whole JVM.
 */
public class GrpcLogAppenderConnectionsBenchmark extends BaseTest {
  private static final int GROUPS = Integer.getInteger("bench.groups", 12);
  private static final int MESSAGE_KB = Integer.getInteger("bench.messageKB", 1024);
  private static final int MESSAGES = Integer.getInteger("bench.messagesPerGroup", 50);
  private static final int WARMUP = Integer.getInteger("bench.warmupPerGroup", 20);
  private static final int WORKERS = Integer.getInteger("bench.workers", 8);
  // 0 keeps Ratis's default (raft.grpc.flow.control.window, 1MB); Ozone sets 5MB for its 4MB chunks
  private static final int FLOW_CONTROL_WINDOW_KB = Integer.getInteger("bench.flowControlWindowKB", 0);
  private static final boolean LEADER_ON_ONE = Boolean.parseBoolean(System.getProperty("bench.leaderOnOne", "true"));
  private static final int WINDOW = Integer.getInteger("bench.window", 8);
  private static final int ROUNDS = Integer.getInteger("bench.rounds", 1);
  private static final String CONNECTIONS = System.getProperty("bench.connections", "1,4");
  private static final boolean TLS = Boolean.parseBoolean(System.getProperty("bench.tls", "true"));

  static {
    // Netty's default (SIMPLE) leak detection samples buffers and records a stack trace for each; not what is measured
    ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.DISABLED);
  }

  @Test
  @Timeout(value = 60, unit = TimeUnit.MINUTES) // BaseTest's 100 s is for unit tests
  public void benchmark() throws Exception {
    final String[] ks = CONNECTIONS.split(",");
    System.out.printf("%nRATIS-2787: %d groups x %d messages x %d KB (warm-up %d), window %d, TLS %s, workers %d,"
        + " leader on one server %s, flow control window %s%n", GROUPS, MESSAGES, MESSAGE_KB, WARMUP, WINDOW, TLS,
        WORKERS, LEADER_ON_ONE, FLOW_CONTROL_WINDOW_KB > 0 ? FLOW_CONTROL_WINDOW_KB + "KB" : "default");
    System.out.printf("%-6s %-3s %10s %12s %40s %10s %10s %10s%n",
        "round", "K", "seconds", "MB/s", "hottest loops (%CPU)", "loops>50%", "loop cores", "JVM cores");
    for (int round = 1; round <= ROUNDS; round++) {
      for (int i = 0; i < ks.length; i++) {
        // rotate the order, so no K always runs first
        final int k = Integer.parseInt(ks[(i + round) % ks.length].trim());
        runArm(round, k);
        System.gc();
      }
    }
  }

  private static Parameters newTlsParameters() throws Exception {
    final GrpcTlsConfig server = new GrpcTlsConfig(
        SecurityTestUtils.getKeyManager(SecurityTestUtils::getServerKeyStore),
        SecurityTestUtils.getTrustManager(SecurityTestUtils::getTrustStore), true);
    final GrpcTlsConfig client = new GrpcTlsConfig(
        SecurityTestUtils.getKeyManager(SecurityTestUtils::getClientKeyStore),
        SecurityTestUtils.getTrustManager(SecurityTestUtils::getTrustStore), true);
    final Parameters parameters = new Parameters();
    GrpcConfigKeys.Server.setTlsConf(parameters, server);
    GrpcConfigKeys.Admin.setTlsConf(parameters, client);
    GrpcConfigKeys.Client.setTlsConf(parameters, client);
    return parameters;
  }

  private void runArm(int round, int connections) throws Exception {
    final RaftProperties properties = new RaftProperties();
    RaftServerConfigKeys.Log.setUseMemory(properties, true);
    GrpcConfigKeys.Server.setLogAppenderConnections(properties, connections);
    if (FLOW_CONTROL_WINDOW_KB > 0) {
      GrpcConfigKeys.setFlowControlWindow(properties, SizeInBytes.valueOf(FLOW_CONTROL_WINDOW_KB + "KB"));
    }
    if (WORKERS > 0) {
      // each server gets its own event loops, as in separate processes, instead of sharing gRPC's default ones
      GrpcConfigKeys.Server.setWorkerGroupSize(properties, WORKERS);
      GrpcConfigKeys.Client.setWorkerGroupSize(properties, WORKERS);
    }
    final MiniRaftClusterWithGrpc cluster = new MiniRaftClusterWithGrpc(MiniRaftCluster.generateIds(3, 0),
        new String[0], properties, TLS ? newTlsParameters() : null);
    try {
      final long setupStart = System.nanoTime();
      cluster.start();
      RaftTestUtil.waitForLeader(cluster);
      final List<RaftPeer> peers = new ArrayList<>(cluster.getGroup().getPeers());
      final RaftPeerId leaderId = peers.get(0).getId();

      // the groups, each on all three servers, so they share the connections between each pair of servers
      final List<RaftGroupId> groupIds = new ArrayList<>();
      final List<RaftClient> clients = new ArrayList<>();
      try (RaftClient admin = cluster.createClient(cluster.getGroup())) {
        for (int g = 0; g < GROUPS; g++) {
          final RaftGroup group = RaftGroup.valueOf(RaftGroupId.randomId(), peers);
          for (RaftPeer peer : peers) {
            Assertions.assertTrue(admin.getGroupManagementApi(peer.getId()).add(group).isSuccess());
          }
          groupIds.add(group.getGroupId());
          clients.add(cluster.createClient(group));
        }
      }
      // wait for a leader in every group
      for (RaftClient client : clients) {
        Assertions.assertTrue(client.io().send(Message.valueOf("first")).isSuccess());
      }
      if (LEADER_ON_ONE) {
        // all groups led by one server, so all replication goes over its connections to the two others
        for (int g = 0; g < GROUPS; g++) {
          if (!cluster.getDivision(leaderId, groupIds.get(g)).getInfo().isLeader()) {
            Assertions.assertTrue(clients.get(g).admin().transferLeadership(leaderId, 30_000).isSuccess());
          }
        }
        for (int g = 0; g < GROUPS; g++) {
          Assertions.assertTrue(cluster.getDivision(leaderId, groupIds.get(g)).getInfo().isLeader());
        }
      }

      final byte[] bytes = new byte[MESSAGE_KB << 10];
      new Random(0).nextBytes(bytes);
      final Message message = Message.valueOf(ByteString.copyFrom(bytes));
      // warm-up, so the JIT is mostly done before the timed writes
      writeAll(clients, message, WARMUP);
      System.out.printf("  K=%d: cluster and %d groups ready and warmed up in %.1f s%n", connections, GROUPS,
          (System.nanoTime() - setupStart) / 1e9);

      final Map<Long, Long> cpuBefore = loopCpuTimes();
      final long processCpuBefore = processCpuTime();
      final long startMillis = System.currentTimeMillis();
      final long start = System.nanoTime();
      writeAll(clients, message, MESSAGES);
      final long elapsedNanos = System.nanoTime() - start;
      final double jvmCores = (double) (processCpuTime() - processCpuBefore) / elapsedNanos;
      final Map<Long, Long> cpuAfter = loopCpuTimes();

      final List<Map.Entry<String, Double>> busy = new ArrayList<>();
      double loopCores = 0;
      final ThreadMXBean threads = ManagementFactory.getThreadMXBean();
      for (Map.Entry<Long, Long> e : cpuAfter.entrySet()) {
        final long cpu = e.getValue() - cpuBefore.getOrDefault(e.getKey(), 0L);
        final ThreadInfo info = threads.getThreadInfo(e.getKey());
        final String name = info == null ? "?" : shortName(info.getThreadName());
        busy.add(new AbstractMap.SimpleImmutableEntry<>(name, 100.0 * cpu / elapsedNanos));
        loopCores += (double) cpu / elapsedNanos;
      }
      busy.sort((a, b) -> Double.compare(b.getValue(), a.getValue()));
      final StringBuilder hottest = new StringBuilder();
      for (int i = 0; i < Math.min(3, busy.size()); i++) {
        hottest.append(i == 0 ? "" : " ").append(busy.get(i).getKey()).append('=')
            .append(Math.round(busy.get(i).getValue()));
      }
      final long overHalf = busy.stream().filter(b -> b.getValue() > 50).count();
      final double seconds = elapsedNanos / 1e9;
      final double mbPerSecond = (double) GROUPS * MESSAGES * MESSAGE_KB / 1024 / seconds;
      System.out.printf("%-6d %-3d %10.2f %12.1f %40s %10d %10.1f %10.1f%n", round, connections, seconds,
          mbPerSecond, hottest, overHalf, loopCores, jvmCores);
      System.out.printf("  K=%d: writes from %d to %d ms (epoch)%n", connections, startMillis,
          System.currentTimeMillis());
      for (RaftClient client : clients) {
        client.close();
      }
    } finally {
      cluster.shutdown();
    }
  }

  /** Writes the message count times to every group at once, with a window of outstanding requests per group. */
  private static void writeAll(List<RaftClient> clients, Message message, int count) throws Exception {
    final AtomicInteger failures = new AtomicInteger();
    final List<CompletableFuture<RaftClientReply>> replies = new ArrayList<>();
    final List<Semaphore> windows = new ArrayList<>();
    for (int g = 0; g < clients.size(); g++) {
      windows.add(new Semaphore(WINDOW));
    }
    for (int m = 0; m < count; m++) {
      for (int g = 0; g < clients.size(); g++) {
        final Semaphore window = windows.get(g);
        window.acquire();
        replies.add(clients.get(g).async().send(message).whenComplete((reply, e) -> {
          window.release();
          if (e != null || !reply.isSuccess()) {
            failures.incrementAndGet();
          }
        }));
      }
    }
    CompletableFuture.allOf(replies.toArray(new CompletableFuture[0])).get(30, TimeUnit.MINUTES);
    Assertions.assertEquals(0, failures.get(), "failed writes");
  }

  /**
   * E.g. ...-client-workers--thread3 to cw3, ...-server-workers--thread3 to sw3, grpc-nio-worker-ELG-3-13 to elg13.
   * The groups are named before the server knows its id, so the names don't tell the servers apart.
   */
  private static String shortName(String name) {
    final String id = name.replaceAll("^.*?(\\d+)$", "$1");
    if (name.contains("-client-workers")) {
      return "cw" + id;
    } else if (name.contains("-server-workers")) {
      return "sw" + id;
    }
    return "elg" + id;
  }

  private static long processCpuTime() {
    return ((com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean())
        .getProcessCpuTime();
  }

  /** CPU time of the Netty event-loop threads (gRPC's and Ratis's), by thread id. */
  private static Map<Long, Long> loopCpuTimes() {
    final ThreadMXBean threads = ManagementFactory.getThreadMXBean();
    final Map<Long, Long> times = new HashMap<>();
    for (ThreadInfo info : threads.dumpAllThreads(false, false)) {
      final String name = info.getThreadName();
      if (name.contains("grpc-default-worker") || name.contains("ELG") || name.contains("nioEventLoop")
          || name.contains("epoll") || name.contains("-client-workers") || name.contains("-server-workers")) {
        final long cpu = threads.getThreadCpuTime(info.getThreadId());
        if (cpu > 0) {
          times.put(info.getThreadId(), cpu);
        }
      }
    }
    return times;
  }
}
