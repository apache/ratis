package org.apache.ratis.examples;

import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.netty.NettyConfigKeys;
import org.apache.ratis.netty.NettyFactory;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.server.RaftConfiguration;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.util.ConcurrentUtils;
import org.apache.ratis.util.NetUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static org.apache.ratis.server.RaftConfiguration.APPLY_OLD_NEW_CONF;
import static org.apache.ratis.server.RaftConfiguration.SHUTDOWN_NEW_PEER;
import static org.apache.ratis.util.Preconditions.assertTrue;
import static org.junit.Assert.assertEquals;

public class TestMembership {

  static final RaftGroupId GROUP_ID = RaftGroupId.randomId();
  static final String TEST_BASE = "/tmp/raft-test/" + UUID.randomUUID();

  static String LOCAL_ADDR;
  static ExecutorService executorService;

  @BeforeClass
  public static void before() throws UnknownHostException {
    File file = new File(TEST_BASE);
    if (file.exists()) {
      assertTrue(file.delete());
    }
    assertTrue(file.mkdirs());

    LOCAL_ADDR = InetAddress.getLocalHost().getCanonicalHostName();
    executorService = ConcurrentUtils.newSingleThreadExecutor("single executor");
  }

  @AfterClass
  public static void after() {
    delete(new File(TEST_BASE));
    executorService.shutdown();
  }

  @Test
  public void testNoLeaderWhenMembershipChange() throws Exception {
    // Control test procedure.
    RaftConfiguration.LatchMap.put(APPLY_OLD_NEW_CONF, new CountDownLatch(1));
    RaftConfiguration.LatchMap.put(SHUTDOWN_NEW_PEER, new CountDownLatch(1));

    RaftServer[] servers = new RaftServer[5];
    RaftPeer[] peers = new RaftPeer[] {
        RaftPeer.newBuilder()
            .setId("node-0")
            .setAddress(LOCAL_ADDR + ":21011")
            .setPriority(0)
            .build(),
        RaftPeer.newBuilder()
            .setId("node-1")
            .setAddress(LOCAL_ADDR + ":21012")
            .setPriority(0)
            .build(),
        RaftPeer.newBuilder()
            .setId("node-2")
            .setAddress(LOCAL_ADDR + ":21013")
            .setPriority(0)
            .build(),
        RaftPeer.newBuilder()
            .setId("node-3")
            .setAddress(LOCAL_ADDR + ":21014")
            .setPriority(0)
            .build(),
        RaftPeer.newBuilder()
            .setId("node-4")
            .setAddress(LOCAL_ADDR + ":21015")
            .setPriority(0)
            .build()
    };

    // 1. Start raft servers.
    // Servers {node-0, node-1, node-2} form the initial raft cluster.
    // Servers {node-3, node-4} are new peers. They start with empty group.
    servers[0] = startServer(RaftGroup.valueOf(GROUP_ID, peers[0], peers[1], peers[2]), peers[0], RaftStorage.StartupOption.FORMAT);
    servers[1] = startServer(RaftGroup.valueOf(GROUP_ID, peers[0], peers[1], peers[2]), peers[1], RaftStorage.StartupOption.FORMAT);
    servers[2] = startServer(RaftGroup.valueOf(GROUP_ID, peers[0], peers[1], peers[2]), peers[2], RaftStorage.StartupOption.FORMAT);
    while (findLeader(servers) == -1) {
      // Wait for leader.
    }
    servers[3] = startServer(RaftGroup.valueOf(GROUP_ID), peers[3], RaftStorage.StartupOption.FORMAT);
    servers[4] = startServer(RaftGroup.valueOf(GROUP_ID), peers[4], RaftStorage.StartupOption.FORMAT);

    // 2. Trigger setConfiguration.
    RaftClient client = createClient(peers[0], peers[1], peers[2]);
    Future<RaftClientReply> future = executorService.submit(
        () -> client.admin().setConfiguration(Arrays.copyOfRange(peers, 2, 5)));

    // 3. Wait leader calling LeaderStateImpl#applyOldNewConf.
    // {node-0, node-1, node-2} conf will be updated to transitional/old_new conf. But {node-3, node-4}
    // conf will remain empty because SHUTDOWN_NEW_PEER latch.
    RaftConfiguration.LatchMap.get(APPLY_OLD_NEW_CONF).await();

    // 4. Shutdown node-3 and node-4, then recover SHUTDOWN_NEW_PEER latch. node-3 and node-4 can
    // handle transitional conf now.
    System.out.println("Shutdown 3, 4 before they appendEntries old_new_conf.");
    shutdownServer(servers, 3);
    shutdownServer(servers, 4);
    RaftConfiguration.LatchMap.get(SHUTDOWN_NEW_PEER).countDown();

    // 5. Wait 1s for leader to notice it is no leader and trigger election. Because we have stopped
    // node-3 and node-4, and leader's conf is old_new_conf.
    Thread.sleep(1000L);
    int leaderIndex = findLeader(servers);
    assertEquals(-1 ,leaderIndex);

    // 6. Start {node-3, node-4}. Now every peer is running without any latch.
    servers[3] = startServer(RaftGroup.valueOf(GROUP_ID), peers[3], RaftStorage.StartupOption.RECOVER);
    servers[4] = startServer(RaftGroup.valueOf(GROUP_ID), peers[4], RaftStorage.StartupOption.RECOVER);

    // 7. We can't elect a new leader anymore. Because {node-3, node-4} have an empty conf. They
    // won't vote for any peer in {node-0, node-1, node-2}.
    //
    RaftClientReply reply = future.get();
    if (!reply.isSuccess()) {
      throw reply.getException();
    }
  }

  private int findLeader(RaftServer[] servers) throws IOException {
    for (int i = 0; i < servers.length; i++) {
      if (servers[i] != null && servers[i].getDivision(GROUP_ID).getInfo().isLeader()) {
        return i;
      }
    }
    return -1;
  }

  private RaftPeer shutdownServer(RaftServer[] servers, int index) throws IOException {
    RaftPeer peer = servers[index].getPeer();
    servers[index].close();
    return peer;
  }

  private RaftClient createClient(RaftPeer... peers) {
    RaftProperties properties = new RaftProperties();
    RaftClient.Builder builder = RaftClient.newBuilder().setProperties(properties);

    builder.setRaftGroup(RaftGroup.valueOf(GROUP_ID, peers));

    builder.setClientRpc(new NettyFactory(new Parameters()).newRaftClientRpc(ClientId.randomId(), properties));

    return builder.build();
  }

  private RaftServer startServer(RaftGroup group, RaftPeer currentPeer,
      RaftStorage.StartupOption option) throws IOException, InterruptedException {
    final RaftProperties properties = new RaftProperties();

    final int port = NetUtils.createSocketAddr(currentPeer.getAddress()).getPort();
    RaftConfigKeys.Rpc.setType(properties, SupportedRpcType.NETTY);
    NettyConfigKeys.Server.setPort(properties, port);
    RaftServerConfigKeys.setStorageDir(properties, Collections.singletonList(
        new File(TEST_BASE, currentPeer.getId().toString())));

    StateMachine stateMachine = new BaseStateMachine();
    RaftServer server = RaftServer.newBuilder()
        .setGroup(group)
        .setProperties(properties)
        .setServerId(currentPeer.getId())
        .setStateMachine(stateMachine)
        .setOption(option)
        .build();
    server.start();
    do {
      Thread.sleep(1000L);
    } while (!server.getDivision(GROUP_ID).getInfo().isAlive());
    return server;
  }

  private static void delete(File file) {
    if (file.isFile()) {
      assertTrue(file.delete());
    } else {
      for (String c : file.list()) {
        delete(new File(file, c));
      }
      assertTrue(file.delete());
    }
  }
}
