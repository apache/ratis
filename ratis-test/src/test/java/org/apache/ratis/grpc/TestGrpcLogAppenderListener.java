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
package org.apache.ratis.grpc;

import org.apache.ratis.BaseTest;
import org.apache.ratis.RaftTestUtil;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.server.GrpcServicesImpl;
import org.apache.ratis.proto.RaftProtos.AppendEntriesReplyProto;
import org.apache.ratis.proto.RaftProtos.AppendEntriesRequestProto;
import org.apache.ratis.proto.RaftProtos.ReplicationLevel;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.impl.MiniRaftCluster;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.statemachine.impl.SimpleStateMachine4Testing;
import org.apache.ratis.util.CodeInjectionForTesting;
import org.apache.ratis.util.JavaUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class TestGrpcLogAppenderListener extends BaseTest {
  private static RaftProperties newProperties() {
    final RaftProperties properties = new RaftProperties();
    properties.setClass(MiniRaftCluster.STATEMACHINE_CLASS_KEY,
        SimpleStateMachine4Testing.class, StateMachine.class);
    return properties;
  }

  @Test
  @Timeout(value = 60, unit = TimeUnit.SECONDS)
  public void testAppendEntries() throws Exception {
    final Parameters parameters = new Parameters();
    final Set<RaftPeerId> destinations = ConcurrentHashMap.newKeySet();
    final ConcurrentLinkedQueue<Throwable> failures = new ConcurrentLinkedQueue<>();
    final AtomicBoolean injectFailure = new AtomicBoolean(true);
    GrpcConfigKeys.Server.setLogAppenderListenerFactory(parameters, (source, destination) ->
        new GrpcLogAppenderListener() {
          @Override
          public AppendEntries appendEntries() {
            return new AppendEntries() {
              private final Set<Long> pending = ConcurrentHashMap.newKeySet();

              @Override
              public void onRequest(AppendEntriesRequestProto request) {
                if (request.getEntriesList().stream().anyMatch(entry -> entry.hasStateMachineLogEntry())) {
                  pending.add(request.getServerRequest().getCallId());
                }
              }

              @Override
              public void onReply(AppendEntriesReplyProto reply) {
                if (pending.remove(reply.getServerReply().getCallId())) {
                  destinations.add(destination.getId());
                }
              }

              @Override
              public void onFailure(long callId, Throwable error) {
                if (pending.remove(callId)) {
                  failures.add(error);
                  throw new IllegalStateException("Injected listener failure");
                }
              }
            };
          }
        });
    try (MiniRaftClusterWithGrpc cluster = new MiniRaftClusterWithGrpc(
        MiniRaftCluster.generateIds(3, 10), new String[0], newProperties(), parameters)) {
      cluster.start();
      final RaftServer.Division leader = RaftTestUtil.waitForLeader(cluster);
      CodeInjectionForTesting.put(GrpcServicesImpl.GRPC_SEND_SERVER_REQUEST, (local, remote, args) -> {
        if (leader.getId().equals(local) && args[0] instanceof AppendEntriesRequestProto) {
          final AppendEntriesRequestProto request = (AppendEntriesRequestProto) args[0];
          if (request.getEntriesList().stream().anyMatch(entry -> entry.hasStateMachineLogEntry())
              && injectFailure.compareAndSet(true, false)) {
            throw new IllegalStateException("Injected send failure");
          }
        }
        return false;
      });
      try (RaftClient client = cluster.createClient(leader.getId())) {
        final RaftClientReply write = client.io().send(new RaftTestUtil.SimpleMessage("listener"));
        Assertions.assertTrue(write.isSuccess());
        Assertions.assertTrue(client.io().watch(write.getLogIndex(), ReplicationLevel.ALL_COMMITTED).isSuccess());
      } finally {
        CodeInjectionForTesting.remove(GrpcServicesImpl.GRPC_SEND_SERVER_REQUEST);
      }
      final Set<RaftPeerId> followers = cluster.getFollowers().stream()
          .map(RaftServer.Division::getId).collect(Collectors.toSet());
      JavaUtils.attempt(() -> Assertions.assertEquals(followers, destinations),
          10, HUNDRED_MILLIS, "append replies", LOG);
      Assertions.assertEquals(1, failures.size());
      Assertions.assertInstanceOf(IllegalStateException.class, failures.element());
    }
  }

}
