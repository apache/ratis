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
import org.apache.ratis.RaftTestUtil.SimpleMessage;
import org.apache.ratis.client.RaftClientConfigKeys;
import org.apache.ratis.protocol.AlreadyClosedException;
import org.apache.ratis.protocol.GroupMismatchException;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.statemachine.SimpleStateMachine4Testing;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.util.TimeDuration;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public abstract class RaftAsyncExceptionTests<CLUSTER extends MiniRaftCluster>
    extends BaseTest
    implements MiniRaftCluster.Factory.Get<CLUSTER> {

  {
    getProperties().setClass(MiniRaftCluster.STATEMACHINE_CLASS_KEY,
        SimpleStateMachine4Testing.class, StateMachine.class);
  }

  @Test
  public void testGroupMismatchException() throws Exception {
    runWithNewCluster(1, this::runTestGroupMismatchException);
  }

  private void runTestGroupMismatchException(CLUSTER cluster) throws Exception {
    // send a message to make sure the cluster is working
    try(RaftClient client = cluster.createClient()) {
      final RaftClientReply reply = client.sendAsync(new SimpleMessage("first")).get();
      Assert.assertTrue(reply.isSuccess());
    }

    // create another group
    final RaftGroup clusterGroup = cluster.getGroup();
    final RaftGroup anotherGroup = RaftGroup.valueOf(RaftGroupId.randomId(), clusterGroup.getPeers());
    Assert.assertNotEquals(clusterGroup.getGroupId(), anotherGroup.getGroupId());

    // create another client using another group
    final SimpleMessage[] messages = SimpleMessage.create(5);
    try(RaftClient client = cluster.createClient(anotherGroup)) {
      // send a few messages
      final List<CompletableFuture<RaftClientReply>> futures = new ArrayList<>();
      for(SimpleMessage m : messages) {
        futures.add(client.sendAsync(m));
      }
      Assert.assertEquals(messages.length, futures.size());

      // check replies
      final Iterator<CompletableFuture<RaftClientReply>> i = futures.iterator();
      testFailureCase("First reply is GroupMismatchException",
          () -> i.next().get(),
          ExecutionException.class, GroupMismatchException.class);
      for(; i.hasNext(); ) {
        testFailureCase("Following replies are AlreadyClosedException caused by GroupMismatchException",
            () -> i.next().get(),
            ExecutionException.class, AlreadyClosedException.class, GroupMismatchException.class);
      }
    }
  }

  @Test
  public void testTimeoutException() throws Exception {
    runWithNewCluster(3, this::runTestTimeoutException);
  }

  private void runTestTimeoutException(CLUSTER cluster) throws Exception {
    // send a message to make sure the cluster is working
    try(RaftClient client = cluster.createClient()) {
      client.send(new SimpleMessage("m0"));

      RaftClientConfigKeys.Rpc.setRequestTimeout(properties.get(),
          TimeDuration.valueOf(3, TimeUnit.SECONDS));
      // Block StartTransaction
      cluster.getServers().stream()
          .map(cluster::getRaftServerImpl)
          .map(SimpleStateMachine4Testing::get)
          .forEach(SimpleStateMachine4Testing::blockStartTransaction);
      final CompletableFuture<RaftClientReply> replyFuture = client.sendAsync(new SimpleMessage("m1"));
      Thread.sleep(10000);
      // Unblock StartTransaction
      cluster.getServers().stream()
          .map(cluster::getRaftServerImpl)
          .map(SimpleStateMachine4Testing::get)
          .forEach(SimpleStateMachine4Testing::unblockStartTransaction);
      // The request should succeed after start transaction is unblocked
      Assert.assertTrue(replyFuture.get().isSuccess());
    }
  }
}
