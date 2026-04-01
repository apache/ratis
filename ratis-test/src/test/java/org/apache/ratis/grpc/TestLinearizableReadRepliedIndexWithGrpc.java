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

import org.apache.ratis.RaftTestUtil;
import org.apache.ratis.ReadOnlyRequestTests.CounterStateMachine;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys.Read.ReadIndex.Type;
import org.apache.ratis.server.impl.MiniRaftCluster;
import org.apache.ratis.server.impl.ReplyFlusher;
import org.apache.ratis.util.CodeInjectionForTesting;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.apache.ratis.ReadOnlyRequestTests.INCREMENT;
import static org.apache.ratis.ReadOnlyRequestTests.QUERY;
import static org.apache.ratis.ReadOnlyRequestTests.assertReplyExact;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestLinearizableReadRepliedIndexWithGrpc
  extends TestLinearizableReadWithGrpc {

  @Override
  public Type readIndexType() {
    return Type.REPLIED_INDEX;
  }

  @Test
  @Override
  public void testFollowerLinearizableReadParallel() throws Exception {
    runWithNewCluster(TestLinearizableReadRepliedIndexWithGrpc::runTestFollowerReadOnlyParallelRepliedIndex);
  }

  static <C extends MiniRaftCluster> void runTestFollowerReadOnlyParallelRepliedIndex(C cluster)
      throws Exception {
    final RaftServer.Division leader = RaftTestUtil.waitForLeader(cluster);
    final CounterStateMachine leaderStateMachine = (CounterStateMachine)leader.getStateMachine();

    final List<RaftServer.Division> followers = cluster.getFollowers();
    Assertions.assertEquals(2, followers.size());
    final RaftPeerId f0 = followers.get(0).getId();
    final RaftPeerId f1 = followers.get(1).getId();

    final BlockingCode blockingReplyFlusher = new BlockingCode();

    try (RaftClient leaderClient = cluster.createClient(leader.getId());
         RaftClient f0Client = cluster.createClient(f0);
         RaftClient f1Client = cluster.createClient(f1)) {
      // Warm up the clients first
      assertReplyExact(0, leaderClient.async().sendReadOnly(QUERY).get());
      assertReplyExact(0, f0Client.async().sendReadOnly(QUERY, f0).get());
      assertReplyExact(0, f1Client.async().sendReadOnly(QUERY, f1).get());

      CodeInjectionForTesting.put(ReplyFlusher.FLUSH, blockingReplyFlusher);

      final int n = 10;
      final List<Reply> writeReplies = new ArrayList<>(n);
      final List<Reply> f0Replies = new ArrayList<>(n);
      final List<Reply> f1Replies = new ArrayList<>(n);
      for (int i = 0; i < n; i++) {
        final int count = i + 1;
        writeReplies.add(new Reply(count, leaderClient.async().send(INCREMENT)));

        // Read reply returns immediately, but they all should return 0 since the repliedIndex has not been updated
        // and write operations should not been applied by the followers
        f0Replies.add(new Reply(0, f0Client.async().sendReadOnly(QUERY, f0)));
        f1Replies.add(new Reply(0, f1Client.async().sendReadOnly(QUERY, f1)));

        // sleep in order to make sure
        // (1) the count is incremented, and
        // (2) the reads will wait for the repliedIndex.
        Thread.sleep(100);
        assertEquals(count, leaderStateMachine.getCount());
      }

      for (int i = 0; i < n; i++) {
        // Write reply should not yet complete since ReplyFlusher remains blocked.
        assertFalse(writeReplies.get(i).isDone(), "Received unexpected Write reply " + writeReplies.get(i));

        // Follower reads should be immediately served, but the read value should return the value before the
        // replyFlusher is blocked
        assertTrue(f0Replies.get(i).isDone(), "Follower read should return immediately");
        f0Replies.get(i).assertExact();
        assertTrue(f1Replies.get(i).isDone(), "Follower read should return immediately");
        f1Replies.get(i).assertExact();
      }

      // unblock ReplyFlusher
      blockingReplyFlusher.complete();
      assertReplyExact(n, f0Client.io().sendReadOnly(QUERY, f0));
      assertReplyExact(n, f1Client.io().sendReadOnly(QUERY, f0));

      for (int i = 0; i < n; i++) {
        //write reply should get the exact count at the write time
        writeReplies.get(i).assertExact();
      }
    }
  }

  static class BlockingCode implements CodeInjectionForTesting.Code {
    private final CompletableFuture<Void> future = new CompletableFuture<>();

    void complete() {
      future.complete(null);
    }

    @Override
    public boolean execute(Object localId, Object remoteId, Object... args) {
      final boolean blocked = !future.isDone();
      if (blocked) {
        LOG.info("{}: ReplyFlusher is blocked", localId, new Throwable());
      }
      future.join();
      if (blocked) {
        LOG.info("{}: ReplyFlusher is unblocked", localId);
      }
      return true;
    }
  }


}
