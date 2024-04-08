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
package org.apache.ratis.server.impl;

import org.apache.ratis.BaseTest;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.api.AsyncApi;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.examples.arithmetic.ArithmeticStateMachine;
import org.apache.ratis.examples.arithmetic.expression.DoubleValue;
import org.apache.ratis.examples.arithmetic.expression.Expression;
import org.apache.ratis.examples.arithmetic.expression.Variable;
import org.apache.ratis.grpc.MiniRaftClusterWithGrpc;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.util.CodeInjectionForTesting;
import org.apache.ratis.util.Slf4jUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.event.Level;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.ratis.examples.arithmetic.expression.BinaryExpression.Op.ADD;

public class TestReadAfterWrite
    extends BaseTest
    implements MiniRaftClusterWithGrpc.FactoryGet {

  @Before
  public void setup() {
    Slf4jUtils.setLogLevel(ArithmeticStateMachine.LOG, Level.DEBUG);
    Slf4jUtils.setLogLevel(CodeInjectionForTesting.LOG, Level.DEBUG);
    Slf4jUtils.setLogLevel(RaftServer.Division.LOG, Level.DEBUG);
    RaftServerTestUtil.setStateMachineUpdaterLogLevel(Level.DEBUG);

    final RaftProperties p = getProperties();
    p.setClass(MiniRaftCluster.STATEMACHINE_CLASS_KEY,
        ArithmeticStateMachine.class, StateMachine.class);
    RaftServerConfigKeys.Read.setOption(p, RaftServerConfigKeys.Read.Option.LINEARIZABLE);

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
        LOG.info("Server {} blocks client {}: {}", localId, remoteId, args[0]);
      }
      future.join();
      if (blocked) {
        LOG.info("Server {} unblocks client {}", localId, remoteId);
      }
      return true;
    }
  }

  @Test
  public void testReadAfterWriteSingleServer() throws Exception {
    runWithNewCluster(1, cluster -> {
      try (final RaftClient client = cluster.createClient()) {
        runTestReadAfterWrite(client);
      }
    });
  }

  @Test
  public void testReadAfterWrite() throws Exception {
    runWithNewCluster(3, cluster -> {
      try (final RaftClient client = cluster.createClient()) {
        runTestReadAfterWrite(client);
      }
    });
  }

  void runTestReadAfterWrite(RaftClient client) throws Exception {
    final Variable a = new Variable("a");
    final Expression a_plus_2 = ADD.apply(a, new DoubleValue(2));

    final AsyncApi async = client.async();
    final int initialValue = 10;
    final RaftClientReply assign = async.send(a.assign(new DoubleValue(initialValue))).join();
    Assert.assertTrue(assign.isSuccess());

    final Message query = Expression.Utils.toMessage(a);
    assertReply(async.sendReadOnly(query), initialValue);

    //block state machine
    final BlockingCode blockingCode = new BlockingCode();
    CodeInjectionForTesting.put(RaftServerImpl.APPEND_TRANSACTION, blockingCode);
    final CompletableFuture<RaftClientReply> plus2 = async.send(a.assign(a_plus_2));

    final CompletableFuture<RaftClientReply> readOnlyUnordered = async.sendReadOnlyUnordered(query);
    final CompletableFuture<RaftClientReply> readAfterWrite = async.sendReadAfterWrite(query);

    Thread.sleep(1000);
    // readOnlyUnordered should get 10
    assertReply(readOnlyUnordered, initialValue);

    LOG.info("readAfterWrite.get");
    try {
      // readAfterWrite should time out
      final RaftClientReply reply = readAfterWrite.get(100, TimeUnit.MILLISECONDS);
      final DoubleValue result = (DoubleValue) Expression.Utils.bytes2Expression(
          reply.getMessage().getContent().toByteArray(), 0);
      Assert.fail("result=" + result + ", reply=" + reply);
    } catch (TimeoutException e) {
      LOG.info("Good", e);
    }

    // plus2 should still be blocked.
    Assert.assertFalse(plus2.isDone());
    // readAfterWrite should still be blocked.
    Assert.assertFalse(readAfterWrite.isDone());

    // unblock plus2
    blockingCode.complete();

    // readAfterWrite should get 12.
    assertReply(readAfterWrite, initialValue + 2);
  }

  void assertReply(CompletableFuture<RaftClientReply> future, int expected) {
    LOG.info("assertReply, expected {}", expected);
    final RaftClientReply reply = future.join();
    Assert.assertTrue(reply.isSuccess());
    LOG.info("reply {}", reply);
    final DoubleValue result = (DoubleValue) Expression.Utils.bytes2Expression(
        reply.getMessage().getContent().toByteArray(), 0);
    Assert.assertEquals(expected, (int) (double) result.evaluate(null));
  }
}
