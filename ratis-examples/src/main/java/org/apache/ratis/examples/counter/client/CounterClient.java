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
package org.apache.ratis.examples.counter.client;

import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.examples.common.Constants;
import org.apache.ratis.examples.counter.CounterCommand;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.util.ConcurrentUtils;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.TimeDuration;
import org.apache.ratis.util.Timestamp;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Counter client application, this application sends specific number of
 * INCREMENT command to the Counter cluster and at the end sends a GET command
 * and print the result
 * <p>
 * Parameter to this application indicate the number of INCREMENT command, if no
 * parameter found, application use default value which is 10
 */
public final class CounterClient implements Closeable {
  enum Mode {
    DRY_RUN, IO, ASYNC;

    static Mode parse(String s) {
      for(Mode m : values()) {
        if (m.name().equalsIgnoreCase(s)) {
          return m;
        }
      }
      return DRY_RUN;
    }
  }

  //build the client
  static RaftClient newClient() {
    return RaftClient.newBuilder()
        .setProperties(new RaftProperties())
        .setRaftGroup(Constants.RAFT_GROUP)
        .build();
  }

  private final RaftClient client = newClient();

  @Override
  public void close() throws IOException {
    client.close();
  }

  static RaftClientReply assertReply(RaftClientReply reply) {
    Preconditions.assertTrue(reply.isSuccess(), "Failed");
    return reply;
  }

  static void send(int increment, Mode mode, RaftClient client) throws Exception {
    final List<CompletableFuture<RaftClientReply>> futures = new ArrayList<>(increment);

    //send INCREMENT command(s)
    if (mode == Mode.IO) {
      // use BlockingApi
      for (int i = 0; i < increment; i++) {
        final RaftClientReply reply = client.io().send(CounterCommand.INCREMENT.getMessage());
        futures.add(CompletableFuture.completedFuture(reply));
      }
    } else if (mode == Mode.ASYNC) {
      // use AsyncApi
      for (int i = 0; i < increment; i++) {
        futures.add(client.async().send(CounterCommand.INCREMENT.getMessage()).thenApply(CounterClient::assertReply));
      }

      //wait for the futures
      JavaUtils.allOf(futures).get();
    }
  }

  private void send(int i, int increment, Mode mode) {
    System.out.println("Start client " + i);
    try (RaftClient c = newClient()) {
      send(increment, mode, c);
    } catch (Exception e) {
      throw new CompletionException(e);
    }
  }

  private RaftClientReply readCounter(RaftPeerId server) {
    try {
      return client.io().sendReadOnly(CounterCommand.GET.getMessage(), server);
    } catch (IOException e) {
      System.err.println("Failed read-only request");
      return RaftClientReply.newBuilder().setSuccess(false).build();
    }
  }

  private void readComplete(RaftClientReply reply, Throwable t, RaftPeerId server, Timestamp readStarted) {
    if (t != null) {
      System.err.println("Failed to get counter from " + server + ": " + t);
      return;
    } else if (reply == null || !reply.isSuccess()) {
      System.err.println("Failed to get counter from " + server + " with reply = " + reply);
      return;
    }

    // reply is success
    final TimeDuration readElapsed = readStarted.elapsedTime();
    final int countValue = reply.getMessage().getContent().asReadOnlyByteBuffer().getInt();
    System.out.printf("read from %s and get counter value: %d, time elapsed: %s.%n",
        server, countValue, readElapsed.toString(TimeUnit.SECONDS, 3));
  }

  private void run(int increment, Mode mode, int numClients, ExecutorService executor) throws Exception {
    Preconditions.assertTrue(increment > 0, "increment <= 0");
    Preconditions.assertTrue(numClients > 0, "numClients <= 0");
    System.out.printf("Sending %d %s command(s) in %s mode with %d client(s) ...%n",
        increment, CounterCommand.INCREMENT, mode, numClients);
    final Timestamp sendStarted = Timestamp.currentTime();
    ConcurrentUtils.parallelForEachAsync(numClients, i -> send(i, increment, mode), executor).get();
    final TimeDuration sendElapsed = sendStarted.elapsedTime();
    final long numOp = numClients * (long)increment;
    System.out.println("******************************************************");
    System.out.printf("*   Completed sending %d command(s) in %s%n",
        numOp, sendElapsed.toString(TimeUnit.SECONDS, 3));
    System.out.printf("*   The rate is %01.2f op/s%n",
        numOp * 1000.0 / sendElapsed.toLong(TimeUnit.MILLISECONDS));
    System.out.println("******************************************************");

    if (mode == Mode.DRY_RUN) {
      return;
    }

    //send a GET command and print the reply
    final RaftClientReply reply = client.io().sendReadOnly(CounterCommand.GET.getMessage());
    final int count = reply.getMessage().getContent().asReadOnlyByteBuffer().getInt();
    System.out.println("Current counter value: " + count);

    // using Linearizable Read
    final Timestamp readStarted = Timestamp.currentTime();
    final List<CompletableFuture<RaftClientReply>> futures = Constants.PEERS.stream()
        .map(RaftPeer::getId)
        .map(server -> CompletableFuture.supplyAsync(() -> readCounter(server), executor)
        .whenComplete((r, t) -> readComplete(r, t, server, readStarted)))
        .collect(Collectors.toList());

    for (Future<RaftClientReply> f : futures) {
      f.get();
    }
  }

  public static void main(String[] args) {
    try(CounterClient client = new CounterClient()) {
      //the number of INCREMENT commands, default is 10
      final int increment = args.length > 0 ? Integer.parseInt(args[0]) : 10;
      final Mode mode = Mode.parse(args.length > 1? args[1] : null);
      final int numClients = args.length > 2 ? Integer.parseInt(args[2]) : 1;

      final ExecutorService executor = Executors.newFixedThreadPool(Math.max(numClients, Constants.PEERS.size()));
      try {
        client.run(increment, mode, numClients, executor);
      } finally {
        executor.shutdown();
      }
    } catch (Throwable e) {
      e.printStackTrace();
      System.err.println();
      System.err.println("args = " + Arrays.toString(args));
      System.err.println();
      System.err.printf("Usage: java %s [INCREMENT] [DRY_RUN|ASYNC|IO] [CLIENTS]%n", CounterClient.class.getName());
      System.err.println();
      System.err.println("       INCREMENT: the number of INCREMENT commands to be sent (default is 10)");
      System.err.println("       DRY_RUN  : dry run only (default)");
      System.err.println("       ASYNC    : use the AsyncApi");
      System.err.println("       IO       : use the BlockingApi");
      System.err.println("       CLIENTS  : the number of clients (default is 1)");
      System.exit(1);
    }
  }
}
