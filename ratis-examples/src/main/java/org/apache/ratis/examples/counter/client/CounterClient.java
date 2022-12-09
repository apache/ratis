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

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Counter client application, this application sends specific number of
 * INCREMENT command to the Counter cluster and at the end sends a GET command
 * and print the result
 * <p>
 * Parameter to this application indicate the number of INCREMENT command, if no
 * parameter found, application use default value which is 10
 */
public final class CounterClient implements Closeable {
  //build the client
  private final RaftClient client = RaftClient.newBuilder()
      .setProperties(new RaftProperties())
      .setRaftGroup(Constants.RAFT_GROUP)
      .build();

  @Override
  public void close() throws IOException {
    client.close();
  }

  private void run(int increment, boolean blocking) throws Exception {
    System.out.printf("Sending %d %s command(s) using the %s ...%n",
        increment, CounterCommand.INCREMENT, blocking? "BlockingApi": "AsyncApi");
    final List<Future<RaftClientReply>> futures = new ArrayList<>(increment);

    //send INCREMENT command(s)
    if (blocking) {
      // use BlockingApi
      final ExecutorService executor = Executors.newFixedThreadPool(10);
      for (int i = 0; i < increment; i++) {
        final Future<RaftClientReply> f = executor.submit(
            () -> client.io().send(CounterCommand.INCREMENT.getMessage()));
        futures.add(f);
      }
      executor.shutdown();
    } else {
      // use AsyncApi
      for (int i = 0; i < increment; i++) {
        final Future<RaftClientReply> f = client.async().send(CounterCommand.INCREMENT.getMessage());
        futures.add(f);
      }
    }

    //wait for the futures
    for (Future<RaftClientReply> f : futures) {
      final RaftClientReply reply = f.get();
      if (reply.isSuccess()) {
        final String count = reply.getMessage().getContent().toStringUtf8();
        System.out.println("Counter is incremented to " + count);
      } else {
        System.err.println("Failed " + reply);
      }
    }

    //send a GET command and print the reply
    final RaftClientReply reply = client.io().sendReadOnly(CounterCommand.GET.getMessage());
    final String count = reply.getMessage().getContent().toStringUtf8();
    System.out.println("Current counter value: " + count);

    // using Linearizable Read
    futures.clear();
    final long startTime = System.currentTimeMillis();
    final ExecutorService executor = Executors.newFixedThreadPool(Constants.PEERS.size());
    Constants.PEERS.forEach(p -> {
      final Future<RaftClientReply> f = CompletableFuture.supplyAsync(() -> {
                try {
                  return client.io().sendReadOnly(CounterCommand.GET.getMessage(), p.getId());
                } catch (IOException e) {
                  System.err.println("Failed read-only request");
                  return RaftClientReply.newBuilder().setSuccess(false).build();
                }
              }, executor).whenCompleteAsync((r, ex) -> {
                if (ex != null || !r.isSuccess()) {
                  System.err.println("Failed " + r);
                  return;
                }
                final long endTime = System.currentTimeMillis();
                final long elapsedSec = (endTime-startTime) / 1000;
                final String countValue = r.getMessage().getContent().toStringUtf8();
                System.out.println("read from " + p.getId() + " and get counter value: " + countValue
                    + ", time elapsed: " + elapsedSec + " seconds");
              });
      futures.add(f);
    });

    for (Future<RaftClientReply> f : futures) {
      f.get();
    }
  }

  public static void main(String[] args) {
    try(CounterClient client = new CounterClient()) {
      //the number of INCREMENT commands, default is 10
      final int increment = args.length > 0 ? Integer.parseInt(args[0]) : 10;
      final boolean io = args.length > 1 && "io".equalsIgnoreCase(args[1]);
      client.run(increment, io);
    } catch (Throwable e) {
      e.printStackTrace();
      System.err.println();
      System.err.println("args = " + Arrays.toString(args));
      System.err.println();
      System.err.println("Usage: java org.apache.ratis.examples.counter.client.CounterClient [increment] [async|io]");
      System.err.println();
      System.err.println("       increment: the number of INCREMENT commands to be sent (default is 10)");
      System.err.println("       async    : use the AsyncApi (default)");
      System.err.println("       io       : use the BlockingApi");
      System.exit(1);
    }
  }
}
