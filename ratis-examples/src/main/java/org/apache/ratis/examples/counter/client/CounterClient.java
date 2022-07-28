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
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Counter client application, this application sends specific number of
 * INCREMENT command to the Counter cluster and at the end sends a GET command
 * and print the result
 * <p>
 * Parameter to this application indicate the number of INCREMENT command, if no
 * parameter found, application use default value which is 10
 */
public final class CounterClient implements Closeable {
  private final RaftClient client = RaftClient.newBuilder()
      .setProperties(new RaftProperties())
      .setRaftGroup(Constants.RAFT_GROUP)
      .build();

  @Override
  public void close() throws IOException {
    client.close();
  }

  private void run(int increment) throws Exception {
    //send INCREMENT commands concurrently
    System.out.printf("Sending %d increment command...%n", increment);
    final ExecutorService executor = Executors.newFixedThreadPool(10);
    for (int i = 0; i < increment; i++) {
      executor.submit(() -> client.io().send(CounterCommand.INCREMENT.getMessage()));
    }

    //shut down the executor and wait.
    executor.shutdown();
    for (int i = 1; executor.awaitTermination(increment * 500L, TimeUnit.MILLISECONDS); i++) {
      System.out.println("Waiting the executor to terminate ... " + i);
    }

    //send GET command and print the reply
    final RaftClientReply reply = client.io().sendReadOnly(CounterCommand.GET.getMessage());
    final String count = reply.getMessage().getContent().toString(Charset.defaultCharset());
    System.out.println("Current counter value: " + count);
  }

  public static void main(String[] args) {
    //build the counter cluster client
    try(CounterClient client = new CounterClient()) {
      //the number of INCREMENT commands, default is 10
      final int increment = args.length > 0 ? Integer.parseInt(args[0]) : 10;
      client.run(increment);
    } catch (Throwable e) {
      e.printStackTrace();
      System.err.println();
      System.err.println("args = " + Arrays.toString(args));
      System.err.println();
      System.err.println("Usage: java org.apache.ratis.examples.counter.client.CounterClient [increment]");
      System.err.println();
      System.err.println("       increment is the number of INCREMENT commands to be sent, where the default is 10.");
      System.exit(1);
    }
  }
}
