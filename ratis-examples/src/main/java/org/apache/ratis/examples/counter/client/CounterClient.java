/**
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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.examples.common.Constants;
import org.apache.ratis.grpc.GrpcFactory;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;

import java.io.IOException;
import java.nio.charset.Charset;
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
public final class CounterClient {

  private CounterClient(){
  }

  @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_BAD_PRACTICE")
  public static void main(String[] args)
      throws IOException, InterruptedException {
    //indicate the number of INCREMENT command, set 10 if no parameter passed
    int increment = args.length > 0 ? Integer.parseInt(args[0]) : 10;

    //build the counter cluster client
    RaftClient raftClient = buildClient();

    //use a executor service with 10 thread to send INCREMENT commands
    // concurrently
    ExecutorService executorService = Executors.newFixedThreadPool(10);

    //send INCREMENT commands concurrently
    System.out.printf("Sending %d increment command...%n", increment);
    for (int i = 0; i < increment; i++) {
      executorService.submit(() ->
          raftClient.io().send(Message.valueOf("INCREMENT")));
    }

    //shutdown the executor service and wait until they finish their work
    executorService.shutdown();
    executorService.awaitTermination(increment * 500L, TimeUnit.MILLISECONDS);

    //send GET command and print the response
    RaftClientReply count = raftClient.io().sendReadOnly(Message.valueOf("GET"));
    String response = count.getMessage().getContent().toString(Charset.defaultCharset());
    System.out.println(response);
  }

  /**
   * build the RaftClient instance which is used to communicate to
   * Counter cluster
   *
   * @return the created client of Counter cluster
   */
  private static RaftClient buildClient() {
    RaftProperties raftProperties = new RaftProperties();
    RaftClient.Builder builder = RaftClient.newBuilder()
        .setProperties(raftProperties)
        .setRaftGroup(Constants.RAFT_GROUP)
        .setClientRpc(
            new GrpcFactory(new Parameters())
                .newRaftClientRpc(ClientId.randomId(), raftProperties));
    return builder.build();
  }
}
