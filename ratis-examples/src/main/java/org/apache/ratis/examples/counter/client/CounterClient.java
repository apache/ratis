package org.apache.ratis.examples.counter.client;

import org.apache.ratis.client.*;
import org.apache.ratis.conf.*;
import org.apache.ratis.examples.counter.*;
import org.apache.ratis.grpc.*;
import org.apache.ratis.protocol.*;

import java.io.*;
import java.nio.charset.*;
import java.util.concurrent.*;

/**
 * Counter client application, this application sends specific number of
 * INCREMENT command to the Counter cluster and at the end sends a GET command
 * and print the result
 * <p>
 * Parameter to this application indicate the number of INCREMENT command, if no
 * parameter found, application use default value which is 10
 */
public class CounterClient {
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
    System.out.printf("Sending %d increment command...\n", increment);
    for (int i = 0; i < increment; i++) {
      executorService.submit(() ->
          raftClient.send(Message.valueOf("INCREMENT")));
    }

    //shutdown the executor service and wait until they finish their work
    executorService.shutdown();
    executorService.awaitTermination(increment * 500, TimeUnit.MILLISECONDS);

    //send GET command and print the response
    RaftClientReply count = raftClient.sendReadOnly(Message.valueOf("GET"));
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
        .setRaftGroup(CounterCommon.RAFT_GROUP)
        .setClientRpc(
            new GrpcFactory(new Parameters())
                .newRaftClientRpc(ClientId.randomId(), raftProperties));
    return builder.build();
  }
}
