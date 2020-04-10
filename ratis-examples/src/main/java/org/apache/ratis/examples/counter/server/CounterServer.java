package org.apache.ratis.examples.counter.server;

import org.apache.ratis.conf.*;
import org.apache.ratis.examples.counter.*;
import org.apache.ratis.grpc.*;
import org.apache.ratis.protocol.*;
import org.apache.ratis.server.*;
import org.apache.ratis.util.*;

import java.io.*;
import java.util.*;

/**
 * Simplest Ratis server, use a simple state machine {@link CounterStateMachine}
 * which maintain a counter across multi server.
 * This server application designed to run several times with different
 * parameters (1,2 or 3). server addresses hard coded in {@link CounterCommon}
 * <p>
 * Run this application three times with three different parameter set-up a
 * ratis cluster which maintain a counter value replicated in each server memory
 */
public class CounterServer {

  public static void main(String[] args) throws IOException, InterruptedException {
    if (args.length < 1) {
      System.err.println("Usage: java -cp *.jar org.apache.ratis.examples.counter.server.CounterServer {serverIndex}");
      System.err.println("{serverIndex} could be 1, 2 or 3");
      System.exit(1);
    }

    //find current peer object based on application parameter
    RaftPeer currentPeer =
        CounterCommon.PEERS.get(Integer.parseInt(args[0]) - 1);

    //create a property object
    RaftProperties properties = new RaftProperties();

    //set the storage directory (different for each peer) in RaftProperty object
    File raftStorageDir = new File("./" + currentPeer.getId().toString());
    RaftServerConfigKeys.setStorageDir(properties,
        Collections.singletonList(raftStorageDir));

    //set the port which server listen to in RaftProperty object
    final int port = NetUtils.createSocketAddr(currentPeer.getAddress()).getPort();
    GrpcConfigKeys.Server.setPort(properties, port);

    //create the counter state machine which hold the counter value
    CounterStateMachine counterStateMachine = new CounterStateMachine();

    //create and start the Raft server
    RaftServer server = RaftServer.newBuilder()
        .setGroup(CounterCommon.RAFT_GROUP)
        .setProperties(properties)
        .setServerId(currentPeer.getId())
        .setStateMachine(counterStateMachine)
        .build();
    server.start();

    //exit when any input entered
    Scanner scanner = new Scanner(System.in);
    scanner.nextLine();
    server.close();
  }
}
