<!---
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# Getting Started
Let's get started to use Raft in your application.
To demonstrate how to use Ratis,
we'll implement a simple counter service,
which maintains a counter value across a cluster.
The client could send the following types of commands to the cluster:

* `INCREMENT`: increase the counter value
* `GET`: query the current value of the counter,
we call such kind of commands as read-only commands

Note: The full source could be found at [Ratis examples](https://github.com/apache/ratis/tree/master/ratis-examples).
This article is mainly intended to show the steps of integration of Ratis,
if you wish to run this example or find more examples,
please refer to [the README](https://github.com/apache/ratis/tree/master/ratis-examples#example-3-counter).

## Add the dependency

First, we need to add Ratis dependencies into the project,
it's available in maven central:

```xml
<dependency>
   <artifactId>ratis-server</artifactId>
   <groupId>org.apache.ratis</groupId>
</dependency>
```

Also, one of the following transports need to be added:

* grpc
* netty
* hadoop

For example, let's use grpc transport:

```xml
<dependency>
   <artifactId>ratis-grpc</artifactId>
   <groupId>org.apache.ratis</groupId>
</dependency>
```

Please note that Apache Hadoop dependencies are shaded,
so it’s safe to use hadoop transport with different versions of Hadoop.

## Create the StateMachine
A state machine is used to maintain the current state of the raft node,
the state machine is responsible for:

* Execute raft logs to get the state. In this example, when a `INCREMENT` log is executed, 
the counter value will be increased by 1.
And a `GET` log does not affect the state but only returns the current counter value to the client.
* Managing snapshots loading/saving.
Snapshots are used to speed the log execution,
the state machine could start from a snapshot point and only execute newer logs.

### Define the StateMachine
To define our state machine,
we can extend a class from the base class `BaseStateMachine`.

Also, a storage is needed to store snapshots,
and we'll use the build-in `SimpleStateMachineStorage`,
which is a file-based storage implementation.

Since we're going to implement a count server,
the `counter` instance is defined in the state machine,
represents the current value.
Below is the declaration of the state machine: 

```java
public class CounterStateMachine extends BaseStateMachine {
    private final SimpleStateMachineStorage storage =
            new SimpleStateMachineStorage();
    private AtomicInteger counter = new AtomicInteger(0);
    // ...
}
```

### Apply Raft Log Item

Once the raft log is committed,
Ratis will notify state machine by invoking the `public CompletableFuture<Message> applyTransaction(TransactionContext trx)` method,
and we need to override this method to decode the message and apply it. 

First, get the log content and decode it:

```java
public class CounterStateMachine extends BaseStateMachine {
    // ...
    public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
        final RaftProtos.LogEntryProto entry = trx.getLogEntry();
        String logData = entry.getStateMachineLogEntry().getLogData()
                .toString(Charset.defaultCharset());
        if (!logData.equals("INCREMENT")) {
            return CompletableFuture.completedFuture(
                    Message.valueOf("Invalid Command"));
        }
        // ...
    }
}
```

After that, if the log is valid,
we could apply it by increasing the counter value.
Remember that we also need to update the committed indexes:

```java
public class CounterStateMachine extends BaseStateMachine {
    // ...
    public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
        // ...
        final long index = entry.getIndex();
        updateLastAppliedTermIndex(entry.getTerm(), index);

        //actual execution of the command: increment the counter
        counter.incrementAndGet();

        //return the new value of the counter to the client
        final CompletableFuture<Message> f =
                CompletableFuture.completedFuture(Message.valueOf(counter.toString()));
        return f;
    }
}
```

### Handle Readonly Command
Note that we only handled `INCREMENT` command,
what about the `GET` command?
The `GET` command is implemented as a readonly command,
so we'll need to implement `public CompletableFuture<Message> query(Message request)` instead of `applyTransaction`.

```java
public class CounterStateMachine extends BaseStateMachine {
    // ...
    @Override
    public CompletableFuture<Message> query(Message request) {
        String msg = request.getContent().toString(Charset.defaultCharset());
        if (!msg.equals("GET")) {
            return CompletableFuture.completedFuture(
                    Message.valueOf("Invalid Command"));
        }
        return CompletableFuture.completedFuture(
                Message.valueOf(counter.toString()));
    }
}
```

### Save and Load Snapshots
When taking a snapshot,
we persist every state in the state machine,
and the value could be loaded directly to the state in the future.
In this example,
the only state is the counter value,
we're going to use `ObjectOutputStream` to write it to a snapshot file:

```java
public class CounterStateMachine extends BaseStateMachine {
    // ...
    @Override
    public long takeSnapshot() {
        //get the last applied index
        final TermIndex last = getLastAppliedTermIndex();

        //create a file with a proper name to store the snapshot
        final File snapshotFile =
                storage.getSnapshotFile(last.getTerm(), last.getIndex());

        //serialize the counter object and write it into the snapshot file
        try (ObjectOutputStream out = new ObjectOutputStream(
                new BufferedOutputStream(new FileOutputStream(snapshotFile)))) {
            out.writeObject(counter);
        } catch (IOException ioe) {
            LOG.warn("Failed to write snapshot file \"" + snapshotFile
                    + "\", last applied index=" + last);
        }

        //return the index of the stored snapshot (which is the last applied one)
        return last.getIndex();
    }
}
```

When loading it,
we could use `ObjectInputStream` to deserialize it.
Remember that we also need to implement `initialize` and `reinitialize` method,
so that the state machine will be correctly initialized.

## Build and Start a RaftServer
In order to build a raft cluster,
each node must start a `RaftServer` instance,
which is responsible for communicating to each other through Raft protocol.

It's important to keep in mind that,
each raft server knows exactly how many raft peers are in the cluster,
and what are the addresses of them.
In this example, we'll set a 3 node cluster.
For simplicity,
each peer listens to specific port on the same machine,
and we can define the addresses of the cluster in a configuration file:

```properties
raft.server.address.list=127.0.0.1:10024,127.0.0.1:10124,127.0.0.1:11124
```

We name those peers as 'n-0', 'n-1' and 'n-2',
and then we will create a `RaftGroup` instance representing them.
Since they are immutable,
we'll put them in the `Constant` class:

```java
public final class Constants {
    public static final List<RaftPeer> PEERS;
    private static final UUID CLUSTER_GROUP_ID = UUID.fromString("02511d47-d67c-49a3-9011-abb3109a44c1");

    static {
        // load addresses from configuration file
        // final String[] addresses = ...
        List<RaftPeer> peers = new ArrayList<>(addresses.length);
        for (int i = 0; i < addresses.length; i++) {
            peers.add(RaftPeer.newBuilder().setId("n" + i).setAddress(addresses[i]).build());
        }
        PEERS = Collections.unmodifiableList(peers);
    }
    
    public static final RaftGroup RAFT_GROUP = RaftGroup.valueOf(
            RaftGroupId.valueOf(Constants.CLUSTER_GROUP_ID), PEERS);
    // ...
}
```

Except for the cluster info,
another important thing is that we need to know the information of the current peer.
To achieve this,
we could pass the current peer's id as a program argument,
and then the raft server could be created:

```java
public final class CounterServer implements Closeable {
    private final RaftServer server;

    // the current peer will be passed as argument
    public CounterServer(RaftPeer peer, File storageDir) throws IOException {
        // ...
        CounterStateMachine counterStateMachine = new CounterStateMachine();

        //create and start the Raft server
        this.server = RaftServer.newBuilder()
                .setGroup(Constants.RAFT_GROUP)
                .setProperties(properties)
                .setServerId(peer.getId())
                .setStateMachine(counterStateMachine)
                .build();
    }

    public void start() throws IOException {
        server.start();
    }
}
```

Each `RaftServer` will own a `CounterStateMachine` instance,
as previously defined by us.
After that, all we need to do is to start it along with our application:

```java
public final class CounterServer implements Closeable {
    // ...
    public static void main(String[] args) throws IOException {
        // ...
        //find current peer object based on application parameter
        final RaftPeer currentPeer = Constants.PEERS.get(Integer.parseInt(args[0]) - 1);

        //start a counter server
        final File storageDir = new File("./" + currentPeer.getId());
        final CounterServer counterServer = new CounterServer(currentPeer, storageDir);
        counterServer.start();
        // ...
    }

}
```

After the server is started,
it will try to communicate with other peers in the cluster,
and perform raft actions like leader election, append log entries, etc.

## Build Raft Client

To send commands to the cluster,
we need to use a `RaftClient` instance.
All we need to know is the peers in the cluster, ie. the raft group. 

```java
public final class CounterClient {
    // ...
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
```

With this raft client,
we can then send commands by `raftClient.io().send` method,
and use `raftClient.io().sendReadonly` method for read only commands.
In this example,
to send `INCREMENT` and `GET` command,
we can do it like this:

```java
raftClient.io().send(Message.valueOf("INCREMENT")));
```

and

```java
RaftClientReply count = raftClient.io().sendReadOnly(Message.valueOf("GET"));
String response = count.getMessage().getContent().toString(Charset.defaultCharset());
System.out.println(response);
```

## Summary
It might seem a little complicated for beginners,
but since Raft itself is a hard topic,
this is already the simplest example we've found as a 'Hello World' for Ratis.
After you have a basic understanding of Ratis,
you'll find it really easy to be integrated into any projects. 

Next, you can take a look at other [examples](https://github.com/apache/ratis/tree/master/ratis-examples),
to know more about the features of Ratis.