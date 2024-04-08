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
we implement a simple *Counter* service,
which maintains a counter value across a raft group.
Clients could send the following types of requests to the raft group:

* `INCREMENT`: increase the counter value by 1.
This command will trigger a transaction to change the state.
* `GET`: query the current value of the counter.
This is a read-only command since it does not change the state.

We have the following `enum` for representing the supported commands.

```java
/**
 * The supported commands the Counter example.
 */
public enum CounterCommand {
  /** Increment the counter by 1. */
  INCREMENT,
  /** Get the counter value. */
  GET;

  private final Message message = Message.valueOf(name());

  public Message getMessage() {
    return message;
  }

  /** Does the given command string match this command? */
  public boolean matches(String command) {
    return name().equalsIgnoreCase(command);
  }
}
```

Note:
The source code of the Counter example and the other examples is at
[Ratis examples](https://github.com/apache/ratis/tree/master/ratis-examples).
This article intends to show the steps of integration of Ratis.
If you wish to run the Counter example
please refer to [the README](https://github.com/apache/ratis/tree/master/ratis-examples#example-3-counter).

## Adding the Dependency

The first step is to add Ratis dependencies into the project.
The dependencies are available in maven central:

```xml
<dependency>
   <artifactId>ratis-server</artifactId>
   <groupId>org.apache.ratis</groupId>
</dependency>
```

Then, add one of the following transports:

* ratis-grpc
* ratis-netty
* ratis-hadoop

In this example,
we choose to use ratis-grpc:

```xml
<dependency>
   <artifactId>ratis-grpc</artifactId>
   <groupId>org.apache.ratis</groupId>
</dependency>
```

Please note that Apache Hadoop dependencies are shaded,
so it’s safe to use hadoop transport with different versions of Hadoop.

## Implementing the `CounterStateMachine`
A state machine manages the application logic.
The state machine is responsible for:

* Apply raft log transactions in order to maintain the current state.
  * When there is an `INCREMENT` request,
    it will first be written to the raft log as a log entry.
    Once the log entry is committed,
    the state machine will be invoked for applying the log entry as a transaction
    so that the counter value will be increased by 1.
  * When there is a `GET` request,
    it will not be written to the raft log
    since it is a readonly request which does not change the state.
    The state machine should return the current value of the counter.
* Manage snapshots loading/saving.
  * Snapshots are used for log compaction
    so that the state machine can be restored from a snapshot
    and then applies only the newer log entries,
    instead of applying a long history of log starting from the beginning.

We discuss how to implement `CounterStateMachine` in the following subsections.
The complete source code of it is in
[CounterStateMachine.java](https://github.com/apache/ratis/blob/master/ratis-examples/src/main/java/org/apache/ratis/examples/counter/server/CounterStateMachine.java).

### Defining the State
In this example,
the `CounterStateMachine` extends the `BaseStateMachine`,
which provides a base implementation of a `StateMachine`.

Inside the `CounterStateMachine`,
there is a `counter` object
which stores the current value.
The `counter` is an `AtomicInteger`
in order to support concurrent access.
We use the build-in `SimpleStateMachineStorage`,
which is a file-based storage implementation,
as a storage for storing snapshots.
The fields are shown below:

```java
public class CounterStateMachine extends BaseStateMachine {
  // ...

  private final SimpleStateMachineStorage storage = new SimpleStateMachineStorage();
  private final AtomicInteger counter = new AtomicInteger(0);

  // ...
}
```

### Applying Raft Log Entries

Once a raft log entry has been committed
(i.e. a majority of the servers have acknowledged),
Ratis notifies the state machine by invoking the `applyTransaction` method.
The `applyTransaction` method first validates the log entry.
Then, it applies the log entry by increasing the counter value and updates the term-index.
The code fragments are shown below.
Note that the `incrementCounter` method is synchronized
in order to update both counter and last applied term-index atomically.

```java
public class CounterStateMachine extends BaseStateMachine {
  // ...

  private synchronized int incrementCounter(TermIndex termIndex) {
    updateLastAppliedTermIndex(termIndex);
    return counter.incrementAndGet();
  }

  // ...

  /**
   * Apply the {@link CounterCommand#INCREMENT} by incrementing the counter object.
   *
   * @param trx the transaction context
   * @return the message containing the updated counter value
   */
  @Override
  public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
    final LogEntryProto entry = trx.getLogEntry();

    //check if the command is valid
    final String command = entry.getStateMachineLogEntry().getLogData().toString(Charset.defaultCharset());
    if (!CounterCommand.INCREMENT.match(command)) {
      return JavaUtils.completeExceptionally(new IllegalArgumentException("Invalid Command: " + command));
    }
    //increment the counter and update term-index
    final TermIndex termIndex = TermIndex.valueOf(entry);
    final long incremented = incrementCounter(termIndex);

    //if leader, log the incremented value and the term-index
    if (trx.getServerRole() == RaftPeerRole.LEADER) {
      LOG.info("{}: Increment to {}", termIndex, incremented);
    }

    //return the new value of the counter to the client
    return CompletableFuture.completedFuture(Message.valueOf(String.valueOf(incremented)));
  }

  // ...
}
```

### Processing Readonly Commands
The `INCREMENT` command is implemented in the previous section.
What about the `GET` command?
Since the `GET` command is a readonly command,
it is implemented by the `query` method instead of the `applyTransaction` method.
The code fragment is shown below.

```java
public class CounterStateMachine extends BaseStateMachine {
  // ...

  /**
   * Process {@link CounterCommand#GET}, which gets the counter value.
   *
   * @param request the GET request
   * @return a {@link Message} containing the current counter value as a {@link String}.
   */
  @Override
  public CompletableFuture<Message> query(Message request) {
    final String command = request.getContent().toString(Charset.defaultCharset());
    if (!CounterCommand.GET.match(command)) {
      return JavaUtils.completeExceptionally(new IllegalArgumentException("Invalid Command: " + command));
    }
    return CompletableFuture.completedFuture(Message.valueOf(counter.toString()));
  }

  // ...
}
```

### Taking Snapshots
When taking a snapshot,
the state is persisted in the storage of the state machine.
The snapshot can be loaded for restoring the state in the future.
In this example,
we use `ObjectOutputStream` to write the counter value to a snapshot file.
The term-index is stored in the file name of the snapshot file.
The code fragments are shown below.
Note that the `getState` method is synchronized
in order to get the applied term-index and the counter value atomically.
Note also that getting the counter value alone does not have to be synchronized
since the `counter` field is already an `AtomicInteger`.

```java
public class CounterStateMachine extends BaseStateMachine {
  // ...

  /** The state of the {@link CounterStateMachine}. */
  static class CounterState {
    private final TermIndex applied;
    private final int counter;

    CounterState(TermIndex applied, int counter) {
      this.applied = applied;
      this.counter = counter;
    }

    TermIndex getApplied() {
      return applied;
    }

    int getCounter() {
      return counter;
    }
  }

  // ...

  /** @return the current state. */
  private synchronized CounterState getState() {
    return new CounterState(getLastAppliedTermIndex(), counter.get());
  }

  // ...

  /**
   * Store the current state as a snapshot file in the {@link #storage}.
   *
   * @return the index of the snapshot
   */
  @Override
  public long takeSnapshot() {
    //get the current state
    final CounterState state = getState();
    final long index = state.getApplied().getIndex();

    //create a file with a proper name to store the snapshot
    final File snapshotFile = storage.getSnapshotFile(state.getApplied().getTerm(), index);

    //write the counter value into the snapshot file
    try (ObjectOutputStream out = new ObjectOutputStream(new BufferedOutputStream(
        Files.newOutputStream(snapshotFile.toPath())))) {
      out.writeInt(state.getCounter());
    } catch (IOException ioe) {
      LOG.warn("Failed to write snapshot file \"" + snapshotFile
              + "\", last applied index=" + state.getApplied());
    }

    //return the index of the stored snapshot (which is the last applied one)
    return index;
  }

  // ...
}
```

### Loading Snapshots
When loading a snapshot,
we use an `ObjectInputStream` to read the snapshot file.
The term-index is read from the file name of the snapshot file.
The code fragments are shown below.
Note that the `updateState` method is synchronized
in order to update the applied term-index and the counter value atomically.

```java
public class CounterStateMachine extends BaseStateMachine {
  // ...

  private synchronized void updateState(TermIndex applied, int counterValue) {
    updateLastAppliedTermIndex(applied);
    counter.set(counterValue);
  }

  // ...

  /**
   * Load the state of the state machine from the {@link #storage}.
   *
   * @param snapshot the information of the snapshot being loaded
   * @return the index of the snapshot or -1 if snapshot is invalid
   * @throws IOException if it failed to read from storage
   */
  private long load(SingleFileSnapshotInfo snapshot) throws IOException {
    //check null
    if (snapshot == null) {
      LOG.warn("The snapshot info is null.");
      return RaftLog.INVALID_LOG_INDEX;
    }
    //check if the snapshot file exists.
    final Path snapshotPath = snapshot.getFile().getPath();
    if (!Files.exists(snapshotPath)) {
      LOG.warn("The snapshot file {} does not exist for snapshot {}", snapshotPath, snapshot);
      return RaftLog.INVALID_LOG_INDEX;
    }

    //read the TermIndex from the snapshot file name
    final TermIndex last = SimpleStateMachineStorage.getTermIndexFromSnapshotFile(snapshotPath.toFile());

    //read the counter value from the snapshot file
    final int counterValue;
    try (ObjectInputStream in = new ObjectInputStream(new BufferedInputStream(Files.newInputStream(snapshotPath)))) {
      counterValue = in.readInt();
    }

    //update state
    updateState(last, counterValue);

    return last.getIndex();
  }

  // ...
}
```

### Implementing the `initialize` and `reinitialize` methods.
The `initialize` method is called at most once
when the server is starting up.
In contrast,
the `reinitialize` method is called when
1. the server is resumed from the `PAUSE` state, or
2. a new snapshot is installed from the leader or from an external source.

In `CounterStateMachine`,
the `reinitialize` method simply loads the latest snapshot
and the `initialize` method additionally initializes the `BaseStateMachine` super class and the storage.
```java
public class CounterStateMachine extends BaseStateMachine {
  // ...

  /**
   * Initialize the state machine storage and then load the state.
   *
   * @param server  the server running this state machine
   * @param groupId the id of the {@link org.apache.ratis.protocol.RaftGroup}
   * @param raftStorage the storage of the server
   * @throws IOException if it fails to load the state.
   */
  @Override
  public void initialize(RaftServer server, RaftGroupId groupId, RaftStorage raftStorage) throws IOException {
    super.initialize(server, groupId, raftStorage);
    storage.init(raftStorage);
    reinitialize();
  }

  /**
   * Simply load the state.
   *
   * @throws IOException if it fails to load the state.
   */
  @Override
  public void reinitialize() throws IOException {
    load(storage.getLatestSnapshot());
  }

  // ...
}
```
## Preparing a `RaftGroup`
In order to run a raft group,
each server must start a `RaftServer` instance,
which is responsible for communicating to each other through the Raft protocol.

It's important to keep in mind that,
each raft server knows the initial raft group when starting up.
They know the number of raft peers in the group
and the addresses of the peers.

In this example, we have a raft group with 3 peers.
For simplicity,
each peer listens to a specific port on the same machine.
The addresses of them are defined in a
[property file](https://github.com/apache/ratis/blob/master/ratis-examples/src/main/resources/conf.properties)
as below.

```properties
raft.server.address.list=127.0.0.1:10024,127.0.0.1:10124,127.0.0.1:11124
```

The peers are named as 'n0', 'n1' and 'n2'
and they form a `RaftGroup`.
For more details, see
[Constants.java](https://github.com/apache/ratis/blob/master/ratis-examples/src/main/java/org/apache/ratis/examples/common/Constants.java).

## Building & Starting the `CounterServer`

We use a `RaftServer.Builder` to build a `RaftServer`.
We first set up a `RaftProperties` object
with a local directory as the storage of the server
and a port number as the gRPC server port.
Then,
we create our `CounterStateMachine`
and pass everything to the builder as below.

```java
public final class CounterServer implements Closeable {
  private final RaftServer server;

  public CounterServer(RaftPeer peer, File storageDir) throws IOException {
    //create a property object
    final RaftProperties properties = new RaftProperties();

    //set the storage directory (different for each peer) in the RaftProperty object
    RaftServerConfigKeys.setStorageDir(properties, Collections.singletonList(storageDir));

    //set the port (different for each peer) in RaftProperty object
    final int port = NetUtils.createSocketAddr(peer.getAddress()).getPort();
    GrpcConfigKeys.Server.setPort(properties, port);

    //create the counter state machine which holds the counter value
    final CounterStateMachine counterStateMachine = new CounterStateMachine();

    //build the Raft server
    this.server = RaftServer.newBuilder()
            .setGroup(Constants.RAFT_GROUP)
            .setProperties(properties)
            .setServerId(peer.getId())
            .setStateMachine(counterStateMachine)
            .build();
  }

  // ...
}
```

Now we are ready to start our `CounterServer` peers and form a raft group.
The command is:
```shell
java org.apache.ratis.examples.counter.server.CounterServer peer_index
```
The argument `peer_index` must be 0, 1 or 2.

After a server is started,
it communicates with other peers in the group,
and performs raft actions such as leader election and append-log-entries.
After all three servers are started,
the counter service is up and running with the Raft protocol.

For more details, see
[CounterServer.java](https://github.com/apache/ratis/blob/master/ratis-examples/src/main/java/org/apache/ratis/examples/counter/server/CounterServer.java).

## Building & Running the `CounterClient`

We use a `RaftGroup` to build a `RaftClient`
and then use the `RaftClient` to send commands to the raft service.
Note that gRPC is the default RPC type
so that we may skip setting it in the `RaftProperties`.

```java
public final class CounterClient implements Closeable {
  private final RaftClient client = RaftClient.newBuilder()
          .setProperties(new RaftProperties())
          .setRaftGroup(Constants.RAFT_GROUP)
          .build();

  // ...
}
```

With this raft client,
we can then send commands using the `BlockingApi` returned by `RaftClient.io()`,
or the `AsyncApi` returned by `RaftClient.async()`.
The `send` method in the `BlockingApi`/`AsyncApi` is used to send the `INCREMENT` command as below.
```java
client.io().send(CounterCommand.INCREMENT.getMessage());
```
or
```java
client.async().send(CounterCommand.INCREMENT.getMessage());
```
The `sendReadonly` method in the `BlockingApi`/`AsyncApi` is used to send the `GET` command as below.
```java
client.io().sendReadOnly(CounterCommand.GET.getMessage());
```
or
```java
client.async().sendReadOnly(CounterCommand.GET.getMessage());
```
For more details, see
[CounterClient.java](https://github.com/apache/ratis/blob/master/ratis-examples/src/main/java/org/apache/ratis/examples/counter/client/CounterClient.java).