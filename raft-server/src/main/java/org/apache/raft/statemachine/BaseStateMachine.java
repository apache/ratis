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

package org.apache.raft.statemachine;

import org.apache.raft.conf.RaftProperties;
import org.apache.raft.proto.RaftProtos;
import org.apache.raft.protocol.Message;
import org.apache.raft.protocol.RaftClientReply;
import org.apache.raft.protocol.RaftClientRequest;
import org.apache.raft.server.RaftConfiguration;
import org.apache.raft.server.RaftServerConstants;
import org.apache.raft.server.storage.RaftStorage;
import org.apache.raft.util.LifeCycle;
import org.apache.raft.util.ProtoUtils;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/**
 * Base implementation for StateMachines.
 */
public class BaseStateMachine implements StateMachine {

  protected RaftProperties properties;
  protected RaftStorage storage;
  protected RaftConfiguration raftConf;
  protected final LifeCycle lifeCycle = new LifeCycle(getClass().getSimpleName());

  @Override
  public LifeCycle.State getLifeCycleState() {
    return lifeCycle.getCurrentState();
  }

  @Override
  public void initialize(RaftProperties properties, RaftStorage storage) throws IOException {
    this.properties = properties;
    this.storage = storage;
  }

  @Override
  public void setRaftConfiguration(RaftConfiguration conf) {
    this.raftConf = conf;
  }

  @Override
  public RaftConfiguration getRaftConfiguration() {
    return this.raftConf;
  }

  @Override
  public SnapshotInfo getLatestSnapshot() {
    return getStateMachineStorage().getLatestSnapshot();
  }

  @Override
  public void notifyNotLeader(Collection<TrxContext> pendingEntries) {
    // do nothing
  }

  public void pause() {
  }

  @Override
  public void reinitialize(RaftProperties properties, RaftStorage storage) throws IOException {
  }

  @Override
  public CompletableFuture<Message> applyLogEntry(TrxContext trx) {
    // return the same message contained in the entry
    Message msg = () -> trx.getLogEntry().get().getSmLogEntry().getData().toByteArray();
    return CompletableFuture.completedFuture(msg);
  }

  @Override
  public long takeSnapshot() throws IOException {
    return RaftServerConstants.INVALID_LOG_INDEX;
  }

  @Override
  public StateMachineStorage getStateMachineStorage() {
    return new StateMachineStorage() {
      @Override
      public void init(RaftStorage raftStorage) throws IOException {
      }

      @Override
      public SnapshotInfo getLatestSnapshot() {
        return null;
      }

      @Override
      public void format() throws IOException {
      }
    };
  }

  @Override
  public CompletableFuture<RaftClientReply> query(
      RaftClientRequest request) {
    return null;
  }

  @Override
  public TrxContext startTransaction(RaftClientRequest request)
      throws IOException {
    return new TrxContext(request,
        RaftProtos.SMLogEntryProto.newBuilder()
            .setData(ProtoUtils.toByteString(request.getMessage().getContent()))
            .build());
  }

  @Override
  public void close() throws IOException {
    // do nothing
  }
}
