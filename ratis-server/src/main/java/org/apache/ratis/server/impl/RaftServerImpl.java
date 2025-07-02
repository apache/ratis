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

import org.apache.ratis.client.impl.ClientProtoUtils;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.metrics.Timekeeper;
import org.apache.ratis.proto.RaftProtos.AppendEntriesReplyProto;
import org.apache.ratis.proto.RaftProtos.AppendEntriesReplyProto.AppendResult;
import org.apache.ratis.proto.RaftProtos.AppendEntriesRequestProto;
import org.apache.ratis.proto.RaftProtos.CommitInfoProto;
import org.apache.ratis.proto.RaftProtos.InstallSnapshotReplyProto;
import org.apache.ratis.proto.RaftProtos.InstallSnapshotRequestProto;
import org.apache.ratis.proto.RaftProtos.InstallSnapshotResult;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.proto.RaftProtos.LogInfoProto;
import org.apache.ratis.proto.RaftProtos.RaftClientRequestProto;
import org.apache.ratis.proto.RaftProtos.RaftClientRequestProto.TypeCase;
import org.apache.ratis.proto.RaftProtos.RaftConfigurationProto;
import org.apache.ratis.proto.RaftProtos.RaftPeerRole;
import org.apache.ratis.proto.RaftProtos.RaftRpcRequestProto;
import org.apache.ratis.proto.RaftProtos.ReadIndexReplyProto;
import org.apache.ratis.proto.RaftProtos.ReadIndexRequestProto;
import org.apache.ratis.proto.RaftProtos.ReplicationLevel;
import org.apache.ratis.proto.RaftProtos.RequestVoteReplyProto;
import org.apache.ratis.proto.RaftProtos.RequestVoteRequestProto;
import org.apache.ratis.proto.RaftProtos.RoleInfoProto;
import org.apache.ratis.proto.RaftProtos.StartLeaderElectionReplyProto;
import org.apache.ratis.proto.RaftProtos.StartLeaderElectionRequestProto;
import org.apache.ratis.protocol.ClientInvocationId;
import org.apache.ratis.protocol.GroupInfoReply;
import org.apache.ratis.protocol.GroupInfoRequest;
import org.apache.ratis.protocol.LeaderElectionManagementRequest;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientAsynchronousProtocol;
import org.apache.ratis.protocol.RaftClientProtocol;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.SetConfigurationRequest;
import org.apache.ratis.protocol.SnapshotManagementRequest;
import org.apache.ratis.protocol.TransferLeadershipRequest;
import org.apache.ratis.protocol.exceptions.GroupMismatchException;
import org.apache.ratis.protocol.exceptions.LeaderNotReadyException;
import org.apache.ratis.protocol.exceptions.LeaderSteppingDownException;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.apache.ratis.protocol.exceptions.RaftException;
import org.apache.ratis.protocol.exceptions.ReadException;
import org.apache.ratis.protocol.exceptions.ReadIndexException;
import org.apache.ratis.protocol.exceptions.ReconfigurationInProgressException;
import org.apache.ratis.protocol.exceptions.ResourceUnavailableException;
import org.apache.ratis.protocol.exceptions.ServerNotReadyException;
import org.apache.ratis.protocol.exceptions.SetConfigurationException;
import org.apache.ratis.protocol.exceptions.StaleReadException;
import org.apache.ratis.protocol.exceptions.StateMachineException;
import org.apache.ratis.protocol.exceptions.TransferLeadershipException;
import org.apache.ratis.server.DataStreamMap;
import org.apache.ratis.server.DivisionInfo;
import org.apache.ratis.server.DivisionProperties;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.RaftServerRpc;
import org.apache.ratis.server.impl.LeaderElection.Phase;
import org.apache.ratis.server.impl.RetryCacheImpl.CacheEntry;
import org.apache.ratis.server.impl.ServerImplUtils.ConsecutiveIndices;
import org.apache.ratis.server.impl.ServerImplUtils.NavigableIndices;
import org.apache.ratis.server.leader.LeaderState;
import org.apache.ratis.server.metrics.LeaderElectionMetrics;
import org.apache.ratis.server.metrics.RaftServerMetricsImpl;
import org.apache.ratis.server.protocol.RaftServerAsynchronousProtocol;
import org.apache.ratis.server.protocol.RaftServerProtocol;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.raftlog.LogProtoUtils;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.server.raftlog.RaftLogIOException;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.server.storage.RaftStorageDirectory;
import org.apache.ratis.statemachine.SnapshotInfo;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.TransactionContextImpl;
import org.apache.ratis.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.ratis.util.CodeInjectionForTesting;
import org.apache.ratis.util.CollectionUtils;
import org.apache.ratis.util.ConcurrentUtils;
import org.apache.ratis.util.FileUtils;
import org.apache.ratis.util.IOUtils;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.LifeCycle;
import org.apache.ratis.util.LifeCycle.State;
import org.apache.ratis.util.MemoizedSupplier;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.ProtoUtils;
import org.apache.ratis.util.TimeDuration;
import org.apache.ratis.util.function.CheckedSupplier;

import java.io.File;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.ratis.server.impl.ServerImplUtils.assertEntries;
import static org.apache.ratis.server.impl.ServerImplUtils.assertGroup;
import static org.apache.ratis.server.impl.ServerImplUtils.effectiveCommitIndex;
import static org.apache.ratis.server.impl.ServerProtoUtils.toAppendEntriesReplyProto;
import static org.apache.ratis.server.impl.ServerProtoUtils.toReadIndexReplyProto;
import static org.apache.ratis.server.impl.ServerProtoUtils.toReadIndexRequestProto;
import static org.apache.ratis.server.impl.ServerProtoUtils.toRequestVoteReplyProto;
import static org.apache.ratis.server.impl.ServerProtoUtils.toStartLeaderElectionReplyProto;
import static org.apache.ratis.server.util.ServerStringUtils.toAppendEntriesReplyString;
import static org.apache.ratis.server.util.ServerStringUtils.toAppendEntriesRequestString;
import static org.apache.ratis.server.util.ServerStringUtils.toRequestVoteReplyString;

class RaftServerImpl implements RaftServer.Division,
    RaftServerProtocol, RaftServerAsynchronousProtocol,
    RaftClientProtocol, RaftClientAsynchronousProtocol {
  private static final String CLASS_NAME = JavaUtils.getClassSimpleName(RaftServerImpl.class);
  static final String REQUEST_VOTE = CLASS_NAME + ".requestVote";
  static final String APPEND_ENTRIES = CLASS_NAME + ".appendEntries";
  static final String INSTALL_SNAPSHOT = CLASS_NAME + ".installSnapshot";
  static final String APPEND_TRANSACTION = CLASS_NAME + ".appendTransaction";
  static final String LOG_SYNC = APPEND_ENTRIES + ".logComplete";
  static final String START_LEADER_ELECTION = CLASS_NAME + ".startLeaderElection";
  static final String START_COMPLETE = CLASS_NAME + ".startComplete";

  class Info implements DivisionInfo {
    @Override
    public RaftPeerRole getCurrentRole() {
      return getRole().getCurrentRole();
    }

    @Override
    public boolean isLeaderReady() {
      return isLeader() && getRole().isLeaderReady();
    }

    @Override
    public RaftPeerId getLeaderId() {
      return getState().getLeaderId();
    }

    @Override
    public LifeCycle.State getLifeCycleState() {
      return lifeCycle.getCurrentState();
    }

    @Override
    public RoleInfoProto getRoleInfoProto() {
      return RaftServerImpl.this.getRoleInfoProto();
    }

    @Override
    public long getCurrentTerm() {
      return getState().getCurrentTerm();
    }

    @Override
    public long getLastAppliedIndex() {
      return getState().getLastAppliedIndex();
    }

    @Override
    public long[] getFollowerNextIndices() {
      return role.getLeaderState()
          .filter(leader -> isLeader())
          .map(LeaderStateImpl::getFollowerNextIndices)
          .orElse(null);
    }

    @Override
    public long[] getFollowerMatchIndices() {
      return role.getLeaderState()
          .filter(leader -> isLeader())
          .map(LeaderStateImpl::getFollowerMatchIndices)
          .orElse(null);
    }
  }

  private final RaftServerProxy proxy;
  private final StateMachine stateMachine;
  private final Info info =  new Info();

  private final DivisionProperties divisionProperties;
  private final TimeDuration leaderStepDownWaitTime;
  private final boolean memberMajorityAddEnabled;
  private final TimeDuration sleepDeviationThreshold;

  private final LifeCycle lifeCycle;
  private final ServerState state;
  private final RoleInfo role;

  private final DataStreamMap dataStreamMap;
  private final RaftServerConfigKeys.Read.Option readOption;

  private final TransactionManager transactionManager;
  private final RetryCacheImpl retryCache;
  private final CommitInfoCache commitInfoCache = new CommitInfoCache();
  private final WriteIndexCache writeIndexCache;

  private final RaftServerJmxAdapter jmxAdapter = new RaftServerJmxAdapter(this);
  private final LeaderElectionMetrics leaderElectionMetrics;
  private final RaftServerMetricsImpl raftServerMetrics;
  private final CountDownLatch closeFinishedLatch = new CountDownLatch(1);

  // To avoid append entry before complete start() method
  // For example, if thread1 start(), but before thread1 startAsFollower(), thread2 receive append entry
  // request, and change state to RUNNING by lifeCycle.compareAndTransition(STARTING, RUNNING),
  // then thread1 execute lifeCycle.transition(RUNNING) in startAsFollower(),
  // So happens IllegalStateException: ILLEGAL TRANSITION: RUNNING -> RUNNING,
  private final AtomicBoolean startComplete;

  private final TransferLeadership transferLeadership;
  private final SnapshotManagementRequestHandler snapshotRequestHandler;
  private final SnapshotInstallationHandler snapshotInstallationHandler;

  private final ExecutorService serverExecutor;
  private final ExecutorService clientExecutor;

  private final AtomicBoolean firstElectionSinceStartup = new AtomicBoolean(true);
  private final ThreadGroup threadGroup;

  private final AtomicReference<CompletableFuture<Void>> appendLogFuture;
  private final NavigableIndices appendLogTermIndices = new NavigableIndices();

  RaftServerImpl(RaftGroup group, StateMachine stateMachine, RaftServerProxy proxy, RaftStorage.StartupOption option)
      throws IOException {
    final RaftPeerId id = proxy.getId();
    LOG.info("{}: new RaftServerImpl for {} with {}", id, group, stateMachine);
    this.lifeCycle = new LifeCycle(id);
    this.stateMachine = stateMachine;
    this.role = new RoleInfo(id);

    final RaftProperties properties = proxy.getProperties();
    this.divisionProperties = new DivisionPropertiesImpl(properties);
    this.leaderStepDownWaitTime = RaftServerConfigKeys.LeaderElection.leaderStepDownWaitTime(properties);
    this.memberMajorityAddEnabled = RaftServerConfigKeys.LeaderElection.memberMajorityAdd(properties);
    this.sleepDeviationThreshold = RaftServerConfigKeys.sleepDeviationThreshold(properties);
    this.proxy = proxy;

    this.state = new ServerState(id, group, stateMachine, this, option, properties);
    this.retryCache = new RetryCacheImpl(properties);
    this.dataStreamMap = new DataStreamMapImpl(id);
    this.readOption = RaftServerConfigKeys.Read.option(properties);
    this.writeIndexCache = new WriteIndexCache(properties);
    this.transactionManager = new TransactionManager(id);

    this.leaderElectionMetrics = LeaderElectionMetrics.getLeaderElectionMetrics(
        getMemberId(), state::getLastLeaderElapsedTimeMs);
    this.raftServerMetrics = RaftServerMetricsImpl.computeIfAbsentRaftServerMetrics(
        getMemberId(), this::getCommitIndex, retryCache::getStatistics);

    this.startComplete = new AtomicBoolean(false);
    this.threadGroup = new ThreadGroup(proxy.getThreadGroup(), getMemberId().toString());

    this.transferLeadership = new TransferLeadership(this, properties);
    this.snapshotRequestHandler = new SnapshotManagementRequestHandler(this);
    this.snapshotInstallationHandler = new SnapshotInstallationHandler(this, properties);
    this.appendLogFuture = new AtomicReference<>(CompletableFuture.completedFuture(null));

    this.serverExecutor = ConcurrentUtils.newThreadPoolWithMax(
        RaftServerConfigKeys.ThreadPool.serverCached(properties),
        RaftServerConfigKeys.ThreadPool.serverSize(properties),
        id + "-server");
    this.clientExecutor = ConcurrentUtils.newThreadPoolWithMax(
        RaftServerConfigKeys.ThreadPool.clientCached(properties),
        RaftServerConfigKeys.ThreadPool.clientSize(properties),
        id + "-client");
  }

  private long getCommitIndex(RaftPeerId id) {
    return commitInfoCache.get(id).orElse(0L);
  }

  @Override
  public DivisionProperties properties() {
    return divisionProperties;
  }

  int getMaxTimeoutMs() {
    return properties().maxRpcTimeoutMs();
  }

  TimeDuration getRandomElectionTimeout() {
    if (firstElectionSinceStartup.get()) {
      return getFirstRandomElectionTimeout();
    }
    final int min = properties().minRpcTimeoutMs();
    final int millis = min + ThreadLocalRandom.current().nextInt(properties().maxRpcTimeoutMs() - min + 1);
    return TimeDuration.valueOf(millis, TimeUnit.MILLISECONDS);
  }

  private TimeDuration getFirstRandomElectionTimeout() {
    final RaftProperties properties = proxy.getProperties();
    final int min = RaftServerConfigKeys.Rpc.firstElectionTimeoutMin(properties).toIntExact(TimeUnit.MILLISECONDS);
    final int max = RaftServerConfigKeys.Rpc.firstElectionTimeoutMax(properties).toIntExact(TimeUnit.MILLISECONDS);
    final int mills = min + ThreadLocalRandom.current().nextInt(max - min + 1);
    return TimeDuration.valueOf(mills, TimeUnit.MILLISECONDS);
  }

  TimeDuration getLeaderStepDownWaitTime() {
    return leaderStepDownWaitTime;
  }

  TimeDuration getSleepDeviationThreshold() {
    return sleepDeviationThreshold;
  }

  @Override
  public ThreadGroup getThreadGroup() {
    return threadGroup;
  }

  @Override
  public StateMachine getStateMachine() {
    return stateMachine;
  }

  @Override
  public RaftLog getRaftLog() {
    return getState().getLog();
  }

  @Override
  public RaftStorage getRaftStorage() {
    return getState().getStorage();
  }

  @Override
  public DataStreamMap getDataStreamMap() {
    return dataStreamMap;
  }

  @Override
  public RetryCacheImpl getRetryCache() {
    return retryCache;
  }

  @Override
  public RaftServerProxy getRaftServer() {
    return proxy;
  }

  TransferLeadership getTransferLeadership() {
    return transferLeadership;
  }

  RaftServerRpc getServerRpc() {
    return proxy.getServerRpc();
  }

  private void setRole(RaftPeerRole newRole, Object reason) {
    LOG.info("{}: changes role from {} to {} at term {} for {}",
        getMemberId(), this.role, newRole, state.getCurrentTerm(), reason);
    this.role.transitionRole(newRole);
  }

  boolean start() throws IOException {
    if (!lifeCycle.compareAndTransition(State.NEW, State.STARTING)) {
      return false;
    }
    state.initialize(stateMachine);

    final RaftConfigurationImpl conf = getRaftConf();
    if (conf != null && conf.containsInBothConfs(getId())) {
      LOG.info("{}: start as a follower, conf={}", getMemberId(), conf);
      startAsPeer(RaftPeerRole.FOLLOWER);
    } else if (conf != null && conf.containsInConf(getId(), RaftPeerRole.LISTENER)) {
      LOG.info("{}: start as a listener, conf={}", getMemberId(), conf);
      startAsPeer(RaftPeerRole.LISTENER);
    } else {
      LOG.info("{}: start with initializing state, conf={}", getMemberId(), conf);
      setRole(RaftPeerRole.FOLLOWER, "start");
    }

    jmxAdapter.registerMBean();
    state.start();
    CodeInjectionForTesting.execute(START_COMPLETE, getId(), null, role);
    if (startComplete.compareAndSet(false, true)) {
      LOG.info("{}: Successfully started.", getMemberId());
    }
    return true;
  }

  /**
   * The peer belongs to the current configuration, should start as a follower or listener
   */
  private void startAsPeer(RaftPeerRole newRole) {
    final Object reason;
    if (newRole == RaftPeerRole.FOLLOWER) {
      reason = "startAsFollower";
      setRole(RaftPeerRole.FOLLOWER, reason);
    } else if (newRole == RaftPeerRole.LISTENER) {
      reason = "startAsListener";
      setRole(RaftPeerRole.LISTENER, reason);
    } else {
      throw new IllegalArgumentException("Unexpected role " + newRole);
    }
    role.startFollowerState(this, reason);

    lifeCycle.transition(State.RUNNING);
  }

  ServerState getState() {
    return state;
  }

  @Override
  public RaftGroupMemberId getMemberId() {
    return getState().getMemberId();
  }

  @Override
  public RaftPeer getPeer() {
    return Optional.ofNullable(getState().getCurrentPeer())
        .orElseGet(() -> getRaftServer().getPeer());
  }

  @Override
  public DivisionInfo getInfo() {
    return info;
  }

  RoleInfo getRole() {
    return role;
  }

  @Override
  public RaftConfigurationImpl getRaftConf() {
    return getState().getRaftConf();
  }

  /**
   * This removes the group from the server.
   * If the deleteDirectory flag is set to false, and renameDirectory
   * the directory is moved to
   * {@link RaftServerConfigKeys#REMOVED_GROUPS_DIR_KEY} location.
   * If the deleteDirectory flag is true, the group is permanently deleted.
   */
  void groupRemove(boolean deleteDirectory, boolean renameDirectory) {
    final RaftStorageDirectory dir = state.getStorage().getStorageDir();

    /* Shutdown is triggered here inorder to avoid any locked files. */
    state.getStateMachineUpdater().setRemoving();
    close();
    try {
      closeFinishedLatch.await();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.warn("{}: Waiting closing interrupted, will not continue to remove group locally", getMemberId());
      return;
    }
    getStateMachine().event().notifyGroupRemove();
    if (deleteDirectory) {
      for (int i = 0; i < FileUtils.NUM_ATTEMPTS; i ++) {
        try {
          FileUtils.deleteFully(dir.getRoot());
          LOG.info("{}: Succeed to remove RaftStorageDirectory {}", getMemberId(), dir);
          break;
        } catch (NoSuchFileException e) {
          LOG.warn("{}: Some file does not exist {}", getMemberId(), dir, e);
        } catch (Exception e) {
          LOG.error("{}: Failed to remove RaftStorageDirectory {}", getMemberId(), dir, e);
          break;
        }
      }
    } else if(renameDirectory) {
      try {
        /* Create path with current group in REMOVED_GROUPS_DIR_KEY location */
        File toBeRemovedGroupFolder = new File(RaftServerConfigKeys
            .removedGroupsDir(proxy.getProperties()),
            dir.getRoot().getName());

        FileUtils.moveDirectory(dir.getRoot().toPath(),
            toBeRemovedGroupFolder.toPath());

        LOG.info("{}: Group {} is renamed successfully", getMemberId(), getGroup());
      } catch (IOException e) {
        LOG.warn("{}: Failed to remove group {}", getMemberId(),
            dir.getRoot().getName(), e);
      }
    }
  }

  @Override
  public void close() {
    lifeCycle.checkStateAndClose(() -> {
      LOG.info("{}: shutdown", getMemberId());
      try {
        jmxAdapter.unregister();
      } catch (Exception e) {
        LOG.warn("{}: Failed to un-register RaftServer JMX bean", getMemberId(), e);
      }
      try {
        role.shutdownFollowerState();
      } catch (Exception e) {
        LOG.warn("{}: Failed to shutdown FollowerState", getMemberId(), e);
      }
      try{
        role.shutdownLeaderElection();
      } catch (Exception e) {
        LOG.warn("{}: Failed to shutdown LeaderElection", getMemberId(), e);
      }
      try{
        role.shutdownLeaderState(true).join();
      } catch (Exception e) {
        LOG.warn("{}: Failed to shutdown LeaderState monitor", getMemberId(), e);
      }
      try{
        state.close();
      } catch (Exception e) {
        LOG.warn("{}: Failed to close state", getMemberId(), e);
      }
      try {
        leaderElectionMetrics.unregister();
        raftServerMetrics.unregister();
        RaftServerMetricsImpl.removeRaftServerMetrics(getMemberId());
      } catch (Exception e) {
        LOG.warn("{}: Failed to unregister metric", getMemberId(), e);
      }
      try {
        ConcurrentUtils.shutdownAndWait(clientExecutor);
      } catch (Exception e) {
        LOG.warn(getMemberId() + ": Failed to shutdown clientExecutor", e);
      }
      try {
        ConcurrentUtils.shutdownAndWait(serverExecutor);
      } catch (Exception e) {
        LOG.warn(getMemberId() + ": Failed to shutdown serverExecutor", e);
      }
      closeFinishedLatch.countDown();
    });
  }

  void setFirstElection(Object reason) {
    if (firstElectionSinceStartup.compareAndSet(true, false)) {
      LOG.info("{}: set firstElectionSinceStartup to false for {}", getMemberId(), reason);
    }
  }

  /**
   * Change the server state to Follower if this server is in a different role or force is true.
   * @param newTerm The new term.
   * @param force Force to start a new {@link FollowerState} even if this server is already a follower.
   * @return if the term/votedFor should be updated to the new term
   */
  private synchronized CompletableFuture<Void> changeToFollower(
      long newTerm, boolean force, boolean allowListener, Object reason, AtomicBoolean metadataUpdated) {
    final RaftPeerRole old = role.getCurrentRole();
    if (old == RaftPeerRole.LISTENER && !allowListener) {
      throw new IllegalStateException("Unexpected role " + old);
    }
    CompletableFuture<Void> future = CompletableFuture.completedFuture(null);
    if ((old != RaftPeerRole.FOLLOWER || force) && old != RaftPeerRole.LISTENER) {
      setRole(RaftPeerRole.FOLLOWER, reason);
      if (old == RaftPeerRole.LEADER) {
        future = role.shutdownLeaderState(false)
            .exceptionally(e -> {
              if (e != null) {
                if (!getInfo().isAlive()) {
                  LOG.info("Since server is not alive {}, safely ignore {}", this, e.toString());
                  return null;
                }
              }
              throw new CompletionException("Failed to shutdownLeaderState: " + this, e);
            });
        state.setLeader(null, reason);
      } else if (old == RaftPeerRole.CANDIDATE) {
        future = role.shutdownLeaderElection();
      } else if (old == RaftPeerRole.FOLLOWER) {
        future = role.shutdownFollowerState();
      }

      metadataUpdated.set(state.updateCurrentTerm(newTerm));
      role.startFollowerState(this, reason);
      setFirstElection(reason);
    } else {
      metadataUpdated.set(state.updateCurrentTerm(newTerm));
    }
    return future;
  }

  synchronized CompletableFuture<Void> changeToFollowerAndPersistMetadata(
      long newTerm,
      boolean allowListener,
      Object reason) throws IOException {
    final AtomicBoolean metadataUpdated = new AtomicBoolean();
    final CompletableFuture<Void> future = changeToFollower(newTerm, false, allowListener, reason, metadataUpdated);
    if (metadataUpdated.get()) {
      state.persistMetadata();
    }
    return future;
  }

  synchronized void changeToLeader() {
    Preconditions.assertTrue(getInfo().isCandidate());
    role.shutdownLeaderElection();
    setRole(RaftPeerRole.LEADER, "changeToLeader");
    final LeaderStateImpl leader = role.updateLeaderState(this);
    state.becomeLeader();

    // start sending AppendEntries RPC to followers
    leader.start();
  }

  @Override
  public Collection<CommitInfoProto> getCommitInfos() {
    final List<CommitInfoProto> infos = new ArrayList<>();
    // add the commit info of this server
    final long commitIndex = updateCommitInfoCache();
    infos.add(ProtoUtils.toCommitInfoProto(getPeer(), commitIndex));

    // add the commit infos of other servers
    if (getInfo().isLeader()) {
      role.getLeaderState().ifPresent(
          leader -> leader.updateFollowerCommitInfos(commitInfoCache, infos));
    } else {
      RaftConfigurationImpl raftConf = getRaftConf();
      Stream.concat(
              raftConf.getAllPeers(RaftPeerRole.FOLLOWER).stream(),
              raftConf.getAllPeers(RaftPeerRole.LISTENER).stream())
          .filter(peer -> !peer.getId().equals(getId()))
          .map(peer -> commitInfoCache.get(peer.getId())
              .map(index -> ProtoUtils.toCommitInfoProto(peer, index))
              .orElse(null))
          .filter(Objects::nonNull)
          .forEach(infos::add);
    }
    return infos;
  }

  GroupInfoReply getGroupInfo(GroupInfoRequest request) {
    final RaftStorageDirectory dir = state.getStorage().getStorageDir();
    final RaftConfigurationProto conf =
        LogProtoUtils.toRaftConfigurationProtoBuilder(getRaftConf()).build();
    return new GroupInfoReply(request, getCommitInfos(), getGroup(), getRoleInfoProto(),
        dir.isHealthy(), conf, getLogInfo());
  }

  LogInfoProto getLogInfo(){
    final RaftLog log = getRaftLog();
    LogInfoProto.Builder logInfoBuilder = LogInfoProto.newBuilder();
    final TermIndex applied = getStateMachine().getLastAppliedTermIndex();
    if (applied != null) {
      logInfoBuilder.setApplied(applied.toProto());
    }
    final TermIndex committed = log.getTermIndex(log.getLastCommittedIndex());
    if (committed != null) {
      logInfoBuilder.setCommitted(committed.toProto());
    }
    final TermIndex entry = log.getLastEntryTermIndex();
    if (entry != null) {
      logInfoBuilder.setLastEntry(entry.toProto());
    }
    final SnapshotInfo snapshot = getStateMachine().getLatestSnapshot();
    if (snapshot != null) {
      logInfoBuilder.setLastSnapshot(snapshot.getTermIndex().toProto());
    }
    return logInfoBuilder.build();
  }

  RoleInfoProto getRoleInfoProto() {
    return role.buildRoleInfoProto(this);
  }

  synchronized void changeToCandidate(boolean forceStartLeaderElection) {
    Preconditions.assertTrue(getInfo().isFollower());
    role.shutdownFollowerState();
    setRole(RaftPeerRole.CANDIDATE, "changeToCandidate");
    if (state.shouldNotifyExtendedNoLeader()) {
      stateMachine.followerEvent().notifyExtendedNoLeader(getRoleInfoProto());
    }
    // start election
    role.startLeaderElection(this, forceStartLeaderElection);
  }

  @Override
  public String toString() {
    return role + " (" + lifeCycle.getCurrentState() + "): " + state;
  }

  RaftClientReply.Builder newReplyBuilder(RaftClientRequest request) {
    return RaftClientReply.newBuilder()
        .setRequest(request)
        .setCommitInfos(getCommitInfos());
  }

  private RaftClientReply.Builder newReplyBuilder(ClientInvocationId invocationId, long logIndex) {
    return RaftClientReply.newBuilder()
        .setClientInvocationId(invocationId)
        .setLogIndex(logIndex)
        .setServerId(getMemberId())
        .setCommitInfos(getCommitInfos());
  }

  RaftClientReply newSuccessReply(RaftClientRequest request) {
    return newReplyBuilder(request)
        .setSuccess()
        .build();
  }

  RaftClientReply newSuccessReply(RaftClientRequest request, long logIndex) {
    return newReplyBuilder(request)
        .setSuccess()
        .setLogIndex(logIndex)
        .build();
  }

  RaftClientReply newExceptionReply(RaftClientRequest request, RaftException exception) {
    return newReplyBuilder(request)
        .setException(exception)
        .build();
  }

  private CompletableFuture<RaftClientReply> checkLeaderState(RaftClientRequest request) {
    return checkLeaderState(request, null);
  }

  /**
   * @return null if the server is in leader state.
   */
  private CompletableFuture<RaftClientReply> checkLeaderState(RaftClientRequest request, CacheEntry entry) {
    try {
      assertGroup(getMemberId(), request);
    } catch (GroupMismatchException e) {
      return RetryCacheImpl.failWithException(e, entry);
    }

    if (!getInfo().isLeader()) {
      NotLeaderException exception = generateNotLeaderException();
      final RaftClientReply reply = newExceptionReply(request, exception);
      return RetryCacheImpl.failWithReply(reply, entry);
    }
    if (!getInfo().isLeaderReady()) {
      final CacheEntry cacheEntry = retryCache.getIfPresent(ClientInvocationId.valueOf(request));
      if (cacheEntry != null && cacheEntry.isCompletedNormally()) {
        return cacheEntry.getReplyFuture();
      }
      final LeaderNotReadyException lnre = new LeaderNotReadyException(getMemberId());
      final RaftClientReply reply = newExceptionReply(request, lnre);
      return RetryCacheImpl.failWithReply(reply, entry);
    }

    if (!request.isReadOnly() && isSteppingDown()) {
      final LeaderSteppingDownException lsde = new LeaderSteppingDownException(getMemberId() + " is stepping down");
      final RaftClientReply reply = newExceptionReply(request, lsde);
      return RetryCacheImpl.failWithReply(reply, entry);
    }

    return null;
  }

  NotLeaderException generateNotLeaderException() {
    if (!lifeCycle.getCurrentState().isRunning()) {
      return new NotLeaderException(getMemberId(), null, null);
    }
    RaftPeerId leaderId = state.getLeaderId();
    if (leaderId == null || leaderId.equals(getId())) {
      // No idea about who is the current leader. Or the peer is the current
      // leader, but it is about to step down. set the suggested leader as null.
      leaderId = null;
    }
    final RaftConfigurationImpl conf = getRaftConf();
    Collection<RaftPeer> peers = conf.getAllPeers();
    return new NotLeaderException(getMemberId(), conf.getPeer(leaderId), peers);
  }

  void assertLifeCycleState(Set<LifeCycle.State> expected) throws ServerNotReadyException {
    lifeCycle.assertCurrentState((n, c) -> new ServerNotReadyException(
        getMemberId() + " is not in " + expected + ": current state is " + c), expected);
  }

  /**
   * Handle a normal update request from client.
   */
  private CompletableFuture<RaftClientReply> appendTransaction(
      RaftClientRequest request, TransactionContextImpl context, CacheEntry cacheEntry) throws IOException {
    CodeInjectionForTesting.execute(APPEND_TRANSACTION, getId(),
        request.getClientId(), request, context, cacheEntry);

    assertLifeCycleState(LifeCycle.States.RUNNING);

    final PendingRequest pending;
    synchronized (this) {
      final CompletableFuture<RaftClientReply> reply = checkLeaderState(request, cacheEntry);
      if (reply != null) {
        return reply;
      }

      // append the message to its local log
      final LeaderStateImpl leaderState = role.getLeaderStateNonNull();
      writeIndexCache.add(request.getClientId(), context.getLogIndexFuture());

      final PendingRequests.Permit permit = leaderState.tryAcquirePendingRequest(request.getMessage());
      if (permit == null) {
        cacheEntry.failWithException(new ResourceUnavailableException(
            getMemberId() + ": Failed to acquire a pending write request for " + request));
        return cacheEntry.getReplyFuture();
      }
      try {
        state.appendLog(context);
      } catch (StateMachineException e) {
        // the StateMachineException is thrown by the SM in the preAppend stage.
        // Return the exception in a RaftClientReply.
        RaftClientReply exceptionReply = newExceptionReply(request, e);
        cacheEntry.failWithReply(exceptionReply);
        // leader will step down here
        if (e.leaderShouldStepDown() && getInfo().isLeader()) {
          leaderState.submitStepDownEvent(LeaderState.StepDownReason.STATE_MACHINE_EXCEPTION);
        }
        return CompletableFuture.completedFuture(exceptionReply);
      }

      // put the request into the pending queue
      pending = leaderState.addPendingRequest(permit, request, context);
      if (pending == null) {
        cacheEntry.failWithException(new ResourceUnavailableException(
            getMemberId() + ": Failed to add a pending write request for " + request));
        return cacheEntry.getReplyFuture();
      }
      leaderState.notifySenders();
    }

    return pending.getFuture();
  }

  /** Wait until the given replication requirement is satisfied. */
  private CompletableFuture<RaftClientReply> waitForReplication(RaftClientReply reply, ReplicationLevel replication) {
    if (!reply.isSuccess()) {
      return CompletableFuture.completedFuture(reply);
    }
    final RaftClientRequest.Type type = RaftClientRequest.watchRequestType(reply.getLogIndex(), replication);
    final RaftClientRequest watch = RaftClientRequest.newBuilder()
        .setServerId(reply.getServerId())
        .setClientId(reply.getClientId())
        .setGroupId(reply.getRaftGroupId())
        .setCallId(reply.getCallId())
        .setType(type)
        .build();
    return watchAsync(watch).thenApply(watchReply -> combineReplies(reply, watchReply));
  }

  private RaftClientReply combineReplies(RaftClientReply reply, RaftClientReply watchReply) {
    final RaftClientReply combinedReply = RaftClientReply.newBuilder()
        .setServerId(getMemberId())
        // from write reply
        .setClientId(reply.getClientId())
        .setCallId(reply.getCallId())
        .setMessage(reply.getMessage())
        .setLogIndex(reply.getLogIndex())
        // from watchReply
        .setSuccess(watchReply.isSuccess())
        .setException(watchReply.getException())
        .setCommitInfos(watchReply.getCommitInfos())
        .build();
    LOG.debug("combinedReply={}", combinedReply);
    return combinedReply;
  }

  void stepDownOnJvmPause() {
    role.getLeaderState().ifPresent(leader -> leader.submitStepDownEvent(LeaderState.StepDownReason.JVM_PAUSE));
  }

  private RaftClientRequest filterDataStreamRaftClientRequest(RaftClientRequest request)
      throws InvalidProtocolBufferException {
    return !request.is(TypeCase.FORWARD) ? request : ClientProtoUtils.toRaftClientRequest(
        RaftClientRequestProto.parseFrom(
            request.getMessage().getContent().asReadOnlyByteBuffer()));
  }

  <REPLY> CompletableFuture<REPLY> executeSubmitServerRequestAsync(
      CheckedSupplier<CompletableFuture<REPLY>, IOException> submitFunction) {
    return CompletableFuture.supplyAsync(
        () -> JavaUtils.callAsUnchecked(submitFunction, CompletionException::new),
        serverExecutor).join();
  }

  CompletableFuture<RaftClientReply> executeSubmitClientRequestAsync(RaftClientRequest request) {
    return CompletableFuture.supplyAsync(
        () -> JavaUtils.callAsUnchecked(() -> submitClientRequestAsync(request), CompletionException::new),
        clientExecutor).join();
  }

  @Override
  public CompletableFuture<RaftClientReply> submitClientRequestAsync(
      RaftClientRequest request) throws IOException {
    assertLifeCycleState(LifeCycle.States.RUNNING);
    LOG.debug("{}: receive client request({})", getMemberId(), request);
    final Timekeeper timer = raftServerMetrics.getClientRequestTimer(request.getType());
    final Optional<Timekeeper.Context> timerContext = Optional.ofNullable(timer).map(Timekeeper::time);
    return replyFuture(request).whenComplete((clientReply, exception) -> {
      timerContext.ifPresent(Timekeeper.Context::stop);
      if (exception != null || clientReply.getException() != null) {
        raftServerMetrics.incFailedRequestCount(request.getType());
      }
    });
  }

  private CompletableFuture<RaftClientReply> replyFuture(RaftClientRequest request) throws IOException {
    retryCache.invalidateRepliedRequests(request);

    final TypeCase type = request.getType().getTypeCase();
    switch (type) {
      case STALEREAD:
        return staleReadAsync(request);
      case READ:
        return readAsync(request);
      case WATCH:
        return watchAsync(request);
      case MESSAGESTREAM:
        return messageStreamAsync(request);
      case WRITE:
      case FORWARD:
        return writeAsync(request);
      default:
        throw new IllegalStateException("Unexpected request type: " + type + ", request=" + request);
    }
  }

  private CompletableFuture<RaftClientReply> writeAsync(RaftClientRequest request) throws IOException {
    final CompletableFuture<RaftClientReply> future = writeAsyncImpl(request);
    if (request.is(TypeCase.WRITE)) {
      // check replication
      final ReplicationLevel replication = request.getType().getWrite().getReplication();
      if (replication != ReplicationLevel.MAJORITY) {
        return future.thenCompose(r -> waitForReplication(r, replication));
      }
    }
    return future;
  }

  private CompletableFuture<RaftClientReply> writeAsyncImpl(RaftClientRequest request) throws IOException {
    final CompletableFuture<RaftClientReply> reply = checkLeaderState(request);
    if (reply != null) {
      return reply;
    }

    // query the retry cache
    final RetryCacheImpl.CacheQueryResult queryResult = retryCache.queryCache(request);
    final CacheEntry cacheEntry = queryResult.getEntry();
    if (queryResult.isRetry()) {
      // return the cached future.
      return cacheEntry.getReplyFuture();
    }
    // TODO: this client request will not be added to pending requests until
    // later which means that any failure in between will leave partial state in
    // the state machine. We should call cancelTransaction() for failed requests
    final TransactionContextImpl context = (TransactionContextImpl) stateMachine.startTransaction(
        filterDataStreamRaftClientRequest(request));
    if (context.getException() != null) {
      final StateMachineException e = new StateMachineException(getMemberId(), context.getException());
      final RaftClientReply exceptionReply = newExceptionReply(request, e);
      cacheEntry.failWithReply(exceptionReply);
      return CompletableFuture.completedFuture(exceptionReply);
    }

    return appendTransaction(request, context, cacheEntry);
  }

  private CompletableFuture<RaftClientReply> watchAsync(RaftClientRequest request) {
    final CompletableFuture<RaftClientReply> reply = checkLeaderState(request);
    if (reply != null) {
      return reply;
    }

    return role.getLeaderState()
        .map(ls -> ls.addWatchRequest(request))
        .orElseGet(() -> CompletableFuture.completedFuture(
            newExceptionReply(request, generateNotLeaderException())));
  }

  private CompletableFuture<RaftClientReply> staleReadAsync(RaftClientRequest request) {
    final long minIndex = request.getType().getStaleRead().getMinIndex();
    final long commitIndex = state.getLog().getLastCommittedIndex();
    LOG.debug("{}: minIndex={}, commitIndex={}", getMemberId(), minIndex, commitIndex);
    if (commitIndex < minIndex) {
      final StaleReadException e = new StaleReadException(
          "Unable to serve stale-read due to server commit index = " + commitIndex + " < min = " + minIndex);
      return CompletableFuture.completedFuture(
          newExceptionReply(request, new StateMachineException(getMemberId(), e)));
    }
    return processQueryFuture(stateMachine.queryStale(request.getMessage(), minIndex), request);
  }

  ReadRequests getReadRequests() {
    return getState().getReadRequests();
  }

  private CompletableFuture<ReadIndexReplyProto> sendReadIndexAsync(RaftClientRequest clientRequest) {
    final RaftPeerId leaderId = getInfo().getLeaderId();
    if (leaderId == null) {
      return JavaUtils.completeExceptionally(new ReadIndexException(getMemberId() + ": Leader is unknown."));
    }
    final ReadIndexRequestProto request = toReadIndexRequestProto(clientRequest, getMemberId(), leaderId);
    try {
      return getServerRpc().async().readIndexAsync(request);
    } catch (IOException e) {
      return JavaUtils.completeExceptionally(e);
    }
  }

  private CompletableFuture<Long> getReadIndex(RaftClientRequest request, LeaderStateImpl leader) {
    return writeIndexCache.getWriteIndexFuture(request).thenCompose(leader::getReadIndex);
  }

  private CompletableFuture<RaftClientReply> readAsync(RaftClientRequest request) {
    if (request.getType().getRead().getPreferNonLinearizable()
        || readOption == RaftServerConfigKeys.Read.Option.DEFAULT) {
      final CompletableFuture<RaftClientReply> reply = checkLeaderState(request);
       if (reply != null) {
         return reply;
       }
       return queryStateMachine(request);
    } else if (readOption == RaftServerConfigKeys.Read.Option.LINEARIZABLE){
      /*
        Linearizable read using ReadIndex. See Raft paper section 6.4.
        1. First obtain readIndex from Leader.
        2. Then waits for statemachine to advance at least as far as readIndex.
        3. Finally, query the statemachine and return the result.
       */
      final LeaderStateImpl leader = role.getLeaderState().orElse(null);

      final CompletableFuture<Long> replyFuture;
      if (leader != null) {
        replyFuture = getReadIndex(request, leader);
      } else {
        replyFuture = sendReadIndexAsync(request).thenApply(reply   -> {
          if (reply.getServerReply().getSuccess()) {
            return reply.getReadIndex();
          } else {
            throw new CompletionException(new ReadIndexException(getId() +
                ": Failed to get read index from the leader: " + reply));
          }
        });
      }

      return replyFuture
          .thenCompose(readIndex -> getReadRequests().waitToAdvance(readIndex))
          .thenCompose(readIndex -> queryStateMachine(request))
          .exceptionally(e -> readException2Reply(request, e));
    } else {
      throw new IllegalStateException("Unexpected read option: " + readOption);
    }
  }

  private RaftClientReply readException2Reply(RaftClientRequest request, Throwable e) {
    e = JavaUtils.unwrapCompletionException(e);
    if (e instanceof StateMachineException ) {
      return newExceptionReply(request, (StateMachineException) e);
    } else if (e instanceof ReadException) {
      return newExceptionReply(request, (ReadException) e);
    } else if (e instanceof ReadIndexException) {
      return newExceptionReply(request, (ReadIndexException) e);
    } else {
      throw new CompletionException(e);
    }
  }

  private CompletableFuture<RaftClientReply> messageStreamAsync(RaftClientRequest request) throws IOException {
    final CompletableFuture<RaftClientReply> reply = checkLeaderState(request);
    if (reply != null) {
      return reply;
    }

    if (request.getType().getMessageStream().getEndOfRequest()) {
      final CompletableFuture<RaftClientRequest> f = streamEndOfRequestAsync(request);
      if (f.isCompletedExceptionally()) {
        return f.thenApply(r -> null);
      }
      // the message stream has ended and the request become a WRITE request
      return replyFuture(f.join());
    }

    return role.getLeaderState()
        .map(ls -> ls.streamAsync(request))
        .orElseGet(() -> CompletableFuture.completedFuture(
            newExceptionReply(request, generateNotLeaderException())));
  }

  private CompletableFuture<RaftClientRequest> streamEndOfRequestAsync(RaftClientRequest request) {
    return role.getLeaderState()
        .map(ls -> ls.streamEndOfRequestAsync(request))
        .orElse(null);
  }

  CompletableFuture<RaftClientReply> queryStateMachine(RaftClientRequest request) {
    return processQueryFuture(stateMachine.query(request.getMessage()), request);
  }

  CompletableFuture<RaftClientReply> processQueryFuture(
      CompletableFuture<Message> queryFuture, RaftClientRequest request) {
    return queryFuture.thenApply(r -> newReplyBuilder(request).setSuccess().setMessage(r).build())
        .exceptionally(e -> {
          e = JavaUtils.unwrapCompletionException(e);
          if (e instanceof StateMachineException) {
            return newExceptionReply(request, (StateMachineException)e);
          }
          throw new CompletionException(e);
        });
  }

  @Override
  public RaftClientReply submitClientRequest(RaftClientRequest request)
      throws IOException {
    return waitForReply(request, submitClientRequestAsync(request));
  }

  RaftClientReply waitForReply(RaftClientRequest request, CompletableFuture<RaftClientReply> future)
      throws IOException {
    return waitForReply(getMemberId(), request, future, e -> newExceptionReply(request, e));
  }

  static <REPLY extends RaftClientReply> REPLY waitForReply(
      Object id, RaftClientRequest request, CompletableFuture<REPLY> future,
      Function<RaftException, REPLY> exceptionReply)
      throws IOException {
    try {
      return future.get();
    } catch (InterruptedException e) {
      final String s = id + ": Interrupted when waiting for reply, request=" + request;
      LOG.info(s, e);
      Thread.currentThread().interrupt();
      throw IOUtils.toInterruptedIOException(s, e);
    } catch (ExecutionException e) {
      final Throwable cause = e.getCause();
      if (cause == null) {
        throw new IOException(e);
      }
      if (cause instanceof NotLeaderException ||
          cause instanceof StateMachineException) {
        final REPLY reply = exceptionReply.apply((RaftException) cause);
        if (reply != null) {
          return reply;
        }
      }
      throw IOUtils.asIOException(cause);
    }
  }

  RaftClientReply transferLeadership(TransferLeadershipRequest request) throws IOException {
    return waitForReply(request, transferLeadershipAsync(request));
  }

  private CompletableFuture<RaftClientReply> logAndReturnTransferLeadershipFail(
      TransferLeadershipRequest request, String msg) {
    LOG.warn(msg);
    return CompletableFuture.completedFuture(
        newExceptionReply(request, new TransferLeadershipException(msg)));
  }

  boolean isSteppingDown() {
    return transferLeadership.isSteppingDown();
  }

  CompletableFuture<RaftClientReply> transferLeadershipAsync(TransferLeadershipRequest request)
      throws IOException {
    if (request.getNewLeader() == null) {
      return stepDownLeaderAsync(request);
    }

    LOG.info("{}: receive transferLeadership {}", getMemberId(), request);
    assertLifeCycleState(LifeCycle.States.RUNNING);
    assertGroup(getMemberId(), request);

    synchronized (this) {
      CompletableFuture<RaftClientReply> reply = checkLeaderState(request);
      if (reply != null) {
        return reply;
      }

      if (getId().equals(request.getNewLeader())) {
        return CompletableFuture.completedFuture(newSuccessReply(request));
      }

      final RaftConfigurationImpl conf = getRaftConf();
      final LeaderStateImpl leaderState = role.getLeaderStateNonNull();

      // make sure there is no raft reconfiguration in progress
      if (!conf.isStable() || leaderState.inStagingState() || !state.isConfCommitted()) {
        String msg = getMemberId() + " refused to transfer leadership to peer " + request.getNewLeader() +
            " when raft reconfiguration in progress.";
        return logAndReturnTransferLeadershipFail(request, msg);
      }

      if (!conf.containsInConf(request.getNewLeader())) {
        String msg = getMemberId() + " refused to transfer leadership to peer " + request.getNewLeader() +
            " as it is not in " + conf;
        return logAndReturnTransferLeadershipFail(request, msg);
      }

      if (!conf.isHighestPriority(request.getNewLeader())) {
        String msg = getMemberId() + " refused to transfer leadership to peer " + request.getNewLeader() +
            " as it does not has highest priority in " + conf;
        return logAndReturnTransferLeadershipFail(request, msg);
      }

      return transferLeadership.start(leaderState, request);
    }
  }

  CompletableFuture<RaftClientReply> takeSnapshotAsync(SnapshotManagementRequest request) throws IOException {
    LOG.info("{}: takeSnapshotAsync {}", getMemberId(), request);
    assertLifeCycleState(LifeCycle.States.RUNNING);
    assertGroup(getMemberId(), request);
    Objects.requireNonNull(request.getCreate(), "create == null");

    final long creationGap = request.getCreate().getCreationGap();
    long minGapValue = creationGap > 0? creationGap : RaftServerConfigKeys.Snapshot.creationGap(proxy.getProperties());
    final long lastSnapshotIndex = Optional.ofNullable(stateMachine.getLatestSnapshot())
        .map(SnapshotInfo::getIndex)
        .orElse(0L);
    if (state.getLastAppliedIndex() - lastSnapshotIndex < minGapValue) {
      return CompletableFuture.completedFuture(newSuccessReply(request, lastSnapshotIndex));
    }

    synchronized (this) {
      final long installSnapshot = snapshotInstallationHandler.getInProgressInstallSnapshotIndex();
      // check snapshot install/load
      if (installSnapshot != RaftLog.INVALID_LOG_INDEX) {
        String msg = String.format("%s: Failed do snapshot as snapshot (%s) installation is in progress",
            getMemberId(), installSnapshot);
        LOG.warn(msg);
        return CompletableFuture.completedFuture(newExceptionReply(request,new RaftException(msg)));
      }
      return snapshotRequestHandler.takingSnapshotAsync(request);
    }
  }

  SnapshotManagementRequestHandler getSnapshotRequestHandler() {
    return snapshotRequestHandler;
  }

  CompletableFuture<RaftClientReply> leaderElectionManagementAsync(LeaderElectionManagementRequest request)
      throws IOException {
    LOG.info("{} receive leaderElectionManagement request {}", getMemberId(), request);
    assertLifeCycleState(LifeCycle.States.RUNNING);
    assertGroup(getMemberId(), request);

    final LeaderElectionManagementRequest.Pause pause = request.getPause();
    if (pause != null) {
      getRole().setLeaderElectionPause(true);
      return CompletableFuture.completedFuture(newSuccessReply(request));
    }
    final LeaderElectionManagementRequest.Resume resume = request.getResume();
    if (resume != null) {
      getRole().setLeaderElectionPause(false);
      return CompletableFuture.completedFuture(newSuccessReply(request));
    }
    return JavaUtils.completeExceptionally(new UnsupportedOperationException(
        getId() + ": Request not supported " + request));
  }

  CompletableFuture<RaftClientReply> stepDownLeaderAsync(TransferLeadershipRequest request) throws IOException {
    LOG.info("{} receive stepDown leader request {}", getMemberId(), request);
    assertLifeCycleState(LifeCycle.States.RUNNING);
    assertGroup(getMemberId(), request);

    return role.getLeaderState().map(leader -> leader.submitStepDownRequestAsync(request))
        .orElseGet(() -> CompletableFuture.completedFuture(
            newExceptionReply(request, generateNotLeaderException())));
  }

  public RaftClientReply setConfiguration(SetConfigurationRequest request) throws IOException {
    return waitForReply(request, setConfigurationAsync(request));
  }

  /**
   * Handle a raft configuration change request from client.
   */
  public CompletableFuture<RaftClientReply> setConfigurationAsync(SetConfigurationRequest request) throws IOException {
    LOG.info("{}: receive setConfiguration {}", getMemberId(), request);
    assertLifeCycleState(LifeCycle.States.RUNNING);
    assertGroup(getMemberId(), request);

    CompletableFuture<RaftClientReply> reply = checkLeaderState(request);
    if (reply != null) {
      return reply;
    }

    final SetConfigurationRequest.Arguments arguments = request.getArguments();
    final PendingRequest pending;
    synchronized (this) {
      reply = checkLeaderState(request);
      if (reply != null) {
        return reply;
      }

      final RaftConfigurationImpl current = getRaftConf();
      final LeaderStateImpl leaderState = role.getLeaderStateNonNull();
      // make sure there is no other raft reconfiguration in progress
      if (!current.isStable() || leaderState.inStagingState() || !state.isConfCommitted()) {
        throw new ReconfigurationInProgressException(
            "Reconfiguration is already in progress: " + current);
      }

      final List<RaftPeer> serversInNewConf;
      final List<RaftPeer> listenersInNewConf;
      if (arguments.getMode() == SetConfigurationRequest.Mode.ADD) {
        serversInNewConf = add(RaftPeerRole.FOLLOWER, current, arguments);
        listenersInNewConf = add(RaftPeerRole.LISTENER, current, arguments);
      } else if (arguments.getMode() == SetConfigurationRequest.Mode.COMPARE_AND_SET) {
        final Comparator<RaftPeer> comparator = Comparator.comparing(RaftPeer::getId,
            Comparator.comparing(RaftPeerId::toString));
        if (CollectionUtils.equalsIgnoreOrder(arguments.getServersInCurrentConf(),
            current.getAllPeers(RaftPeerRole.FOLLOWER), comparator)
            && CollectionUtils.equalsIgnoreOrder(arguments.getListenersInCurrentConf(),
            current.getAllPeers(RaftPeerRole.LISTENER), comparator)) {
          serversInNewConf = arguments.getPeersInNewConf(RaftPeerRole.FOLLOWER);
          listenersInNewConf = arguments.getPeersInNewConf(RaftPeerRole.LISTENER);
        } else {
          throw new SetConfigurationException("Failed to set configuration: current configuration "
              + current + " is different than the request " + request);
        }
      } else {
        serversInNewConf = arguments.getPeersInNewConf(RaftPeerRole.FOLLOWER);
        listenersInNewConf = arguments.getPeersInNewConf(RaftPeerRole.LISTENER);
      }

      // return success with a null message if the new conf is the same as the current
      if (current.hasNoChange(serversInNewConf, listenersInNewConf)) {
        pending = new PendingRequest(request);
        pending.setReply(newSuccessReply(request));
        return pending.getFuture();
      }
      if (current.changeMajority(serversInNewConf)) {
        if (!memberMajorityAddEnabled) {
          throw new SetConfigurationException("Failed to set configuration: request " + request
              + " changes a majority set of the current configuration " + current);
        }
        LOG.warn("Try to add/replace a majority of servers in a single setConf: {}", request);
      }

      getRaftServer().addRaftPeers(serversInNewConf);
      getRaftServer().addRaftPeers(listenersInNewConf);
      // add staging state into the leaderState
      pending = leaderState.startSetConfiguration(request, serversInNewConf);
    }
    return pending.getFuture();
  }

  static List<RaftPeer> add(RaftPeerRole role, RaftConfigurationImpl conf, SetConfigurationRequest.Arguments args) {
    final Map<RaftPeerId, RaftPeer> inConfs = conf.getAllPeers(role).stream()
        .collect(Collectors.toMap(RaftPeer::getId, Function.identity()));

    final List<RaftPeer> toAdds = args.getPeersInNewConf(role);
    toAdds.stream().map(RaftPeer::getId).forEach(inConfs::remove);

    return Stream.concat(toAdds.stream(), inConfs.values().stream()).collect(Collectors.toList());
  }

  /**
   * The remote peer should shut down if all the following are true.
   * 1. this is the current leader
   * 2. current conf is stable and has been committed
   * 3. candidate is not in the current conf
   * 4. candidate last entry index < conf index (the candidate was removed)
   */
  private boolean shouldSendShutdown(RaftPeerId candidateId, TermIndex candidateLastEntry) {
    return getInfo().isLeader()
        && getRaftConf().isStable()
        && getState().isConfCommitted()
        && !getRaftConf().containsInConf(candidateId)
        && candidateLastEntry.getIndex() < getRaftConf().getLogEntryIndex()
        && role.getLeaderState().map(ls -> !ls.isBootStrappingPeer(candidateId)).orElse(false);
  }

  @Override
  public RequestVoteReplyProto requestVote(RequestVoteRequestProto r) throws IOException {
    final RaftRpcRequestProto request = r.getServerRequest();
    return requestVote(r.getPreVote() ? Phase.PRE_VOTE : Phase.ELECTION,
        RaftPeerId.valueOf(request.getRequestorId()),
        ProtoUtils.toRaftGroupId(request.getRaftGroupId()),
        r.getCandidateTerm(),
        TermIndex.valueOf(r.getCandidateLastEntry()));
  }

  private RequestVoteReplyProto requestVote(Phase phase,
      RaftPeerId candidateId, RaftGroupId candidateGroupId,
      long candidateTerm, TermIndex candidateLastEntry) throws IOException {
    CodeInjectionForTesting.execute(REQUEST_VOTE, getId(),
        candidateId, candidateTerm, candidateLastEntry);
    LOG.info("{}: receive requestVote({}, {}, {}, {}, {})",
        getMemberId(), phase, candidateId, candidateGroupId, candidateTerm, candidateLastEntry);
    assertLifeCycleState(LifeCycle.States.RUNNING);
    assertGroup(getMemberId(), candidateId, candidateGroupId);

    boolean shouldShutdown = false;
    final RequestVoteReplyProto reply;
    CompletableFuture<Void> future = null;
    synchronized (this) {
      // Check life cycle state again to avoid the PAUSING/PAUSED state.
      assertLifeCycleState(LifeCycle.States.RUNNING);

      final VoteContext context = new VoteContext(this, phase, candidateId);
      final RaftPeer candidate = context.recognizeCandidate(candidateTerm);
      final boolean voteGranted = context.decideVote(candidate, candidateLastEntry);
      if (candidate != null && phase == Phase.ELECTION) {
        // change server state in the ELECTION phase
        final AtomicBoolean termUpdated = new AtomicBoolean();
        future = changeToFollower(candidateTerm, true, false, "candidate:" + candidateId, termUpdated);
        if (voteGranted) {
          state.grantVote(candidate.getId());
        }
        if (termUpdated.get() || voteGranted) {
          state.persistMetadata(); // sync metafile
        }
      }
      if (voteGranted) {
        role.getFollowerState().ifPresent(fs -> fs.updateLastRpcTime(FollowerState.UpdateType.REQUEST_VOTE));
      } else if(shouldSendShutdown(candidateId, candidateLastEntry)) {
        shouldShutdown = true;
      }
      reply = toRequestVoteReplyProto(candidateId, getMemberId(),
          voteGranted, state.getCurrentTerm(), shouldShutdown, state.getLastEntry());
      if (LOG.isInfoEnabled()) {
        LOG.info("{} replies to {} vote request: {}. Peer's state: {}",
            getMemberId(), phase, toRequestVoteReplyString(reply), state);
      }
    }
    if (future != null) {
      future.join();
    }
    return reply;
  }

  @Override
  public AppendEntriesReplyProto appendEntries(AppendEntriesRequestProto r)
      throws IOException {
    try {
      return appendEntriesAsync(r).join();
    } catch (CompletionException e) {
      throw IOUtils.asIOException(JavaUtils.unwrapCompletionException(e));
    }
  }

  @Override
  public CompletableFuture<AppendEntriesReplyProto> appendEntriesAsync(AppendEntriesRequestProto r)
      throws IOException {
    final RaftRpcRequestProto request = r.getServerRequest();
    final TermIndex previous = r.hasPreviousLog()? TermIndex.valueOf(r.getPreviousLog()) : null;
    try {
      final RaftPeerId leaderId = RaftPeerId.valueOf(request.getRequestorId());
      final RaftGroupId leaderGroupId = ProtoUtils.toRaftGroupId(request.getRaftGroupId());

      CodeInjectionForTesting.execute(APPEND_ENTRIES, getId(), leaderId, previous, r);

      assertLifeCycleState(LifeCycle.States.STARTING_OR_RUNNING);
      if (!startComplete.get()) {
        throw new ServerNotReadyException(getMemberId() + ": The server role is not yet initialized.");
      }
      assertGroup(getMemberId(), leaderId, leaderGroupId);
      assertEntries(r, previous, state);

      return appendEntriesAsync(leaderId, request.getCallId(), previous, r);
    } catch(Exception t) {
      LOG.error("{}: Failed appendEntries* {}", getMemberId(),
          toAppendEntriesRequestString(r, stateMachine::toStateMachineLogEntryString), t);
      throw IOUtils.asIOException(t);
    }
  }

  @Override
  public CompletableFuture<ReadIndexReplyProto> readIndexAsync(ReadIndexRequestProto request) throws IOException {
    assertLifeCycleState(LifeCycle.States.RUNNING);

    final RaftPeerId peerId = RaftPeerId.valueOf(request.getServerRequest().getRequestorId());

    final LeaderStateImpl leader = role.getLeaderState().orElse(null);
    if (leader == null) {
      return CompletableFuture.completedFuture(toReadIndexReplyProto(peerId, getMemberId()));
    }

    return getReadIndex(ClientProtoUtils.toRaftClientRequest(request.getClientRequest()), leader)
        .thenApply(index -> toReadIndexReplyProto(peerId, getMemberId(), true, index))
        .exceptionally(throwable -> toReadIndexReplyProto(peerId, getMemberId()));
  }

  static void logAppendEntries(boolean isHeartbeat, Supplier<String> message) {
    if (isHeartbeat) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("HEARTBEAT: " + message.get());
      }
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug(message.get());
      }
    }
  }

  Optional<FollowerState> updateLastRpcTime(FollowerState.UpdateType updateType) {
    final Optional<FollowerState> fs = role.getFollowerState();
    if (fs.isPresent() && lifeCycle.getCurrentState().isRunning()) {
      fs.get().updateLastRpcTime(updateType);
      return fs;
    } else {
      return Optional.empty();
    }
  }

  private long updateCommitInfoCache() {
    return commitInfoCache.update(getId(), state.getLog().getLastCommittedIndex());
  }

  ExecutorService getServerExecutor() {
    return serverExecutor;
  }

  private CompletableFuture<AppendEntriesReplyProto> appendEntriesAsync(RaftPeerId leaderId, long callId,
      TermIndex previous, AppendEntriesRequestProto proto) throws IOException {
    final List<LogEntryProto> entries = proto.getEntriesList();
    final boolean isHeartbeat = entries.isEmpty();
    logAppendEntries(isHeartbeat, () -> getMemberId() + ": appendEntries* "
        + toAppendEntriesRequestString(proto, stateMachine::toStateMachineLogEntryString));

    final long leaderTerm = proto.getLeaderTerm();
    final long currentTerm;
    final long followerCommit = state.getLog().getLastCommittedIndex();
    final Optional<FollowerState> followerState;
    final Timekeeper.Context timer = raftServerMetrics.getFollowerAppendEntryTimer(isHeartbeat).time();
    final CompletableFuture<Void> future;
    synchronized (this) {
      // Check life cycle state again to avoid the PAUSING/PAUSED state.
      assertLifeCycleState(LifeCycle.States.STARTING_OR_RUNNING);
      currentTerm = state.getCurrentTerm();
      final boolean recognized = state.recognizeLeader(Op.APPEND_ENTRIES, leaderId, leaderTerm);
      if (!recognized) {
        return CompletableFuture.completedFuture(toAppendEntriesReplyProto(
            leaderId, getMemberId(), currentTerm, followerCommit, state.getNextIndex(),
            AppendResult.NOT_LEADER, callId, RaftLog.INVALID_LOG_INDEX, isHeartbeat));
      }
      try {
        future = changeToFollowerAndPersistMetadata(leaderTerm, true, "appendEntries");
      } catch (IOException e) {
        return JavaUtils.completeExceptionally(e);
      }
      state.setLeader(leaderId, "appendEntries");

      if (!proto.getInitializing() && lifeCycle.compareAndTransition(State.STARTING, State.RUNNING)) {
        role.startFollowerState(this, Op.APPEND_ENTRIES);
      }
      followerState = updateLastRpcTime(FollowerState.UpdateType.APPEND_START);

      // Check that the append entries are not inconsistent. There are 3
      // scenarios which can result in inconsistency:
      //      1. There is a snapshot installation in progress
      //      2. There is an overlap between the snapshot index and the entries
      //      3. There is a gap between the local log and the entries
      // In any of these scenarios, we should return an INCONSISTENCY reply
      // back to leader so that the leader can update this follower's next index.
      final long inconsistencyReplyNextIndex = checkInconsistentAppendEntries(previous, entries);
      if (inconsistencyReplyNextIndex > RaftLog.INVALID_LOG_INDEX) {
        final AppendEntriesReplyProto reply = toAppendEntriesReplyProto(
            leaderId, getMemberId(), currentTerm, followerCommit, inconsistencyReplyNextIndex,
            AppendResult.INCONSISTENCY, callId, RaftLog.INVALID_LOG_INDEX, isHeartbeat);
        LOG.info("{}: appendEntries* reply {}", getMemberId(), toAppendEntriesReplyString(reply));
        followerState.ifPresent(fs -> fs.updateLastRpcTime(FollowerState.UpdateType.APPEND_COMPLETE));
        return future.thenApply(dummy -> reply);
      }

      state.updateConfiguration(entries);
    }
    future.join();
    final CompletableFuture<Void> appendLog = entries.isEmpty()? CompletableFuture.completedFuture(null)
        : appendLog(entries);

    proto.getCommitInfosList().forEach(commitInfoCache::update);

    CodeInjectionForTesting.execute(LOG_SYNC, getId(), null);
    if (!isHeartbeat) {
      final long installedIndex = snapshotInstallationHandler.getInstalledIndex();
      if (installedIndex >= RaftLog.LEAST_VALID_LOG_INDEX) {
        LOG.info("{}: Follower has completed install the snapshot {}.", this, installedIndex);
        stateMachine.event().notifySnapshotInstalled(InstallSnapshotResult.SUCCESS, installedIndex, getPeer());
      }
    }

    final long commitIndex = effectiveCommitIndex(proto.getLeaderCommit(), previous, entries.size());
    final long matchIndex = isHeartbeat? RaftLog.INVALID_LOG_INDEX: entries.get(entries.size() - 1).getIndex();
    return appendLog.whenCompleteAsync((r, t) -> {
      followerState.ifPresent(fs -> fs.updateLastRpcTime(FollowerState.UpdateType.APPEND_COMPLETE));
      timer.stop();
    }, getServerExecutor()).thenApply(v -> {
      final boolean updated = state.updateCommitIndex(commitIndex, currentTerm, false);
      if (updated) {
        updateCommitInfoCache();
      }
      final long nextIndex = isHeartbeat? state.getNextIndex(): matchIndex + 1;
      final AppendEntriesReplyProto reply = toAppendEntriesReplyProto(leaderId, getMemberId(),
          currentTerm, updated? commitIndex : state.getLog().getLastCommittedIndex(),
          nextIndex, AppendResult.SUCCESS, callId, matchIndex, isHeartbeat);
      logAppendEntries(isHeartbeat, () -> getMemberId()
          + ": appendEntries* reply " + toAppendEntriesReplyString(reply));
      return reply;
    });
  }
  private CompletableFuture<Void> appendLog(List<LogEntryProto> entries) {
    final List<ConsecutiveIndices> entriesTermIndices = ConsecutiveIndices.convert(entries);
    if (!appendLogTermIndices.append(entriesTermIndices)) {
      // index already exists, return the last future
      return appendLogFuture.get();
    }


    return appendLogFuture.updateAndGet(f -> f.thenComposeAsync(
            ignored -> JavaUtils.allOf(state.getLog().append(entries)), serverExecutor))
        .whenComplete((v, e) -> appendLogTermIndices.removeExisting(entriesTermIndices));
  }

  private long checkInconsistentAppendEntries(TermIndex previous, List<LogEntryProto> entries) {
    // Check if a snapshot installation through state machine is in progress.
    final long installSnapshot = snapshotInstallationHandler.getInProgressInstallSnapshotIndex();
    if (installSnapshot != RaftLog.INVALID_LOG_INDEX) {
      LOG.info("{}: Failed appendEntries as snapshot ({}) installation is in progress", getMemberId(), installSnapshot);
      return state.getNextIndex();
    }

    // Check that the first log entry is greater than the snapshot index in the latest snapshot and follower's last
    // committed index. If not, reply to the leader the new next index.
    if (entries != null && !entries.isEmpty()) {
      final long firstEntryIndex = entries.get(0).getIndex();
      final long snapshotIndex = state.getSnapshotIndex();
      final long commitIndex =  state.getLog().getLastCommittedIndex();
      final long nextIndex = Math.max(snapshotIndex, commitIndex);
      if (nextIndex > RaftLog.INVALID_LOG_INDEX && nextIndex >= firstEntryIndex) {
        LOG.info("{}: Failed appendEntries as the first entry (index {})" +
                " already exists (snapshotIndex: {}, commitIndex: {})",
            getMemberId(), firstEntryIndex, snapshotIndex, commitIndex);
        return nextIndex + 1;
      }
    }

    // Check if "previous" is contained in current state.
    if (previous != null && !(appendLogTermIndices.contains(previous) || state.containsTermIndex(previous))) {
      final long replyNextIndex = Math.min(state.getNextIndex(), previous.getIndex());
      LOG.info("{}: Failed appendEntries as previous log entry ({}) is not found", getMemberId(), previous);
      return replyNextIndex;
    }

    return RaftLog.INVALID_LOG_INDEX;
  }

  @Override
  public InstallSnapshotReplyProto installSnapshot(InstallSnapshotRequestProto request) throws IOException {
    return snapshotInstallationHandler.installSnapshot(request);
  }

  boolean pause() {
    // TODO: should pause() be limited on only working for a follower?

    // Now the state of lifeCycle should be PAUSING, which will prevent future other operations.
    // Pause() should pause ongoing operations:
    //  a. call {@link StateMachine#pause()}.
    synchronized (this) {
      if (!lifeCycle.compareAndTransition(State.RUNNING, State.PAUSING)) {
        return false;
      }
      // TODO: any other operations that needs to be paused?
      stateMachine.pause();
      lifeCycle.compareAndTransition(State.PAUSING, State.PAUSED);
    }
    return true;
  }

  boolean resume() throws IOException {
    synchronized (this) {
      if (!lifeCycle.compareAndTransition(State.PAUSED, State.STARTING)) {
        return false;
      }
      // TODO: any other operations that needs to be resumed?
      try {
        stateMachine.reinitialize();
      } catch (IOException e) {
        LOG.warn("Failed to reinitialize statemachine: {}", stateMachine);
        lifeCycle.compareAndTransition(State.STARTING, State.EXCEPTION);
        throw e;
      }
      lifeCycle.compareAndTransition(State.STARTING, State.RUNNING);
    }
    return true;
  }

  @Override
  public StartLeaderElectionReplyProto startLeaderElection(StartLeaderElectionRequestProto request) throws IOException {
    final RaftRpcRequestProto r = request.getServerRequest();
    final RaftPeerId leaderId = RaftPeerId.valueOf(r.getRequestorId());
    final RaftGroupId leaderGroupId = ProtoUtils.toRaftGroupId(r.getRaftGroupId());
    CodeInjectionForTesting.execute(START_LEADER_ELECTION, getId(), leaderId, request);

    if (!request.hasLeaderLastEntry()) {
      // It should have a leaderLastEntry since there is a placeHolder entry.
      LOG.warn("{}: leaderLastEntry is missing in {}", getMemberId(), request);
      return toStartLeaderElectionReplyProto(leaderId, getMemberId(), false);
    }

    final TermIndex leaderLastEntry = TermIndex.valueOf(request.getLeaderLastEntry());
    LOG.debug("{}: receive startLeaderElection from {} with lastEntry {}", getMemberId(), leaderId, leaderLastEntry);

    assertLifeCycleState(LifeCycle.States.RUNNING);
    assertGroup(getMemberId(), leaderId, leaderGroupId);

    synchronized (this) {
      // Check life cycle state again to avoid the PAUSING/PAUSED state.
      assertLifeCycleState(LifeCycle.States.STARTING_OR_RUNNING);
      final boolean recognized = state.recognizeLeader("startLeaderElection", leaderId, leaderLastEntry.getTerm());
      if (!recognized) {
        return toStartLeaderElectionReplyProto(leaderId, getMemberId(), false);
      }

      if (!getInfo().isFollower()) {
        LOG.warn("{} refused StartLeaderElectionRequest from {}, because role is:{}",
            getMemberId(), leaderId, role.getCurrentRole());
        return toStartLeaderElectionReplyProto(leaderId, getMemberId(), false);
      }

      if (ServerState.compareLog(state.getLastEntry(), leaderLastEntry) < 0) {
        LOG.warn("{} refused StartLeaderElectionRequest from {}, because lastEntry:{} less than leaderEntry:{}",
            getMemberId(), leaderId, leaderLastEntry, state.getLastEntry());
        return toStartLeaderElectionReplyProto(leaderId, getMemberId(), false);
      }

      changeToCandidate(true);
      return toStartLeaderElectionReplyProto(leaderId, getMemberId(), true);
    }
  }

  void submitUpdateCommitEvent() {
    role.getLeaderState().ifPresent(LeaderStateImpl::submitUpdateCommitEvent);
  }

  /**
   * The log has been submitted to the state machine. Use the future to update
   * the pending requests and retry cache.
   * @param stateMachineFuture the future returned by the state machine
   *                           from which we will get transaction result later
   */
  private CompletableFuture<Message> replyPendingRequest(
      ClientInvocationId invocationId, TermIndex termIndex, CompletableFuture<Message> stateMachineFuture) {
    // update the retry cache
    final CacheEntry cacheEntry = retryCache.getOrCreateEntry(invocationId);
    Objects.requireNonNull(cacheEntry , "cacheEntry == null");
    if (getInfo().isLeader() && cacheEntry.isCompletedNormally()) {
      LOG.warn("{} retry cache entry of leader should be pending: {}", this, cacheEntry);
    }
    if (cacheEntry.isFailed()) {
      retryCache.refreshEntry(new CacheEntry(cacheEntry.getKey()));
    }

    return stateMachineFuture.whenComplete((reply, exception) -> {
      getTransactionManager().remove(termIndex);
      final RaftClientReply.Builder b = newReplyBuilder(invocationId, termIndex.getIndex());
      final RaftClientReply r;
      if (exception == null) {
        r = b.setSuccess().setMessage(reply).build();
      } else {
        // the exception is coming from the state machine. wrap it into the
        // reply as a StateMachineException
        final StateMachineException e = new StateMachineException(getMemberId(), exception);
        r = b.setException(e).build();
      }

      // update pending request
      role.getLeaderState().ifPresent(leader -> leader.replyPendingRequest(termIndex, r));
      cacheEntry.updateResult(r);
    });
  }

  TransactionManager getTransactionManager() {
    return transactionManager;
  }

  @VisibleForTesting
  Map<TermIndex, MemoizedSupplier<TransactionContext>> getTransactionContextMapForTesting() {
    return getTransactionManager().getMap();
  }

  TransactionContext getTransactionContext(LogEntryProto entry, Boolean createNew) {
    if (!entry.hasStateMachineLogEntry()) {
      return null;
    }

    final TermIndex termIndex = TermIndex.valueOf(entry);
    final Optional<LeaderStateImpl> leader = getRole().getLeaderState();
    if (leader.isPresent()) {
      final TransactionContext context = leader.get().getTransactionContext(termIndex);
      if (context != null) {
        return context;
      }
    }

    if (!createNew) {
      return getTransactionManager().get(termIndex);
    }
    return getTransactionManager().computeIfAbsent(termIndex,
        // call startTransaction only once
        MemoizedSupplier.valueOf(() -> stateMachine.startTransaction(entry, getInfo().getCurrentRole())));
  }

  CompletableFuture<Message> applyLogToStateMachine(LogEntryProto next) throws RaftLogIOException {
    CompletableFuture<Message> messageFuture = null;

    switch (next.getLogEntryBodyCase()) {
    case CONFIGURATIONENTRY:
      // the reply should have already been set. only need to record
      // the new conf in the metadata file and notify the StateMachine.
      state.writeRaftConfiguration(next);
      stateMachine.event().notifyConfigurationChanged(next.getTerm(), next.getIndex(),
          next.getConfigurationEntry());
      role.getLeaderState().ifPresent(leader -> leader.checkReady(next));
      break;
    case STATEMACHINELOGENTRY:
      TransactionContext trx = getTransactionContext(next, true);
      Objects.requireNonNull(trx, "trx == null");
      final ClientInvocationId invocationId = ClientInvocationId.valueOf(next.getStateMachineLogEntry());
      writeIndexCache.add(invocationId.getClientId(), ((TransactionContextImpl) trx).getLogIndexFuture());

      try {
        // Let the StateMachine inject logic for committed transactions in sequential order.
        trx = stateMachine.applyTransactionSerial(trx);

        final CompletableFuture<Message> stateMachineFuture = stateMachine.applyTransaction(trx);
        messageFuture = replyPendingRequest(invocationId, TermIndex.valueOf(next), stateMachineFuture);
      } catch (Exception e) {
        throw new RaftLogIOException(e);
      }
      break;
    case METADATAENTRY:
      break;
    default:
      throw new IllegalStateException("Unexpected LogEntryBodyCase " + next.getLogEntryBodyCase() + ", next=" + next);
    }

    if (next.getLogEntryBodyCase() != LogEntryProto.LogEntryBodyCase.STATEMACHINELOGENTRY) {
      stateMachine.event().notifyTermIndexUpdated(next.getTerm(), next.getIndex());
    }
    return messageFuture;
  }

  /**
   * The given log entry is being truncated.
   * Fail the corresponding client request, if there is any.
   *
   * @param logEntry the log entry being truncated
   */
  void notifyTruncatedLogEntry(LogEntryProto logEntry) {
    Optional.ofNullable(getState()).ifPresent(s -> s.truncate(logEntry.getIndex()));
    if (logEntry.hasStateMachineLogEntry()) {
      getTransactionManager().remove(TermIndex.valueOf(logEntry));

      final ClientInvocationId invocationId = ClientInvocationId.valueOf(logEntry.getStateMachineLogEntry());
      final CacheEntry cacheEntry = getRetryCache().getIfPresent(invocationId);
      if (cacheEntry != null) {
        cacheEntry.failWithReply(newReplyBuilder(invocationId, logEntry.getIndex())
            .setException(generateNotLeaderException())
            .build());
      }
    }
  }

  LeaderElectionMetrics getLeaderElectionMetrics() {
    return leaderElectionMetrics;
  }

  @Override
  public RaftServerMetricsImpl getRaftServerMetrics() {
    return raftServerMetrics;
  }

  void onGroupLeaderElected() {
    transferLeadership.complete(TransferLeadership.Result.SUCCESS);
  }

  boolean isRunning() {
    return startComplete.get() && lifeCycle.getCurrentState() == State.RUNNING;
  }
}
