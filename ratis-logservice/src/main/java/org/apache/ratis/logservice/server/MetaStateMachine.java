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

package org.apache.ratis.logservice.server;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.logservice.api.LogInfo;
import org.apache.ratis.logservice.api.LogName;
import org.apache.ratis.logservice.common.LogAlreadyExistException;
import org.apache.ratis.logservice.common.LogNotFoundException;
import org.apache.ratis.logservice.common.NoEnoughWorkersException;
import org.apache.ratis.logservice.proto.MetaServiceProtos;
import org.apache.ratis.logservice.proto.MetaServiceProtos.ArchiveLogReplyProto;
import org.apache.ratis.logservice.proto.MetaServiceProtos.ArchiveLogRequestProto;
import org.apache.ratis.logservice.proto.MetaServiceProtos.CreateLogRequestProto;
import org.apache.ratis.logservice.proto.MetaServiceProtos.DeleteLogRequestProto;
import org.apache.ratis.logservice.proto.MetaServiceProtos.LogServicePingRequestProto;
import org.apache.ratis.logservice.proto.MetaServiceProtos.LogServiceRegisterLogRequestProto;
import org.apache.ratis.logservice.proto.MetaServiceProtos.LogServiceUnregisterLogRequestProto;
import org.apache.ratis.logservice.proto.MetaServiceProtos.MetaSMRequestProto;
import org.apache.ratis.logservice.util.LogServiceProtoUtil;
import org.apache.ratis.logservice.util.MetaServiceProtoUtil;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.ratis.util.AutoCloseableLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * State Machine serving meta data for LogService. It persists the pairs 'log name' -> RaftGroup
 * During the start basing on the persisted data it would be able to build a list of the existing servers.
 * Requests from clients for DDL operations are handled by query mechanism (so only the leader accept that.
 * It performs the operation (such as Log creation) and sends a message with the log -> group pair to itself
 * to persis this data internally and on followers.
 */

public class MetaStateMachine extends BaseStateMachine {

    Logger LOG = LoggerFactory.getLogger(MetaStateMachine.class);


    //Persisted map between log and RaftGroup
    private Map<LogName, RaftGroup> map = new ConcurrentHashMap<>();
    // List of the currently known peers.
    private final Set<RaftPeer> peers = new HashSet<>();

    // keep a copy of raftServer to get group information.
    private RaftServer raftServer;


    private RaftGroup currentGroup = null;

    // MinHeap queue for load balancing groups across the peers
    private PriorityBlockingQueue<PeerGroups> avail = new PriorityBlockingQueue<PeerGroups>();

    //Properties
    private RaftProperties properties = new RaftProperties();

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);

    private RaftGroupId metadataGroupId;
    private RaftGroupId logServerGroupId;

    public MetaStateMachine(RaftGroupId metadataGroupId, RaftGroupId logServerGroupId) {
      this.metadataGroupId = metadataGroupId;
      this.logServerGroupId = logServerGroupId;
    }

    @Override
    public void initialize(RaftServer server, RaftGroupId groupId, RaftStorage storage) throws IOException {
        this.raftServer = server;
        super.initialize(server, groupId, storage);
    }

    @Override
    public TransactionContext applyTransactionSerial(TransactionContext trx) {
        RaftProtos.LogEntryProto x = trx.getLogEntry();
        MetaSMRequestProto req = null;
        try {
            req = MetaSMRequestProto.parseFrom(x.getStateMachineLogEntry().getLogData());
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
        switch (req.getTypeCase()) {
            case REGISTERREQUEST:
                LogServiceRegisterLogRequestProto r = req.getRegisterRequest();
                LogName logname = LogServiceProtoUtil.toLogName(r.getLogname());
                RaftGroup rg = MetaServiceProtoUtil.toRaftGroup(r.getRaftGroup());
                map.put(logname, rg);
                LOG.info("Log {} registered at {} with group {} ", logname, getId(), rg );
                break;
            case UNREGISTERREQUEST:
                LogServiceUnregisterLogRequestProto unregReq = req.getUnregisterRequest();
                logname = LogServiceProtoUtil.toLogName(unregReq.getLogname());
                map.remove(logname);
                break;
            case PINGREQUEST:
                LogServicePingRequestProto pingRequest = req.getPingRequest();
                RaftPeer peer = MetaServiceProtoUtil.toRaftPeer(pingRequest.getPeer());
                if (peers.contains(peer)) {
                    //Do Nothing, that's just heartbeat
                } else {
                    peers.add(peer);
                    avail.add(new PeerGroups(peer));
                }
                break;

            default:
        }
        return super.applyTransactionSerial(trx);
    }

    @Override
    public TransactionContext startTransaction(RaftClientRequest request) throws IOException {
        return super.startTransaction(request);
    }

    @Override
    public TransactionContext preAppendTransaction(TransactionContext trx) throws IOException {
        return super.preAppendTransaction(trx);
    }

    @Override
    public CompletableFuture<Message> queryStale(Message request, long minIndex) {
        return super.queryStale(request, minIndex);
    }

    @Override
    public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
        return super.applyTransaction(trx);
    }

    @Override
    public CompletableFuture<Message> query(Message request) {
        if (currentGroup == null) {
            try {
                List<RaftGroup> x = StreamSupport.stream(raftServer.getGroups().spliterator(), false)
                    .filter(group -> group.getGroupId().equals(metadataGroupId))
                    .collect(Collectors.toList());
                if (x.size() == 1) {
                    currentGroup = x.get(0);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        RaftProperties properties = new RaftProperties();
        MetaServiceProtos.MetaServiceRequestProto req = null;
        try {
            req =  MetaServiceProtos.MetaServiceRequestProto.parseFrom(request.getContent());
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
        MetaServiceProtos.MetaServiceRequestProto.TypeCase type = req.getTypeCase();
        switch (type) {

            case CREATELOG:
                return processCreateLogRequest(req);
            case LISTLOGS:
                return processListLogsRequest();
            case GETLOG:
                return processGetLogRequest(req);
            case ARCHIVELOG:
                return processArchiveLog(req);
            case DELETELOG:
                return processDeleteLog(req);
                default:
        }
        CompletableFuture<Message> reply = super.query(request);
        return reply;
    }



    private CompletableFuture<Message>
    processDeleteLog(MetaServiceProtos.MetaServiceRequestProto logServiceRequestProto) {
        DeleteLogRequestProto deleteLog = logServiceRequestProto.getDeleteLog();
        LogName logName = LogServiceProtoUtil.toLogName(deleteLog.getLogName());
        RaftGroup raftGroup = map.get(logName);
        if (raftGroup == null) {
            return CompletableFuture.completedFuture(Message.valueOf(
                    MetaServiceProtoUtil.toDeleteLogExceptionReplyProto(
                            new LogNotFoundException(logName.getName())).build().toByteString()));
        } else {
            Collection<RaftPeer> peers = raftGroup.getPeers();
            peers.stream().forEach(peer -> {
                RaftClient client = RaftClient.newBuilder()
                        .setProperties(properties)
                        .setClientId(ClientId.randomId())
                        .setRaftGroup(RaftGroup.valueOf(logServerGroupId, peer))
                        .build();
                try {
                    client.groupRemove(raftGroup.getGroupId(), true, peer.getId());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
            RaftClient client = RaftClient.newBuilder()
                    .setRaftGroup(currentGroup)
                    .setClientId(ClientId.randomId())
                    .setProperties(properties)
                    .build();
            try {
                client.send(() -> MetaServiceProtos.MetaSMRequestProto.newBuilder()
                        .setUnregisterRequest(
                                LogServiceUnregisterLogRequestProto.newBuilder()
                                        .setLogname(LogServiceProtoUtil.toLogNameProto(logName)))
                        .build().toByteString());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return CompletableFuture.completedFuture(Message.valueOf(
                MetaServiceProtoUtil.toDeleteLogReplyProto().toByteString()));
    }

//    private CompletableFuture<Message> processCloseLog(MetaServiceProtos.MetaServiceRequestProto logServiceRequestProto) {
//        CloseLogRequestProto closeLog = logServiceRequestProto.getCloseLog();
//        LogName logName = LogServiceProtoUtil.toLogName(closeLog.getLogName());
//        // Need to check whether the file is opened if opened close it.
//        // TODO need to handle exceptions while operating with files.
//        return CompletableFuture.completedFuture(Message
//                .valueOf(CloseLogReplyProto.newBuilder().build().toByteString()));
//    }

    private CompletableFuture<Message>
    processArchiveLog(MetaServiceProtos.MetaServiceRequestProto logServiceRequestProto) {
        ArchiveLogRequestProto archiveLog = logServiceRequestProto.getArchiveLog();
        LogName logName = LogServiceProtoUtil.toLogName(archiveLog.getLogName());
        // Handle log archiving.
        return CompletableFuture.completedFuture(Message
                .valueOf(ArchiveLogReplyProto.newBuilder().build().toByteString()));
    }

//    private CompletableFuture<Message> processGetStateRequest(
//            MetaServiceProtos.MetaServiceRequestProto logServiceRequestProto) {
//        MetaServiceProtos.GetStateRequestProto getState = logServiceRequestProto.getGetState();
//        LogName logName = LogServiceProtoUtil.toLogName(getState.getLogName());
//        return CompletableFuture.completedFuture(Message.valueOf(LogServiceProtoUtil
//                .toGetStateReplyProto(true).toByteString()));
//    }
//
    private CompletableFuture<Message> processCreateLogRequest(
            MetaServiceProtos.MetaServiceRequestProto logServiceRequestProto) {
        LogName name;
        try (final AutoCloseableLock writeLock = writeLock()) {
            CreateLogRequestProto createLog = logServiceRequestProto.getCreateLog();
            name = LogServiceProtoUtil.toLogName(createLog.getLogName());
            if(map.containsKey(name)) {
                return CompletableFuture.completedFuture(Message.valueOf(MetaServiceProtoUtil
                        .toCreateLogExceptionReplyProto(new LogAlreadyExistException(name.getName()))
                        .build()
                        .toByteString()));
            }
            // Check that we have at least 3 nodes
            if (avail.size() < 3) {
                return CompletableFuture.completedFuture(Message.valueOf(MetaServiceProtoUtil
                .toCreateLogExceptionReplyProto(new NoEnoughWorkersException(avail.size()))
                        .build()
                        .toByteString()));
            } else {
                List<PeerGroups> peerGroup = IntStream.range(0, 3).mapToObj(i -> avail.poll()).collect(Collectors.toList());
                List<RaftPeer> peers = peerGroup.stream().map(obj -> obj.getPeer()).collect(Collectors.toList());
                RaftGroup raftGroup = RaftGroup.valueOf(RaftGroupId.randomId(), peers);
                peerGroup.stream().forEach(pg -> {
                    pg.getGroups().add(raftGroup);
                    avail.add(pg);
                });
                peers.forEach(i -> {
                    RaftClient client = RaftClient.newBuilder().setProperties(properties)
                      .setRaftGroup(RaftGroup.valueOf(logServerGroupId, i)).build();
                    try {
                        client.groupAdd(raftGroup, i.getId());
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
                RaftClient client = RaftClient.newBuilder()
                        .setRaftGroup(currentGroup)
                        .setClientId(ClientId.randomId())
                        .setProperties(properties)
                        .build();
                try {
                    client.send(() -> MetaServiceProtos.MetaSMRequestProto.newBuilder()
                            .setRegisterRequest(LogServiceRegisterLogRequestProto
                                    .newBuilder()
                                    .setLogname(LogServiceProtoUtil.toLogNameProto(name))
                                    .setRaftGroup(MetaServiceProtoUtil
                                            .toRaftGroupProto(raftGroup)))
                            .build().toByteString());
                } catch (IOException e) {
                    e.printStackTrace();
                }
                return CompletableFuture.completedFuture(Message.valueOf(MetaServiceProtoUtil
                        .toCreateLogReplyProto(new LogInfo((name), raftGroup)).build().toByteString()));
            }
        }
    }


    private AutoCloseableLock writeLock() {
        return AutoCloseableLock.acquire(lock.writeLock());
    }

    private CompletableFuture<Message> processListLogsRequest() {
        return CompletableFuture.completedFuture(Message.valueOf(MetaServiceProtoUtil
                .toListLogLogsReplyProto(
                        map.entrySet()
                                .stream()
                                .map(log -> new LogInfo(log.getKey(), log.getValue()))
                                .collect(Collectors.toList())).toByteString()));
    }

    private CompletableFuture<Message> processGetLogRequest(
            MetaServiceProtos.MetaServiceRequestProto logServiceRequestProto) {
        MetaServiceProtos.GetLogRequestProto getLog = logServiceRequestProto.getGetLog();
        LogName logName = LogServiceProtoUtil.toLogName(getLog.getLogName());
        RaftGroup raftGroup = map.get(logName);
        if (raftGroup != null) {
            return CompletableFuture.completedFuture(Message.valueOf(
                    MetaServiceProtoUtil.toGetLogReplyProto(new LogInfo(logName, raftGroup))
                            .toByteString()));
        } else {
            return CompletableFuture.completedFuture(Message.valueOf(
                    MetaServiceProtoUtil.toGetLogExceptionReplyProto(
                            new LogNotFoundException(logName.getName())).build().toByteString()));
        }
    }


    class PeerGroups implements Comparable{
        RaftPeer peer;
        Set<RaftGroup> groups = new HashSet<>();

        public PeerGroups(RaftPeer peer) {
            this.peer = peer;

        }

        public Set<RaftGroup> getGroups () {
            return groups;
        }

        public RaftPeer getPeer() {
            return peer;
        }

        @Override
        public int compareTo(Object o) {
            return groups.size() - ((PeerGroups) o).groups.size();
        }
    }
}
