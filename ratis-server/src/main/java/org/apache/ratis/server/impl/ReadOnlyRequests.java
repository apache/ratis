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

import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.exceptions.ResourceUnavailableException;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.TimeDuration;
import org.apache.ratis.util.TimeoutScheduler;
import org.apache.ratis.util.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class ReadOnlyRequests {
    private static final Logger LOG = LoggerFactory.getLogger(ReadOnlyRequests.class);
    private final StateMachine stateMachine;
    private final ReadIndexQueue readIndexQueue;

    private final TimeDuration readOnlyTimeoutNanos;
    private final TimeoutScheduler scheduler = TimeoutScheduler.getInstance();

    public ReadOnlyRequests(StateMachine stateMachine, RaftProperties properties) {
        this.stateMachine = stateMachine;
        this.readIndexQueue = new ReadIndexQueue();
        final TimeDuration readOnlyTimeout = RaftServerConfigKeys.ReadOnly.timeout(properties);
        this.readOnlyTimeoutNanos = readOnlyTimeout.to(TimeUnit.NANOSECONDS);
    }

    public Consumer<Long> getAppliedIndexListener() {
        return this.readIndexQueue;
    }

    public static class ReadIndexHeartbeatWatcher implements Consumer<RaftProtos.AppendEntriesReplyProto> {
        private final RaftServerImpl server;
        private final int confPeerCount;
        private final CompletableFuture<Boolean> result;
        private int successCount;
        private int failCount;
        private volatile boolean done;

        public ReadIndexHeartbeatWatcher(RaftServerImpl server) {
            this.server = server;
            this.confPeerCount = server.getGroup().getPeers().size();
            this.successCount = 0;
            this.failCount = 0;
            this.done = false;
            this.result = new CompletableFuture<>();
        }

        @Override
        public synchronized void accept(RaftProtos.AppendEntriesReplyProto reply) {
            if (done) {
                return;
            }
            if (reply.getResult() == RaftProtos.AppendEntriesReplyProto.AppendResult.SUCCESS) {
                successCount++;
            } else {
                failCount++;
            }

            if (hasMajorityAck()) {
                result.complete(true);
                done = true;
            }
            if (hasMajorityFail()) {
                result.completeExceptionally(server.generateNotLeaderException());
                done = true;
            }
        }

        public CompletableFuture<Boolean> getFuture() {
            return result;
        }

        private synchronized boolean hasMajorityAck() {
            return successCount + 1 > confPeerCount / 2;
        }

        private synchronized boolean hasMajorityFail() {
            return failCount > confPeerCount / 2;
        }
    }

    static class PendingReadIndex {
        private final Supplier<CompletableFuture<Long>> future = JavaUtils.memoize(CompletableFuture::new);
        private final Timestamp creationTime;
        private final long readIndex;

        PendingReadIndex(long readIndex, Timestamp creationTime) {
            this.readIndex = readIndex;
            this.creationTime = creationTime;
        }

        public CompletableFuture<Long> getFuture() {
            return future.get();
        }
    }

    private class ReadIndexQueue implements Consumer<Long>{
        private  SortedMap<Long, List<PendingReadIndex>> q;

        public ReadIndexQueue() {
            this.q = new TreeMap<>();
        }

        public CompletableFuture<Long> addPendingReadIndex(long readIndex) {
            final long currentTime = Timestamp.currentTimeNanos();
            PendingReadIndex pendingReadIndex = new PendingReadIndex(readIndex, Timestamp.valueOf(currentTime));
            synchronized (this) {
                q.putIfAbsent(readIndex, new ArrayList<>(10));
                q.get(readIndex).add(pendingReadIndex);
            }

            scheduler.onTimeout(readOnlyTimeoutNanos, () -> handleTimeout(readIndex),
                    LOG, () -> "read only request timeout on " + readIndex);
            return pendingReadIndex.getFuture();
        }

        void handleTimeout(long readIndex) {
            List<PendingReadIndex> pendingReadIndexList = removePendingRequestsOn(readIndex);
            pendingReadIndexList.forEach(pendingReadIndex -> {
                // TODO add a proper exception
                pendingReadIndex.getFuture().completeExceptionally(new ResourceUnavailableException(""));
            });
        }

        synchronized List<PendingReadIndex> removePendingRequestsOn(long readIndex) {
            return q.remove(readIndex);
        }

        @Override
        public synchronized void accept(Long appliedIndex) {
            Optional.ofNullable(q.get(appliedIndex)).ifPresent(
                    list -> list.forEach(pi -> pi.getFuture().complete(appliedIndex)));
            q.remove(appliedIndex);
        }
    }

    CompletableFuture<Long> add(long readIndex) {
        if (stateMachine.getLastAppliedTermIndex().getIndex() >= readIndex) {
            return CompletableFuture.completedFuture(readIndex);
        }
        return readIndexQueue.addPendingReadIndex(readIndex);
    }
}
