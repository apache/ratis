package org.apache.ratis.server.impl;

import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.util.Timestamp;

import java.util.List;
import java.util.SortedMap;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

public class ReadOnlyRequests {
    private final StateMachine stateMachine;
    private final ReadIndexQueue readIndexQueue;

    public ReadOnlyRequests(StateMachine stateMachine) {
        this.stateMachine = stateMachine;
        this.readIndexQueue = new ReadIndexQueue();
    }

    public static class ReadIndexHeartbeatWatcher implements Consumer<RaftProtos.AppendEntriesReplyProto> {
        private final int confPeerCount;
        private final CompletableFuture<Boolean> result;
        private volatile int successCount;
        private volatile int failCount;
        private volatile boolean done;

        public ReadIndexHeartbeatWatcher(int confPeers) {
            this.confPeerCount = confPeers;
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
                result.completeExceptionally(null);
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
        private final CompletableFuture<Long> future;
        private final long readIndex;

        PendingReadIndex(long readIndex) {
            this.readIndex = readIndex;
            future = new CompletableFuture<>();
        }

        public CompletableFuture<Long> getFuture() {
            return future;
        }
    }

    private class ReadIndexQueue {
        private  SortedMap<Long, List<PendingReadIndex>> q;

        public CompletableFuture<Message> addRequest(PendingReadIndex readIndex) {
            return null;
        }
    }

    CompletableFuture<Message> add(long readIndex) {
        CompletableFuture<Message> future = new CompletableFuture<Message>();
        future.completeExceptionally(new Exception("FUCK"));
        return future;
    }
}
