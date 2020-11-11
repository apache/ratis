package org.apache.ratis;

import static org.apache.ratis.DataStreamTestUtils.initBuffer;
import static org.apache.ratis.RaftTestUtil.waitForLeader;

import org.apache.ratis.client.DataStreamOutputRpc;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.proto.RaftProtos.StateMachineLogEntryProto;
import org.apache.ratis.protocol.DataStreamReply;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.server.impl.RaftServerImpl;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.raftlog.RaftLog;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;

public abstract class DataStreamTests <CLUSTER extends MiniRaftCluster> extends BaseTest
        implements MiniRaftCluster.Factory.Get<CLUSTER> {

    public static final int NUM_SERVERS = 3;
    // TODO: change bufferSize and bufferNum configurable
    private static int bufferSize = 1_000_000;
    private static int bufferNum =  10;

    @Test
    public void testStreamWrites() throws Exception {
        runWithNewCluster(NUM_SERVERS, this::testStreamWrites);
    }

    void testStreamWrites(CLUSTER cluster) throws Exception {
        final RaftServerImpl leader = waitForLeader(cluster);
        Assert.assertTrue(leader != null);

        final RaftGroup raftGroup = cluster.getGroup();
        final Collection<RaftPeer> peers = raftGroup.getPeers();
        Assert.assertEquals(NUM_SERVERS, peers.size());
        RaftPeer raftPeer = peers.iterator().next();

        try (RaftClient client = cluster.createClient(raftPeer)) {
            // send header
            DataStreamOutputRpc dataStreamOutputRpc = client.getDataStreamApi().stream();
            final List<CompletableFuture<DataStreamReply>> futures = new ArrayList<>();
            final List<Integer> sizes = new ArrayList<>();

            dataStreamOutputRpc.getHeaderFuture().join();

            // send data
            final int halfBufferSize = bufferSize / 2;
            int dataSize = 0;
            for(int i = 0; i < bufferNum; i++) {
                final int size = halfBufferSize + ThreadLocalRandom.current().nextInt(halfBufferSize);
                sizes.add(size);

                final ByteBuffer bf = initBuffer(dataSize, size);
                futures.add(dataStreamOutputRpc.writeAsync(bf));
                dataSize += size;
            }

            for (int i = 0; i < futures.size(); i++) {
                futures.get(i).join();
            }

            // send close
            dataStreamOutputRpc.closeAsync().join();

            // send start transaction
            dataStreamOutputRpc.startTransactionAsync().join();

            // get request call id
            long callId = dataStreamOutputRpc.getHeaderFuture().get().getStreamId();

            // verify the write request is in the Raft log.
            RaftLog log = leader.getState().getLog();
            boolean transactionFound = false;
            for (TermIndex termIndex : log.getEntries(0, Long.MAX_VALUE)) {
                RaftProtos.LogEntryProto entryProto = log.get(termIndex.getIndex());
                if (entryProto.hasStateMachineLogEntry()) {
                    StateMachineLogEntryProto stateMachineEntryProto = entryProto.getStateMachineLogEntry();
                    if (stateMachineEntryProto.getCallId() == callId) {
                        transactionFound = true;
                        break;
                    }
                }
            }
            Assert.assertTrue(transactionFound);
        }
    }
}
