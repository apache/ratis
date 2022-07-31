package org.apache.ratis;

import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.RaftClientConfigKeys;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.retry.RetryPolicies;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.impl.MiniRaftCluster;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.TimeDuration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public abstract class ReadIndexRequestTests<CLUSTER extends MiniRaftCluster>
    extends BaseTest
    implements MiniRaftCluster.Factory.Get<CLUSTER> {


    static final int NUM_SERVERS = 3;

    @Before
    public void setup() {
        final RaftProperties p = getProperties();
        p.setClass(MiniRaftCluster.STATEMACHINE_CLASS_KEY,
                VersionedStateMachine.class, StateMachine.class);
        RaftServerConfigKeys.Rpc.setTimeoutMin(properties.get(), TimeDuration.valueOf(10, TimeUnit.SECONDS));
        RaftServerConfigKeys.Rpc.setTimeoutMax(properties.get(), TimeDuration.valueOf(20, TimeUnit.SECONDS));
        RaftServerConfigKeys.Rpc.setRequestTimeout(properties.get(), TimeDuration.valueOf(50, TimeUnit.SECONDS));
        RaftClientConfigKeys.Rpc.setRequestTimeout(properties.get(), TimeDuration.valueOf(50, TimeUnit.SECONDS));
    }

    @Test
    public void testReadIndex() throws Exception {
        runWithNewCluster(NUM_SERVERS, this::testReadIndexImpl);
    }

    private void testReadIndexImpl(CLUSTER cluster) throws Exception {
        try {
            RaftTestUtil.waitForLeader(cluster);
            final RaftPeerId leaderId = cluster.getLeader().getId();
            try (final RaftClient client = cluster.createClient(leaderId, RetryPolicies.noRetry())) {
                for (int i = 0; i < 10; i++) {
                    RaftClientReply reply =
                            client.io().send(new RaftTestUtil.SimpleMessage("a=" + i));
                    Assert.assertTrue(reply.isSuccess());
                }

                System.out.println("FUCK");

                RaftClientReply reply = client.io().sendReadIndex(
                        new RaftTestUtil.SimpleMessage("a"),
                        RaftProtos.ReadOnlyOption.ReadOnlySafe,
                        leaderId);

                Assert.assertEquals(reply.getMessage().getContent().toString(StandardCharsets.UTF_8), "9");
            }
        } finally {
            cluster.shutdown();
        }
    }

    static class VersionedStateMachine extends BaseStateMachine {
        private Map<String, Integer> data = new ConcurrentHashMap<>();

        @Override
        public CompletableFuture<Message> query(Message request) {
            return CompletableFuture.completedFuture(
                    Message.valueOf(Integer.toString(
                            data.get(request.getContent().toString(StandardCharsets.UTF_8)))));
        }

        @Override
        public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
            System.out.println("index is " + trx.getLogEntry().getIndex());
            updateLastAppliedTermIndex(trx.getLogEntry().getTerm(), trx.getLogEntry().getIndex());

            String content = trx.getLogEntry().getStateMachineLogEntry().getLogData().toString(StandardCharsets.UTF_8);
            String[] kv = content.split("=");
            data.put(kv[0], Integer.parseInt(kv[1]));
            return CompletableFuture.completedFuture(
                    Message.valueOf(trx.getLogEntry().getIndex() + " OK"));
        }
    }
}

