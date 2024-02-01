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
package org.apache.ratis.datastream;

import org.apache.ratis.client.RaftClientConfigKeys;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.netty.NettyConfigKeys;
import org.apache.ratis.util.SizeInBytes;
import org.apache.ratis.util.TimeDuration;
import org.junit.jupiter.api.BeforeEach;

public class TestNettyDataStreamChainTopologyWithGrpcCluster
    extends DataStreamAsyncClusterTests<MiniRaftClusterWithRpcTypeGrpcAndDataStreamTypeNetty>
    implements MiniRaftClusterWithRpcTypeGrpcAndDataStreamTypeNetty.FactoryGet {

  @BeforeEach
  public void setup() {
    final RaftProperties p = getProperties();
    RaftClientConfigKeys.DataStream.setRequestTimeout(p, TimeDuration.ONE_MINUTE);
    RaftClientConfigKeys.DataStream.setFlushRequestCountMin(p, 4);
    RaftClientConfigKeys.DataStream.setFlushRequestBytesMin(p, SizeInBytes.valueOf("10MB"));
    RaftClientConfigKeys.DataStream.setOutstandingRequestsMax(p, 2 << 16);

    NettyConfigKeys.DataStream.Client.setWorkerGroupSize(p,100);
  }
}
