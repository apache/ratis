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
package org.apache.ratis.grpc;

import org.apache.log4j.Level;
import org.apache.ratis.OutputStreamBaseTest;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.client.GrpcClientStreamer;
import org.apache.ratis.grpc.client.GrpcOutputStream;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.util.Log4jUtils;
import org.apache.ratis.util.SizeInBytes;

import java.io.OutputStream;

/**
 * Test {@link GrpcOutputStream}
 */
public class TestGrpcOutputStream
    extends OutputStreamBaseTest<MiniRaftClusterWithGrpc>
    implements MiniRaftClusterWithGrpc.FactoryGet {
  static {
    Log4jUtils.setLogLevel(GrpcClientStreamer.LOG, Level.ALL);
  }

  @Override
  public OutputStream newOutputStream(MiniRaftClusterWithGrpc cluster, int bufferSize) {
    final RaftProperties p = getProperties();
    GrpcConfigKeys.OutputStream.setBufferSize(p, SizeInBytes.valueOf(bufferSize));
    return new GrpcOutputStream(p, ClientId.randomId(), cluster.getGroup(), cluster.getLeader().getId(), null);
  }
}
