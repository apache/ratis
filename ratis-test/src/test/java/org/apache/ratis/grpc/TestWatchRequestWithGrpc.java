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

import org.apache.ratis.WatchRequestTests;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.impl.UnorderedAsync;
import org.apache.ratis.grpc.client.GrpcClientRpc;
import org.apache.ratis.util.Slf4jUtils;
import org.slf4j.event.Level;

public class TestWatchRequestWithGrpc
    extends WatchRequestTests<MiniRaftClusterWithGrpc>
    implements MiniRaftClusterWithGrpc.FactoryGet {
  {
    Slf4jUtils.setLogLevel(GrpcClientRpc.LOG, Level.TRACE);
    Slf4jUtils.setLogLevel(UnorderedAsync.LOG, Level.TRACE);
    Slf4jUtils.setLogLevel(RaftClient.LOG, Level.TRACE);
  }
}