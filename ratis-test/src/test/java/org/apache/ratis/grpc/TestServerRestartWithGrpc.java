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
package org.apache.ratis.grpc;

import org.apache.ratis.server.ServerRestartTests;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;

@RunWith(Parameterized.class)
public class TestServerRestartWithGrpc
    extends ServerRestartTests<MiniRaftClusterWithGrpc>
    implements MiniRaftClusterWithGrpc.FactoryGet {
  public TestServerRestartWithGrpc(Boolean zeroCopyEnabled) {
    GrpcConfigKeys.Server.setZeroCopyEnabled(getProperties(), zeroCopyEnabled);
  }

  @Parameterized.Parameters
  public static Collection<Boolean[]> data() {
    return Arrays.asList((new Boolean[][] {{FALSE}, {TRUE}}));
  }
}