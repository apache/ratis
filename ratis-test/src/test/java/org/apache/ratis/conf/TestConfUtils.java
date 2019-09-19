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
package org.apache.ratis.conf;

import org.apache.ratis.BaseTest;
import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.client.RaftClientConfigKeys;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.netty.NettyConfigKeys;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.junit.Test;

public class TestConfUtils  extends BaseTest {
  @Test
  public void testRaftConfigKeys() {
    ConfUtils.printAll(RaftConfigKeys.class);
  }

  @Test
  public void testRaftServerConfigKeys() {
    ConfUtils.printAll(RaftServerConfigKeys.class);
  }

  @Test
  public void testRaftClientConfigKeys() {
    ConfUtils.printAll(RaftClientConfigKeys.class);
  }

  @Test
  public void testGrpcConfigKeys() {
    ConfUtils.printAll(GrpcConfigKeys.class);
  }

  @Test
  public void testNettyConfigKeys() {
    ConfUtils.printAll(NettyConfigKeys.class);
  }
}
