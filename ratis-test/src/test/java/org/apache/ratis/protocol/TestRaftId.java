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
package org.apache.ratis.protocol;

import org.apache.ratis.BaseTest;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.Assert;
import org.junit.Test;

public class TestRaftId extends BaseTest {
  @Override
  public int getGlobalTimeoutSeconds() {
    return 1;
  }

  @Test
  public void testClientId() {
    final ClientId id = ClientId.randomId();
    final ByteString bytes = id.toByteString();
    Assert.assertEquals(bytes, id.toByteString());
    Assert.assertEquals(id, ClientId.valueOf(bytes));
  }

  @Test
  public void testRaftGroupId() {
    final RaftGroupId id = RaftGroupId.randomId();
    final ByteString bytes = id.toByteString();
    Assert.assertEquals(bytes, id.toByteString());
    Assert.assertEquals(id, RaftGroupId.valueOf(bytes));
  }

  @Test
  public void testRaftPeerId() {
    final RaftPeerId id = RaftPeerId.valueOf("abc");
    final ByteString bytes = id.toByteString();
    Assert.assertEquals(bytes, id.toByteString());
    Assert.assertEquals(id, RaftPeerId.valueOf(bytes));
  }
}
