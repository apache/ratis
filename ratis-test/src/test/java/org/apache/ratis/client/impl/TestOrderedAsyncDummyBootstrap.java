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
package org.apache.ratis.client.impl;

import org.apache.ratis.BaseTest;
import org.apache.ratis.protocol.RaftClientRequest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;

class TestOrderedAsyncDummyBootstrap extends BaseTest {
  @Test
  void testBootstrapStartsWithNoopAndFallsBackToWatchOnce() {
    final OrderedAsync.DummyBootstrap bootstrap = new OrderedAsync.DummyBootstrap();
    Assertions.assertEquals(RaftClientRequest.noopRequestType(), bootstrap.nextRequestType());
    Assertions.assertTrue(bootstrap.handleFailure(new IllegalArgumentException("Unexpected request type: TYPE_NOT_SET")));
    Assertions.assertEquals(RaftClientRequest.watchRequestType(), bootstrap.nextRequestType());
    Assertions.assertNull(bootstrap.nextRequestType());
  }

  @Test
  void testNonCompatibilityFailuresDoNotTriggerLegacyWatchFallback() {
    final OrderedAsync.DummyBootstrap bootstrap = new OrderedAsync.DummyBootstrap();
    Assertions.assertEquals(RaftClientRequest.noopRequestType(), bootstrap.nextRequestType());
    Assertions.assertFalse(bootstrap.handleFailure(new IOException("connection reset")));
    Assertions.assertNull(bootstrap.nextRequestType());
  }
}
