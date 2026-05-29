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
package org.apache.ratis.server.impl;

import org.apache.ratis.util.TimeDuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class TestReplyFlusher {
  @Test
  public void testAdvanceRepliedIndexBeforeCompletingReplies() throws Exception {
    final ReplyFlusher flusher = new ReplyFlusher(getClass().getSimpleName(), 0,
        TimeDuration.valueOf(10, TimeUnit.MILLISECONDS));
    final CompletableFuture<Long> repliedIndexWhenCompleted = new CompletableFuture<>();

    flusher.hold(7, () -> repliedIndexWhenCompleted.complete(flusher.getRepliedIndex()));
    flusher.start(0);
    try {
      Assertions.assertEquals(7, repliedIndexWhenCompleted.get(5, TimeUnit.SECONDS));
    } finally {
      flusher.stop();
    }
  }
}
