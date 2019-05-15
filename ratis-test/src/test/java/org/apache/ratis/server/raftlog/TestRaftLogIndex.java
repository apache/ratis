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
package org.apache.ratis.server.raftlog;

import org.apache.ratis.BaseTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.LongUnaryOperator;

public class TestRaftLogIndex extends BaseTest {

  static void assertUpdate(RaftLogIndex index, BiFunction<RaftLogIndex, Long, Boolean> update,
      long oldValue, long newValue, boolean expectUpdate) {
    assertUpdate(index, (i, op) -> update.apply(i, newValue), oldValue, old -> newValue, expectUpdate);
  }

  static void assertUpdate(RaftLogIndex index, BiFunction<RaftLogIndex, LongUnaryOperator, Boolean> update,
      long oldValue, LongUnaryOperator op, boolean expectUpdate) {
    Assert.assertEquals(oldValue, index.get());
    final boolean updated = update.apply(index, op);
    Assert.assertEquals(expectUpdate, updated);
    Assert.assertEquals(expectUpdate? op.applyAsLong(oldValue): oldValue, index.get());
  }


  @Test
  public void testIndex() {
    final int initialValue = 900;
    final RaftLogIndex index = new RaftLogIndex("index", initialValue);
    Assert.assertEquals(initialValue, index.get());

    final Consumer<Object> log = System.out::println;
    { // test updateIncreasingly
      final BiFunction<RaftLogIndex, Long, Boolean> f = (j, n) -> j.updateIncreasingly(n, log);
      final long i = index.get() + 1;
      assertUpdate(index, f, i - 1, i, true);
      assertUpdate(index, f, i, i, false);
      testFailureCase("updateIncreasingly to a smaller value",
          () -> assertUpdate(index, f, i, i - 1, false), IllegalStateException.class);
    }

    { // test updateToMax
      final BiFunction<RaftLogIndex, Long, Boolean> f = (j, n) -> j.updateToMax(n, log);
      final long i = index.get() + 1;
      assertUpdate(index, f, i - 1, i, true);
      assertUpdate(index, f, i, i, false);
      assertUpdate(index, f, i, i - 1, false);
    }

    { // test setUnconditionally
      final BiFunction<RaftLogIndex, Long, Boolean> f = (j, n) -> j.setUnconditionally(n, log);
      final long i = index.get() + 1;
      assertUpdate(index, f, i - 1, i, true);
      assertUpdate(index, f, i, i, false);
      assertUpdate(index, f, i, i - 1, true);
    }

    { // test updateUnconditionally
      final BiFunction<RaftLogIndex, LongUnaryOperator, Boolean> f = (j, op) -> j.updateUnconditionally(op, log);
      final long i = index.get() + 1;
      assertUpdate(index, f, i - 1, n -> n + 1, true);
      assertUpdate(index, f, i, n -> n, false);
      assertUpdate(index, f, i, n -> n - 1, true);
    }
  }
}
