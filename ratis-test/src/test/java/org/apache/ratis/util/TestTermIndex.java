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
package org.apache.ratis.util;

import org.apache.ratis.BaseTest;
import org.apache.ratis.RaftTestUtil;
import org.apache.ratis.server.protocol.ProtocolTestUtils;
import org.apache.ratis.server.protocol.TermIndex;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/** Testing {@link BiWeakValueCache}. */
public class TestTermIndex extends BaseTest {
  static BiWeakValueCache<Long, Long, TermIndex> CACHE = ProtocolTestUtils.getTermIndexCache();

  static void dumpCache(Integer expectedEmptyCount) {
    final int computed = CACHE.dump(System.out::print);
    if (expectedEmptyCount != null) {
      assertEquals(expectedEmptyCount, computed);
    }
    System.out.flush();
  }

  static void assertCacheSize(int expectedSize, long term) {
    final int computed = CACHE.count(term);
    if (computed != expectedSize) {
      dumpCache(null);
    }
    assertEquals(expectedSize, computed);
  }

  void assertCacheSizeWithGC(int expectedSize, long term) throws Exception{
    JavaUtils.attempt(() -> {
      RaftTestUtil.gc();
      assertCacheSize(expectedSize, term);
    }, 5, HUNDRED_MILLIS, "assertCacheSizeWithGC", LOG);
  }

  static void initTermIndex(TermIndex[][] ti, int term, int index) {
    ti[term][index] = TermIndex.valueOf(term, index);
  }

  @Test
  public void testCaching() throws Exception {
    final int n = 9;
    final TermIndex[][] ti = new TermIndex[n][n];
    final long[] terms = new long[n];
    final long[] indices = new long[n];
    for(int j = 0; j < n; j++) {
      terms[j] = j;
      indices[j] = j;
    }

    assertCacheSize(0, terms[1]);
    initTermIndex(ti, 1, 1);
    assertSame(ti[1][1], TermIndex.valueOf(terms[1], indices[1]));
    assertCacheSize(1, terms[1]);

    initTermIndex(ti, 1, 2);
    assertSame(ti[1][1], TermIndex.valueOf(terms[1], indices[1]));
    assertSame(ti[1][2], TermIndex.valueOf(terms[1], indices[2]));
    assertCacheSize(2, terms[1]);
    dumpCache(0);

    initTermIndex(ti, 2, 2);
    assertSame(ti[1][1], TermIndex.valueOf(terms[1], indices[1]));
    assertSame(ti[1][2], TermIndex.valueOf(terms[1], indices[2]));
    assertSame(ti[2][2], TermIndex.valueOf(terms[2], indices[2]));
    assertCacheSize(2, terms[1]);
    assertCacheSize(1, terms[2]);
    dumpCache(0);

    ti[1][1] = null; // release ti[1][1];
    assertCacheSizeWithGC(1, terms[1]);
    dumpCache(0);

    ti[1][2] = null; // release ti[1][2];
    assertCacheSizeWithGC(0, terms[1]);
    dumpCache(1);

    CACHE.cleanupEmptyInnerMaps();
    dumpCache(0);
  }
}
