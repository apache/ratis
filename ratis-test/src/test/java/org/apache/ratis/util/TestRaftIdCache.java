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
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.TestRaftId;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

/** Testing {@link WeakValueCache}. */
public class TestRaftIdCache extends BaseTest {
  static WeakValueCache<UUID, ClientId> CACHE = TestRaftId.getClientIdCache();

  static String dumpCache() {
    final List<ClientId> values = CACHE.getValues();
    values.sort(Comparator.comparing(ClientId::getUuid));
    String header = CACHE + ": " + values.size();
    System.out.println(header);
    System.out.println("  " + values);
    return header;
  }

  static void assertCache(IDs expectedIDs) {
    final List<ClientId> computed = CACHE.getValues();
    computed.sort(Comparator.comparing(ClientId::getUuid));

    final List<ClientId> expected = expectedIDs.getIds();
    expected.sort(Comparator.comparing(ClientId::getUuid));

    assertEquals(expected, computed, TestRaftIdCache::dumpCache);
  }

  void assertCacheSizeWithGC(IDs expectedIDs) throws Exception{
    JavaUtils.attempt(() -> {
      RaftTestUtil.gc();
      assertCache(expectedIDs);
    }, 5, HUNDRED_MILLIS, "assertCacheSizeWithGC", LOG);
  }

  class IDs {
    private final List<ClientId> ids = new LinkedList<>();

    List<ClientId> getIds() {
      return new ArrayList<>(ids);
    }

    int size() {
      return ids.size();
    }

    ClientId allocate() {
      final ClientId id = ClientId.randomId();
      LOG.info("allocate {}", id);
      ids.add(id);
      return id;
    }

    void release() {
      final int r = ThreadLocalRandom.current().nextInt(size());
      final ClientId removed = ids.remove(r);
      LOG.info("release {}", removed);
    }
  }

  @Test
  public void testCaching() throws Exception {
    final int n = 100;
    final IDs ids = new IDs();
    assertEquals(0, ids.size());
    assertCache(ids);

    for(int i = 0; i < n; i++) {
      final ClientId id = ids.allocate();
      assertSame(id, ClientId.valueOf(id.getUuid()));
      assertCache(ids);
    }

    for(int i = 0; i < n/2; i++) {
      ids.release();
      if (ThreadLocalRandom.current().nextInt(10) == 0) {
        assertCacheSizeWithGC(ids);
      }
    }
    assertCacheSizeWithGC(ids);

    for(int i = 0; i < n/2; i++) {
      final ClientId id = ids.allocate();
      assertSame(id, ClientId.valueOf(id.getUuid()));
      assertCache(ids);
    }


    for(int i = 0; i < n; i++) {
      ids.release();
      if (ThreadLocalRandom.current().nextInt(10) == 0) {
        assertCacheSizeWithGC(ids);
      }
    }
    assertCacheSizeWithGC(ids);

    assertEquals(0, ids.size());
  }
}
