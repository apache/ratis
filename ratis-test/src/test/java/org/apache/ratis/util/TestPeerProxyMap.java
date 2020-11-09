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
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.junit.Assert;
import org.junit.Test;

import java.io.Closeable;

/** Tests for {@link PeerProxyMap}. */
public class TestPeerProxyMap extends BaseTest {
  class DummyProxy implements Closeable {
    private final RaftPeer peer;

    DummyProxy(RaftPeer peer) {
      this.peer = peer;
    }

    @Override
    public void close() {
      LOG.info("{}: close before lock", this);
      synchronized(this) {
        LOG.info("{}: close in lock", this);
      }
    }

    @Override
    public String toString() {
      return peer.toString();
    }
  }

  @Test(timeout = 10_000)
  public void testCloseDeadLock() throws Exception {
    final PeerProxyMap<DummyProxy> map = new PeerProxyMap<>("test", DummyProxy::new);
    final RaftPeerId id = RaftPeerId.valueOf("s0");
    final RaftPeer peer = RaftPeer.newBuilder().setId(id).build();
    map.computeIfAbsent(peer);

    final DummyProxy proxy = map.getProxy(id);

    final Thread t = new Thread(() -> {
      // hold proxy lock and then getProxy(..) which requires resetLock
      synchronized (proxy) {
        LOG.info("Acquired lock");
        try {
          HUNDRED_MILLIS.sleep();
          LOG.info("Try getProxy");
          final DummyProxy newProxy = map.getProxy(id);
          Assert.assertNotSame(proxy, newProxy);
        } catch (Exception e) {
          setFirstException(e);
        }
        LOG.info("Will release lock");
      }
    });
    t.start();

    map.resetProxy(id); // hold resetLock and then call close() with requires proxy lock
    t.join();
  }
}
