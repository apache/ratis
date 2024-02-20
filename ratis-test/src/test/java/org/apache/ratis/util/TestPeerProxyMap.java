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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.Closeable;
import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketAddress;

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

  @Test
  @Timeout(value = 10_000)
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
          Assertions.assertNotSame(proxy, newProxy);
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

  /** Copied from {@link org.apache.ratis.thirdparty.io.netty.channel.AbstractChannel}. */
  private static final class AnnotatedConnectException extends ConnectException {
    private static final long serialVersionUID = 3901958112696433556L;

    AnnotatedConnectException(ConnectException exception, SocketAddress remoteAddress) {
      super(exception.getMessage() + ": " + remoteAddress);
      initCause(exception);
    }

    // Suppress a warning since this method doesn't need synchronization
    @Override
    public Throwable fillInStackTrace() {
      return this;
    }
  }

  private static class ExceptionProxy implements Closeable {
    private final RaftPeer peer;

    ExceptionProxy(RaftPeer peer) {
      this.peer = peer;
    }

    @Override
    public void close() throws IOException {
      throw new AnnotatedConnectException(new ConnectException("Failed to connect to " + peer.getId()), null);
    }

    @Override
    public String toString() {
      return peer.getId().toString();
    }
  }

  @Test
  @Timeout(value = 1000)
  public void testStackTrace() {
    final RaftPeerId id = RaftPeerId.valueOf("s0");
    final RaftPeer peer = RaftPeer.newBuilder().setId(id).build();
    try(final PeerProxyMap<ExceptionProxy> map = new PeerProxyMap<>("test", ExceptionProxy::new);
        final ExceptionProxy ignored = map.computeIfAbsent(peer).get()) {
    } catch (IOException e) {
      assertThrowable("closeProxy", e, AnnotatedConnectException.class, LOG, ConnectException.class);
      Assertions.assertEquals(0, e.getStackTrace().length);
    }
  }
}
