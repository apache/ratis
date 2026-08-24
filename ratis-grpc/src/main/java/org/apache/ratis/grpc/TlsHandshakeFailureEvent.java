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
package org.apache.ratis.grpc;

import java.net.SocketAddress;
import java.util.Objects;

/** Information about a failed TLS handshake. */
public final class TlsHandshakeFailureEvent {
  private final Throwable cause;
  private final SocketAddress localAddress;
  private final SocketAddress remoteAddress;
  private final boolean inbound;

  public TlsHandshakeFailureEvent(Throwable cause, SocketAddress localAddress,
      SocketAddress remoteAddress, boolean inbound) {
    this.cause = Objects.requireNonNull(cause, "cause");
    this.localAddress = localAddress;
    this.remoteAddress = remoteAddress;
    this.inbound = inbound;
  }

  public Throwable getCause() {
    return cause;
  }

  public SocketAddress getLocalAddress() {
    return localAddress;
  }

  public SocketAddress getRemoteAddress() {
    return remoteAddress;
  }

  public boolean isInbound() {
    return inbound;
  }
}
