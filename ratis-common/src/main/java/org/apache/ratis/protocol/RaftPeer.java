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

import org.apache.ratis.util.NetUtils;

import java.net.InetSocketAddress;
import java.util.Objects;

/**
 * A {@link RaftPeer} is a server in a Raft cluster.
 *
 * The objects of this class are immutable.
 */
public class RaftPeer {
  private static final RaftPeer[] EMPTY_ARRAY = {};

  /** @return an empty array. */
  public static RaftPeer[] emptyArray() {
    return EMPTY_ARRAY;
  }

  /** The id of the peer. */
  private final RaftPeerId id;
  /** The address of the peer. */
  private final String address;

  /** Construct a peer with the given id and a null address. */
  public RaftPeer(RaftPeerId id) {
    this(id, (String)null);
  }

  /** Construct a peer with the given id and address. */
  public RaftPeer(RaftPeerId id, InetSocketAddress address) {
    this(id, address == null ? null : NetUtils.address2String(address));
  }

  /** Construct a peer with the given id and address. */
  public RaftPeer(RaftPeerId id, String address) {
    this.id = Objects.requireNonNull(id, "id == null");
    this.address = address;
  }

  /** @return The id of the peer. */
  public RaftPeerId getId() {
    return id;
  }

  /** @return The address of the peer. */
  public String getAddress() {
    return address;
  }

  @Override
  public String toString() {
    return id + ":" + address;
  }

  @Override
  public boolean equals(Object o) {
    return (o instanceof RaftPeer) && id.equals(((RaftPeer) o).getId());
  }

  @Override
  public int hashCode() {
    return id.hashCode();
  }
}
