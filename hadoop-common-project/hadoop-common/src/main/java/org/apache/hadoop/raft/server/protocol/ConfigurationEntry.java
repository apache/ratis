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
package org.apache.hadoop.raft.server.protocol;

import java.util.Arrays;

public class ConfigurationEntry extends Entry {
  private final RaftPeer[] oldPeers;
  private final RaftPeer[] peers;

  public ConfigurationEntry(long term, long logIndex, RaftPeer[] oldPeers,
      RaftPeer[] peers) {
    super(term, logIndex, null);
    this.oldPeers = oldPeers;
    this.peers = peers;
  }

  public boolean isTranstional() {
    return oldPeers != null && peers != null;
  }

  public RaftPeer[] getOldPeers() {
    return oldPeers;
  }

  public RaftPeer[] getPeers() {
    return peers;
  }

  @Override
  public boolean isConfigurationEntry() {
    return true;
  }

  @Override
  public String toString() {
    return super.toString() + ", peers:" + Arrays.asList(peers) + ", old peers:"
        + (oldPeers != null ? Arrays.asList(oldPeers) : "[]");
  }
}
