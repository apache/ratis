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

import org.apache.hadoop.raft.server.RaftConfiguration;

public class ConfigurationEntry extends RaftLogEntry {
  /**
   * the raft configuration before this entry. we need to use this to set back
   * the configuration in case that the new conf entry is truncated later.
   */
  private final RaftConfiguration prev;
  /** the new configuration described by this entry */
  private final RaftConfiguration current;

  public ConfigurationEntry(long term, long logIndex, RaftConfiguration prev,
      RaftConfiguration current) {
    super(term, logIndex, null);
    this.prev = prev;
    this.current = current;
  }

  @Override
  public boolean isConfigurationEntry() {
    return true;
  }

  @Override
  public String toString() {
    return super.toString() + " current conf:" + current
        + ", previous conf: " + prev;
  }

  public RaftConfiguration getCurrent() {
    return this.current;
  }

  public RaftConfiguration getPrev() {
    return this.prev;
  }
}
