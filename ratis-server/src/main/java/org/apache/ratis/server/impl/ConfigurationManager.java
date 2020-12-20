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

import org.apache.ratis.server.RaftConfiguration;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.StringUtils;

import java.util.*;

/**
 * Maintain the mappings between log index and corresponding raft configuration.
 * Initialized when starting the raft peer. The mappings are loaded from the
 * raft log, and updated while appending/truncating configuration related log
 * entries.
 */
public class ConfigurationManager {
  private final RaftConfigurationImpl initialConf;
  private final NavigableMap<Long, RaftConfigurationImpl> configurations = new TreeMap<>();
  /**
   * The current raft configuration. If configurations is not empty, should be
   * the last entry of the map. Otherwise is initialConf.
   */
  private volatile RaftConfigurationImpl currentConf;

  ConfigurationManager(RaftConfigurationImpl initialConf) {
    this.initialConf = initialConf;
    this.currentConf = initialConf;
  }

  synchronized void addConfiguration(RaftConfiguration conf) {
    final long logIndex = conf.getLogEntryIndex();
    final RaftConfiguration found = configurations.get(logIndex);
    if (found != null) {
      Preconditions.assertTrue(found.equals(conf));
      return;
    }
    addRaftConfigurationImpl(logIndex, (RaftConfigurationImpl) conf);
  }

  private void addRaftConfigurationImpl(long logIndex, RaftConfigurationImpl conf) {
    configurations.put(logIndex, conf);
    if (logIndex == configurations.lastEntry().getKey()) {
      currentConf = conf;
    }
  }

  RaftConfigurationImpl getCurrent() {
    return currentConf;
  }

  /**
   * Remove all the configurations whose log index is >= the given index.
   * @param index The given index. All the configurations whose log index is >=
   *              this value will be removed.
   * @return The configuration with largest log index < the given index.
   */
  synchronized RaftConfiguration removeConfigurations(long index) {
    // remove all configurations starting at the index
    for(final Iterator<?> iter = configurations.tailMap(index).entrySet().iterator(); iter.hasNext();) {
      iter.next();
      iter.remove();
    }
    currentConf = configurations.isEmpty() ? initialConf :
        configurations.lastEntry().getValue();
    return currentConf;
  }

  synchronized int numOfConf() {
    return 1 + configurations.size();
  }

  @Override
  public synchronized String toString() {
    return JavaUtils.getClassSimpleName(getClass())
        + ", init=" + initialConf
        + ", confs=" + StringUtils.map2String(configurations);
  }

  // TODO: remove Configuration entries after they are committed
}
