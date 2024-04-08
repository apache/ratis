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
package org.apache.ratis.server.metrics;

import org.apache.ratis.metrics.RatisMetricRegistry;
import org.apache.ratis.metrics.impl.RatisMetricRegistryImpl;
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.thirdparty.com.codahale.metrics.Gauge;
import org.apache.ratis.util.Preconditions;

import java.util.SortedMap;
import java.util.function.Supplier;

public interface ServerMetricsTestUtils {
  static Gauge getGaugeWithName(String gaugeName, Supplier<RatisMetricRegistry> metrics) {
    final SortedMap<String, Gauge> gaugeMap = ((RatisMetricRegistryImpl)metrics.get()).getGauges(
        (s, metric) -> s.contains(gaugeName));
    return gaugeMap.get(gaugeMap.firstKey());
  }

  /**
   * Get the commit index gauge for the given peer of the server
   * @return Metric Gauge holding the value of commit index of the peer
   */
  static Gauge getPeerCommitIndexGauge(RaftGroupMemberId serverId, RaftPeerId peerId) {
    final RaftServerMetricsImpl serverMetrics = RaftServerMetricsImpl.getImpl(serverId);
    if (serverMetrics == null) {
      return null;
    }

    final String followerCommitIndexKey = RaftServerMetricsImpl.getPeerCommitIndexGaugeKey(peerId);

    final SortedMap<String, Gauge> map = ((RatisMetricRegistryImpl)serverMetrics.getRegistry())
        .getGauges((s, metric) -> s.contains(followerCommitIndexKey));

    Preconditions.assertTrue(map.size() <= 1);
    return map.get(map.firstKey());
  }
}