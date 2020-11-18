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
package org.apache.ratis.server;

import org.apache.ratis.server.impl.RaftServerImpl;
import org.apache.ratis.server.impl.RaftServerProxy;
import org.apache.ratis.thirdparty.com.google.common.base.Joiner;
import org.apache.ratis.thirdparty.com.google.common.base.Stopwatch;
import org.apache.ratis.thirdparty.com.google.common.collect.Lists;
import org.apache.ratis.thirdparty.com.google.common.collect.Maps;
import org.apache.ratis.thirdparty.com.google.common.collect.Sets;
import org.apache.ratis.util.Daemon;
import org.apache.ratis.util.JavaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;


public class JvmPauseMonitor {
  public static final Logger LOG =
    LoggerFactory.getLogger(JvmPauseMonitor.class);
  /**
   * The target sleep time
   */
  private static final long SLEEP_INTERVAL_MS = 500;

  private Thread monitorThread;
  private volatile boolean shouldRun = true;
  private final RaftServerProxy proxy;

  private String formatMessage(long extraSleepTime,
                               Map<String, GcTimes> gcTimesAfterSleep,
                               Map<String, GcTimes> gcTimesBeforeSleep) {

    Set<String> gcBeanNames = Sets.intersection(gcTimesAfterSleep.keySet(),
      gcTimesBeforeSleep.keySet());
    List<String> gcDiffs = Lists.newArrayList();
    for (String name : gcBeanNames) {
      GcTimes diff = gcTimesAfterSleep.get(name)
                       .subtract(gcTimesBeforeSleep.get(name));
      if (diff.gcCount != 0) {
        gcDiffs.add("GC pool '" + name + "' had collection(s): " + diff);
      }
    }

    String ret = "Detected pause in JVM or host machine (eg GC): "
                   + "pause of approximately " + extraSleepTime + "ms\n";
    if (gcDiffs.isEmpty()) {
      ret += "No GCs detected";
    } else {
      ret += Joiner.on("\n").join(gcDiffs);
    }
    return ret;
  }

  public JvmPauseMonitor(RaftServerProxy proxy) {
    this.proxy = proxy;
  }

  private Map<String, GcTimes> getGcTimes() {
    Map<String, GcTimes> map = Maps.newHashMap();
    List<GarbageCollectorMXBean> gcBeans =
      ManagementFactory.getGarbageCollectorMXBeans();
    for (GarbageCollectorMXBean gcBean : gcBeans) {
      map.put(gcBean.getName(), new GcTimes(gcBean));
    }
    return map;
  }

  private static final class GcTimes {
    private GcTimes(GarbageCollectorMXBean gcBean) {
      gcCount = gcBean.getCollectionCount();
      gcTimeMillis = gcBean.getCollectionTime();
    }

    private GcTimes(long count, long time) {
      this.gcCount = count;
      this.gcTimeMillis = time;
    }

    private GcTimes subtract(GcTimes other) {
      return new GcTimes(this.gcCount - other.gcCount,
        this.gcTimeMillis - other.gcTimeMillis);
    }

    @Override
    public String toString() {
      return "count=" + gcCount + " time=" + gcTimeMillis + "ms";
    }

    private long gcCount;
    private long gcTimeMillis;
  }

  class Monitor implements Runnable {
    private final String name = JvmPauseMonitor.this + "-" + JavaUtils
                                                               .getClassSimpleName(getClass());

    public void run() {
      int leaderStepDownWaitTime =
        RaftServerConfigKeys.LeaderElection.leaderStepDownWaitTime(proxy.getProperties()).toIntExact(TimeUnit
                                                                                                       .MILLISECONDS);
      int rpcSlownessTimeoutMs = RaftServerConfigKeys.Rpc.slownessTimeout(proxy.getProperties()).toIntExact(
        TimeUnit.MILLISECONDS);
      Stopwatch sw = Stopwatch.createUnstarted();
      Map<String, GcTimes> gcTimesBeforeSleep = getGcTimes();
      LOG.info("Starting Ratis JVM pause monitor");
      while (shouldRun) {
        sw.reset().start();
        try {
          Thread.sleep(SLEEP_INTERVAL_MS);
        } catch (InterruptedException ie) {
          return;
        }
        long extraSleepTime = sw.elapsed(TimeUnit.MILLISECONDS) - SLEEP_INTERVAL_MS;
        Map<String, GcTimes> gcTimesAfterSleep = getGcTimes();

        if (extraSleepTime > 100) {
          LOG.warn(formatMessage(extraSleepTime, gcTimesAfterSleep, gcTimesBeforeSleep));
        }
        try {
          if (extraSleepTime > rpcSlownessTimeoutMs) {
            // close down all pipelines if the total gc period exceeds
            // rpc slowness timeout
            proxy.close();
          } else if (extraSleepTime > leaderStepDownWaitTime) {
            proxy.getImpls().forEach(RaftServerImpl::stepDownOnJvmPause);
          }
        } catch (IOException ioe) {
          LOG.info("Encountered exception in JvmPauseMonitor {}", ioe);
        }
      }
    }

    @Override
    public String toString() {
      return name;
    }
  }


  public void start() {
    monitorThread = new Daemon(new Monitor());
    monitorThread.start();
  }

  public void stop() {
    shouldRun = false;
    if (monitorThread != null) {
      monitorThread.interrupt();
      try {
        monitorThread.join();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }
}
