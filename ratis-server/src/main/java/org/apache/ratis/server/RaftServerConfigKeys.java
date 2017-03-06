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
package org.apache.ratis.server;

import org.apache.ratis.conf.ConfUtils;

import java.util.function.BiConsumer;
import java.util.function.BiFunction;

import static org.apache.ratis.conf.ConfUtils.requireMin;

public interface RaftServerConfigKeys {
  String PREFIX = "raft.server";
  int KB = 1024;
  int MB = 1024*KB;

  String STORAGE_DIR_KEY = PREFIX + ".storage.dir";
  String STORAGE_DIR_DEFAULT = "file:///tmp/raft-server/";
  static String storageDir(BiFunction<String, String, String> getTrimmed) {
    return ConfUtils.get(getTrimmed, STORAGE_DIR_KEY, STORAGE_DIR_DEFAULT);
  }
  static void setStorageDir(BiConsumer<String, String> setString, String storageDir) {
    ConfUtils.set(setString, STORAGE_DIR_KEY, storageDir);
  }

  /**
   * When bootstrapping a new peer, If the gap between the match index of the
   * peer and the leader's latest committed index is less than this gap, we
   * treat the peer as caught-up.
   */
  String STAGING_CATCHUP_GAP_KEY = PREFIX + ".staging.catchup.gap";
  int STAGING_CATCHUP_GAP_DEFAULT = 1000; // increase this number when write throughput is high
  static int stagingCatchupGap(BiFunction<String, Integer, Integer> getInt) {
    return ConfUtils.getInt(getInt,
        STAGING_CATCHUP_GAP_KEY, STAGING_CATCHUP_GAP_DEFAULT, requireMin(0));
  }
  static void setStagingCatchupGap(BiConsumer<String, Integer> setInt, int stagingCatchupGap) {
    ConfUtils.setInt(setInt, STAGING_CATCHUP_GAP_KEY, stagingCatchupGap);
  }

  interface Log {
    String PREFIX = RaftServerConfigKeys.PREFIX + ".log";

    String USE_MEMORY_KEY = PREFIX + ".use.memory";
    boolean USE_MEMORY_DEFAULT = false;
    static boolean useMemory(BiFunction<String, Boolean, Boolean> getBool) {
      return ConfUtils.getBoolean(getBool, USE_MEMORY_KEY, USE_MEMORY_DEFAULT);
    }
    static void setUseMemory(BiConsumer<String, Boolean> setLong, boolean useMemory) {
      ConfUtils.setBoolean(setLong, USE_MEMORY_KEY, useMemory);
    }

    String SEGMENT_SIZE_MAX_KEY = PREFIX + ".segment.size.max";
    long SEGMENT_SIZE_MAX_DEFAULT = 8*MB;
    static long segmentSizeMax(BiFunction<String, Long, Long> getLong) {
      return ConfUtils.getLong(getLong,
          SEGMENT_SIZE_MAX_KEY, SEGMENT_SIZE_MAX_DEFAULT, requireMin(0L));
    }
    static void setSegmentSizeMax(
        BiConsumer<String, Long> setLong, long segmentSizeMax) {
      ConfUtils.setLong(setLong, SEGMENT_SIZE_MAX_KEY, segmentSizeMax);
    }

    String PREALLOCATED_SIZE_KEY = PREFIX + ".preallocated.size";
    int PREALLOCATED_SIZE_DEFAULT = 4*MB;
    static int preallocatedSize(BiFunction<String, Integer, Integer> getInt) {
      return ConfUtils.getInt(getInt,
          PREALLOCATED_SIZE_KEY, PREALLOCATED_SIZE_DEFAULT, requireMin(0));
    }
    static void setPreallocatedSize(BiConsumer<String, Integer> setInt, int preallocatedSize) {
      ConfUtils.setInt(setInt, PREALLOCATED_SIZE_KEY, preallocatedSize);
    }

    String WRITE_BUFFER_SIZE_KEY = PREFIX + ".write.buffer.size";
    int WRITE_BUFFER_SIZE_DEFAULT = 64*KB;
    static int writeBufferSize(BiFunction<String, Integer, Integer> getInt) {
      return ConfUtils.getInt(getInt,
          WRITE_BUFFER_SIZE_KEY, WRITE_BUFFER_SIZE_DEFAULT, requireMin(0));
    }
    static void setWriteBufferSize(BiConsumer<String, Integer> setInt, int writeBufferSize) {
      ConfUtils.setInt(setInt, WRITE_BUFFER_SIZE_KEY, writeBufferSize);
    }

    String FORCE_SYNC_NUM_KEY = PREFIX + ".force.sync.num";
    int FORCE_SYNC_NUM_DEFAULT = 128;
    static int forceSyncNum(BiFunction<String, Integer, Integer> getInt) {
      return ConfUtils.getInt(getInt,
          FORCE_SYNC_NUM_KEY, FORCE_SYNC_NUM_DEFAULT, requireMin(0));
    }

    interface Appender {
      String PREFIX = Log.PREFIX + ".appender";

      String BUFFER_CAPACITY_KEY = PREFIX + ".buffer.capacity";
      int BUFFER_CAPACITY_DEFAULT = 4*MB;
      static int bufferCapacity(BiFunction<String, Integer, Integer> getInt) {
        return ConfUtils.getInt(getInt,
            BUFFER_CAPACITY_KEY, BUFFER_CAPACITY_DEFAULT, requireMin(0));
      }
      static void setBufferCapacity(BiConsumer<String, Integer> setInt, int bufferCapacity) {
        ConfUtils.setInt(setInt, BUFFER_CAPACITY_KEY, bufferCapacity);
      }

      String BATCH_ENABLED_KEY = PREFIX + ".batch.enabled";
      boolean BATCH_ENABLED_DEFAULT = false;
      static boolean batchEnabled(BiFunction<String, Boolean, Boolean> getBool) {
        return ConfUtils.getBoolean(getBool, BATCH_ENABLED_KEY, BATCH_ENABLED_DEFAULT);
      }
      static void setBatchEnabled(
          BiConsumer<String, Boolean> setLong, boolean batchEnabled) {
        ConfUtils.setBoolean(setLong, BATCH_ENABLED_KEY, batchEnabled);
      }

      String SNAPSHOT_CHUNK_SIZE_MAX_KEY = PREFIX + ".snapshot.chunk.size.max";
      int SNAPSHOT_CHUNK_SIZE_MAX_DEFAULT = 16*MB;
      static int snapshotChunkSizeMax(BiFunction<String, Integer, Integer> getInt) {
        return ConfUtils.getInt(getInt,
            SNAPSHOT_CHUNK_SIZE_MAX_KEY, SNAPSHOT_CHUNK_SIZE_MAX_DEFAULT, requireMin(0));
      }
    }
  }

  interface Snapshot {
    String PREFIX = RaftServerConfigKeys.PREFIX + ".snapshot";

    /** whether trigger snapshot when log size exceeds limit */
    String AUTO_TRIGGER_ENABLED_KEY = PREFIX + ".auto.trigger.enabled";
    /** by default let the state machine to decide when to do checkpoint */
    boolean AUTO_TRIGGER_ENABLED_DEFAULT = false;
    static boolean autoTriggerEnabled(BiFunction<String, Boolean, Boolean> getBool) {
      return ConfUtils.getBoolean(getBool,
          AUTO_TRIGGER_ENABLED_KEY, AUTO_TRIGGER_ENABLED_DEFAULT);
    }
    static void setAutoTriggerEnabled(
        BiConsumer<String, Boolean> setLong, boolean autoTriggerThreshold) {
      ConfUtils.setBoolean(setLong, AUTO_TRIGGER_ENABLED_KEY, autoTriggerThreshold);
    }

    /** log size limit (in number of log entries) that triggers the snapshot */
    String AUTO_TRIGGER_THRESHOLD_KEY = PREFIX + ".auto.trigger.threshold";
    long AUTO_TRIGGER_THRESHOLD_DEFAULT = 400000L;
    static long autoTriggerThreshold(BiFunction<String, Long, Long> getLong) {
      return ConfUtils.getLong(getLong,
          AUTO_TRIGGER_THRESHOLD_KEY, AUTO_TRIGGER_THRESHOLD_DEFAULT, requireMin(0L));
    }
    static void setAutoTriggerThreshold(
        BiConsumer<String, Long> setLong, long autoTriggerThreshold) {
      ConfUtils.setLong(setLong, AUTO_TRIGGER_THRESHOLD_KEY, autoTriggerThreshold);
    }
  }

  /** server rpc timeout related */
  interface Rpc {
    String PREFIX = RaftServerConfigKeys.PREFIX + ".rpc";

    String TIMEOUT_MIN_MS_KEY = PREFIX + ".timeout.min.ms";
    int TIMEOUT_MIN_MS_DEFAULT = 150;
    static int timeoutMinMs(BiFunction<String, Integer, Integer> getInt) {
      return ConfUtils.getInt(getInt,
          TIMEOUT_MIN_MS_KEY, TIMEOUT_MIN_MS_DEFAULT, requireMin(0));
    }

    String TIMEOUT_MAX_MS_KEY = PREFIX + ".timeout.max.ms";
    int TIMEOUT_MAX_MS_DEFAULT = 300;
    static int timeoutMaxMs(BiFunction<String, Integer, Integer> getInt) {
      return ConfUtils.getInt(getInt,
          TIMEOUT_MAX_MS_KEY, TIMEOUT_MAX_MS_DEFAULT, requireMin(0));
    }

    String SLEEP_TIME_MS_KEY = PREFIX + ".sleep.time.ms";
    int SLEEP_TIME_MS_DEFAULT = 25;
    static int sleepTimeMs(BiFunction<String, Integer, Integer> getInt) {
      return ConfUtils.getInt(getInt,
          SLEEP_TIME_MS_KEY, SLEEP_TIME_MS_DEFAULT, requireMin(0));
    }
  }

  static void main(String[] args) {
    ConfUtils.printAll(RaftServerConfigKeys.class);
  }
}
