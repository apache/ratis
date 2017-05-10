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
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.util.SizeInBytes;
import org.apache.ratis.util.TimeDuration;

import java.util.concurrent.TimeUnit;

import static org.apache.ratis.conf.ConfUtils.*;

public interface RaftServerConfigKeys {
  String PREFIX = "raft.server";

  String STORAGE_DIR_KEY = PREFIX + ".storage.dir";
  String STORAGE_DIR_DEFAULT = "file:///tmp/raft-server/";
  static String storageDir(RaftProperties properties) {
    return get(properties::getTrimmed, STORAGE_DIR_KEY, STORAGE_DIR_DEFAULT);
  }
  static void setStorageDir(RaftProperties properties, String storageDir) {
    set(properties::set, STORAGE_DIR_KEY, storageDir);
  }

  /**
   * When bootstrapping a new peer, If the gap between the match index of the
   * peer and the leader's latest committed index is less than this gap, we
   * treat the peer as caught-up.
   */
  String STAGING_CATCHUP_GAP_KEY = PREFIX + ".staging.catchup.gap";
  int STAGING_CATCHUP_GAP_DEFAULT = 1000; // increase this number when write throughput is high
  static int stagingCatchupGap(RaftProperties properties) {
    return getInt(properties::getInt,
        STAGING_CATCHUP_GAP_KEY, STAGING_CATCHUP_GAP_DEFAULT, requireMin(0));
  }
  static void setStagingCatchupGap(RaftProperties properties, int stagingCatchupGap) {
    setInt(properties::setInt, STAGING_CATCHUP_GAP_KEY, stagingCatchupGap);
  }

  interface Log {
    String PREFIX = RaftServerConfigKeys.PREFIX + ".log";

    String USE_MEMORY_KEY = PREFIX + ".use.memory";
    boolean USE_MEMORY_DEFAULT = false;
    static boolean useMemory(RaftProperties properties) {
      return getBoolean(properties::getBoolean, USE_MEMORY_KEY, USE_MEMORY_DEFAULT);
    }
    static void setUseMemory(RaftProperties properties, boolean useMemory) {
      setBoolean(properties::setBoolean, USE_MEMORY_KEY, useMemory);
    }

    String SEGMENT_SIZE_MAX_KEY = PREFIX + ".segment.size.max";
    SizeInBytes SEGMENT_SIZE_MAX_DEFAULT = SizeInBytes.valueOf("8MB");
    static SizeInBytes segmentSizeMax(RaftProperties properties) {
      return getSizeInBytes(properties::getSizeInBytes,
          SEGMENT_SIZE_MAX_KEY, SEGMENT_SIZE_MAX_DEFAULT);
    }
    static void setSegmentSizeMax(RaftProperties properties, SizeInBytes segmentSizeMax) {
      setSizeInBytes(properties::set, SEGMENT_SIZE_MAX_KEY, segmentSizeMax);
    }

    /**
     * Besides the open segment, the max number of segments caching log entries.
     */
    String SEGMENT_CACHE_MAX_NUM_KEY = PREFIX + ".segment.cache.num.max";
    int SEGMENT_CACHE_MAX_NUM_DEFAULT = 6;
    static int maxCachedSegmentNum(RaftProperties properties) {
      return getInt(properties::getInt, SEGMENT_CACHE_MAX_NUM_KEY,
          SEGMENT_CACHE_MAX_NUM_DEFAULT, requireMin(0));
    }

    String PREALLOCATED_SIZE_KEY = PREFIX + ".preallocated.size";
    SizeInBytes PREALLOCATED_SIZE_DEFAULT = SizeInBytes.valueOf("4MB");
    static SizeInBytes preallocatedSize(RaftProperties properties) {
      return getSizeInBytes(properties::getSizeInBytes,
          PREALLOCATED_SIZE_KEY, PREALLOCATED_SIZE_DEFAULT);
    }
    static void setPreallocatedSize(RaftProperties properties, SizeInBytes preallocatedSize) {
      setSizeInBytes(properties::set, PREALLOCATED_SIZE_KEY, preallocatedSize);
    }

    String WRITE_BUFFER_SIZE_KEY = PREFIX + ".write.buffer.size";
    SizeInBytes WRITE_BUFFER_SIZE_DEFAULT =SizeInBytes.valueOf("64KB");
    static SizeInBytes writeBufferSize(RaftProperties properties) {
      return getSizeInBytes(properties::getSizeInBytes,
          WRITE_BUFFER_SIZE_KEY, WRITE_BUFFER_SIZE_DEFAULT);
    }
    static void setWriteBufferSize(RaftProperties properties, SizeInBytes writeBufferSize) {
      setSizeInBytes(properties::set, WRITE_BUFFER_SIZE_KEY, writeBufferSize);
    }

    String FORCE_SYNC_NUM_KEY = PREFIX + ".force.sync.num";
    int FORCE_SYNC_NUM_DEFAULT = 128;
    static int forceSyncNum(RaftProperties properties) {
      return getInt(properties::getInt,
          FORCE_SYNC_NUM_KEY, FORCE_SYNC_NUM_DEFAULT, requireMin(0));
    }

    interface Appender {
      String PREFIX = Log.PREFIX + ".appender";

      String BUFFER_CAPACITY_KEY = PREFIX + ".buffer.capacity";
      SizeInBytes BUFFER_CAPACITY_DEFAULT =SizeInBytes.valueOf("4MB");
      static SizeInBytes bufferCapacity(RaftProperties properties) {
        return getSizeInBytes(properties::getSizeInBytes,
            BUFFER_CAPACITY_KEY, BUFFER_CAPACITY_DEFAULT);
      }
      static void setBufferCapacity(RaftProperties properties, SizeInBytes bufferCapacity) {
        setSizeInBytes(properties::set, BUFFER_CAPACITY_KEY, bufferCapacity);
      }

      String BATCH_ENABLED_KEY = PREFIX + ".batch.enabled";
      boolean BATCH_ENABLED_DEFAULT = false;
      static boolean batchEnabled(RaftProperties properties) {
        return getBoolean(properties::getBoolean, BATCH_ENABLED_KEY, BATCH_ENABLED_DEFAULT);
      }
      static void setBatchEnabled(RaftProperties properties, boolean batchEnabled) {
        setBoolean(properties::setBoolean, BATCH_ENABLED_KEY, batchEnabled);
      }

      String SNAPSHOT_CHUNK_SIZE_MAX_KEY = PREFIX + ".snapshot.chunk.size.max";
      SizeInBytes SNAPSHOT_CHUNK_SIZE_MAX_DEFAULT =SizeInBytes.valueOf("16MB");
      static SizeInBytes snapshotChunkSizeMax(RaftProperties properties) {
        return getSizeInBytes(properties::getSizeInBytes,
            SNAPSHOT_CHUNK_SIZE_MAX_KEY, SNAPSHOT_CHUNK_SIZE_MAX_DEFAULT);
      }
    }
  }

  interface Snapshot {
    String PREFIX = RaftServerConfigKeys.PREFIX + ".snapshot";

    /** whether trigger snapshot when log size exceeds limit */
    String AUTO_TRIGGER_ENABLED_KEY = PREFIX + ".auto.trigger.enabled";
    /** by default let the state machine to decide when to do checkpoint */
    boolean AUTO_TRIGGER_ENABLED_DEFAULT = false;
    static boolean autoTriggerEnabled(RaftProperties properties) {
      return getBoolean(properties::getBoolean,
          AUTO_TRIGGER_ENABLED_KEY, AUTO_TRIGGER_ENABLED_DEFAULT);
    }
    static void setAutoTriggerEnabled(RaftProperties properties, boolean autoTriggerThreshold) {
      setBoolean(properties::setBoolean, AUTO_TRIGGER_ENABLED_KEY, autoTriggerThreshold);
    }

    /** log size limit (in number of log entries) that triggers the snapshot */
    String AUTO_TRIGGER_THRESHOLD_KEY = PREFIX + ".auto.trigger.threshold";
    long AUTO_TRIGGER_THRESHOLD_DEFAULT = 400000L;
    static long autoTriggerThreshold(RaftProperties properties) {
      return getLong(properties::getLong,
          AUTO_TRIGGER_THRESHOLD_KEY, AUTO_TRIGGER_THRESHOLD_DEFAULT, requireMin(0L));
    }
    static void setAutoTriggerThreshold(RaftProperties properties, long autoTriggerThreshold) {
      setLong(properties::setLong, AUTO_TRIGGER_THRESHOLD_KEY, autoTriggerThreshold);
    }
  }

  /** server rpc timeout related */
  interface Rpc {
    String PREFIX = RaftServerConfigKeys.PREFIX + ".rpc";

    String TIMEOUT_MIN_KEY = PREFIX + ".timeout.min";
    TimeDuration TIMEOUT_MIN_DEFAULT = TimeDuration.valueOf(150, TimeUnit.MILLISECONDS);
    static TimeDuration timeoutMin(RaftProperties properties) {
      return getTimeDuration(properties.getTimeDuration(TIMEOUT_MIN_DEFAULT.getUnit()),
          TIMEOUT_MIN_KEY, TIMEOUT_MIN_DEFAULT);
    }

    String TIMEOUT_MAX_KEY = PREFIX + ".timeout.max";
    TimeDuration TIMEOUT_MAX_DEFAULT = TimeDuration.valueOf(300, TimeUnit.MILLISECONDS);
    static TimeDuration timeoutMax(RaftProperties properties) {
      return getTimeDuration(properties.getTimeDuration(TIMEOUT_MAX_DEFAULT.getUnit()),
          TIMEOUT_MAX_KEY, TIMEOUT_MAX_DEFAULT);
    }

    String SLEEP_TIME_KEY = PREFIX + ".sleep.time";
    TimeDuration SLEEP_TIME_DEFAULT = TimeDuration.valueOf(25, TimeUnit.MILLISECONDS);
    static TimeDuration sleepTime(RaftProperties properties) {
      return getTimeDuration(properties.getTimeDuration(SLEEP_TIME_DEFAULT.getUnit()),
          SLEEP_TIME_KEY, SLEEP_TIME_DEFAULT);
    }
  }

  /** server retry cache related */
  interface RetryCache {
    String PREFIX = RaftServerConfigKeys.PREFIX + ".retrycache";

    String CAPACITY_KEY = PREFIX + ".capacity";
    int CAPACITY_DEFAULT = 4096;
    static int capacity(RaftProperties properties) {
      return ConfUtils.getInt(properties::getInt, CAPACITY_KEY, CAPACITY_DEFAULT,
          ConfUtils.requireMin(0));
    }

    String EXPIRYTIME_KEY = PREFIX + ".expirytime";
    TimeDuration EXPIRYTIME_DEFAULT = TimeDuration.valueOf(60, TimeUnit.SECONDS);
    static TimeDuration expiryTime(RaftProperties properties) {
      return getTimeDuration(properties.getTimeDuration(EXPIRYTIME_DEFAULT.getUnit()),
          EXPIRYTIME_KEY, EXPIRYTIME_DEFAULT);
    }
  }

  static void main(String[] args) {
    printAll(RaftServerConfigKeys.class);
  }
}
