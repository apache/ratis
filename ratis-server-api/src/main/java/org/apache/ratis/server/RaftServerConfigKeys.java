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

import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.SizeInBytes;
import org.apache.ratis.util.TimeDuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.apache.ratis.conf.ConfUtils.*;

public interface RaftServerConfigKeys {
  Logger LOG = LoggerFactory.getLogger(RaftServerConfigKeys.class);
  static Consumer<String> getDefaultLog() {
    return LOG::info;
  }

  String PREFIX = "raft.server";

  String STORAGE_DIR_KEY = PREFIX + ".storage.dir";
  List<File> STORAGE_DIR_DEFAULT = Collections.singletonList(new File("/tmp/raft-server/"));
  static List<File> storageDir(RaftProperties properties) {
    return getFiles(properties::getFiles, STORAGE_DIR_KEY, STORAGE_DIR_DEFAULT, getDefaultLog());
  }
  static void setStorageDir(RaftProperties properties, List<File> storageDir) {
    setFiles(properties::setFiles, STORAGE_DIR_KEY, storageDir);
  }

  String STORAGE_FREE_SPACE_MIN_KEY = PREFIX + ".storage.free-space.min";
  SizeInBytes STORAGE_FREE_SPACE_MIN_DEFAULT = SizeInBytes.valueOf("0MB");
  static SizeInBytes storageFreeSpaceMin(RaftProperties properties) {
    return getSizeInBytes(properties::getSizeInBytes,
        STORAGE_FREE_SPACE_MIN_KEY, STORAGE_FREE_SPACE_MIN_DEFAULT, getDefaultLog());
  }
  static void setStorageFreeSpaceMin(RaftProperties properties, SizeInBytes storageFreeSpaceMin) {
    setSizeInBytes(properties::set, STORAGE_FREE_SPACE_MIN_KEY, storageFreeSpaceMin);
  }

  String REMOVED_GROUPS_DIR_KEY = PREFIX + ".removed.groups.dir";
  File REMOVED_GROUPS_DIR_DEFAULT = new File("/tmp/raft-server/removed-groups/");
  static File removedGroupsDir(RaftProperties properties) {
    return getFile(properties::getFile, REMOVED_GROUPS_DIR_KEY,
        REMOVED_GROUPS_DIR_DEFAULT, getDefaultLog());
  }
  static void setRemovedGroupsDir(RaftProperties properties, File removedGroupsStorageDir) {
    setFile(properties::setFile, REMOVED_GROUPS_DIR_KEY, removedGroupsStorageDir);
  }

  String SLEEP_DEVIATION_THRESHOLD_KEY = PREFIX + ".sleep.deviation.threshold";
  TimeDuration SLEEP_DEVIATION_THRESHOLD_DEFAULT = TimeDuration.valueOf(300, TimeUnit.MILLISECONDS);
  static TimeDuration sleepDeviationThreshold(RaftProperties properties) {
    return getTimeDuration(properties.getTimeDuration(SLEEP_DEVIATION_THRESHOLD_DEFAULT.getUnit()),
        SLEEP_DEVIATION_THRESHOLD_KEY, SLEEP_DEVIATION_THRESHOLD_DEFAULT, getDefaultLog());
  }
  static void setSleepDeviationThreshold(RaftProperties properties, int thresholdMs) {
    setInt(properties::setInt, SLEEP_DEVIATION_THRESHOLD_KEY, thresholdMs);
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
        STAGING_CATCHUP_GAP_KEY, STAGING_CATCHUP_GAP_DEFAULT, getDefaultLog(), requireMin(0));
  }
  static void setStagingCatchupGap(RaftProperties properties, int stagingCatchupGap) {
    setInt(properties::setInt, STAGING_CATCHUP_GAP_KEY, stagingCatchupGap);
  }

  interface ThreadPool {
    String PREFIX = RaftServerConfigKeys.PREFIX + ".threadpool";

    String PROXY_CACHED_KEY = PREFIX + ".proxy.cached";
    boolean PROXY_CACHED_DEFAULT = true;
    static boolean proxyCached(RaftProperties properties) {
      return getBoolean(properties::getBoolean, PROXY_CACHED_KEY, PROXY_CACHED_DEFAULT, getDefaultLog());
    }
    static void setProxyCached(RaftProperties properties, boolean useCached) {
      setBoolean(properties::setBoolean, PROXY_CACHED_KEY, useCached);
    }

    String PROXY_SIZE_KEY = PREFIX + ".proxy.size";
    int PROXY_SIZE_DEFAULT = 0;
    static int proxySize(RaftProperties properties) {
      return getInt(properties::getInt, PROXY_SIZE_KEY, PROXY_SIZE_DEFAULT, getDefaultLog(),
          requireMin(0), requireMax(65536));
    }
    static void setProxySize(RaftProperties properties, int size) {
      setInt(properties::setInt, PROXY_SIZE_KEY, size);
    }

    String SERVER_CACHED_KEY = PREFIX + ".server.cached";
    boolean SERVER_CACHED_DEFAULT = true;
    static boolean serverCached(RaftProperties properties) {
      return getBoolean(properties::getBoolean, SERVER_CACHED_KEY, SERVER_CACHED_DEFAULT, getDefaultLog());
    }
    static void setServerCached(RaftProperties properties, boolean useCached) {
      setBoolean(properties::setBoolean, SERVER_CACHED_KEY, useCached);
    }

    String SERVER_SIZE_KEY = PREFIX + ".server.size";
    int SERVER_SIZE_DEFAULT = 0;
    static int serverSize(RaftProperties properties) {
      return getInt(properties::getInt, SERVER_SIZE_KEY, SERVER_SIZE_DEFAULT, getDefaultLog(),
          requireMin(0), requireMax(65536));
    }
    static void setServerSize(RaftProperties properties, int size) {
      setInt(properties::setInt, SERVER_SIZE_KEY, size);
    }

    String CLIENT_CACHED_KEY = PREFIX + ".client.cached";
    boolean CLIENT_CACHED_DEFAULT = true;
    static boolean clientCached(RaftProperties properties) {
      return getBoolean(properties::getBoolean, CLIENT_CACHED_KEY, CLIENT_CACHED_DEFAULT, getDefaultLog());
    }
    static void setClientCached(RaftProperties properties, boolean useCached) {
      setBoolean(properties::setBoolean, CLIENT_CACHED_KEY, useCached);
    }

    String CLIENT_SIZE_KEY = PREFIX + ".client.size";
    int CLIENT_SIZE_DEFAULT = 0;
    static int clientSize(RaftProperties properties) {
      return getInt(properties::getInt, CLIENT_SIZE_KEY, CLIENT_SIZE_DEFAULT, getDefaultLog(),
          requireMin(0), requireMax(65536));
    }
    static void setClientSize(RaftProperties properties, int size) {
      setInt(properties::setInt, CLIENT_SIZE_KEY, size);
    }
  }

  interface Read {
    String PREFIX = RaftServerConfigKeys.PREFIX
        + "." + JavaUtils.getClassSimpleName(Read.class).toLowerCase();

    String TIMEOUT_KEY = PREFIX + ".timeout";
    TimeDuration TIMEOUT_DEFAULT = TimeDuration.valueOf(10, TimeUnit.SECONDS);
    static TimeDuration timeout(RaftProperties properties) {
      return getTimeDuration(properties.getTimeDuration(TIMEOUT_DEFAULT.getUnit()),
          TIMEOUT_KEY, TIMEOUT_DEFAULT, getDefaultLog(), requirePositive());
    }
    static void setTimeout(RaftProperties properties, TimeDuration readOnlyTimeout) {
      setTimeDuration(properties::setTimeDuration, TIMEOUT_KEY, readOnlyTimeout);
    }

    enum Option {
      /** Directly query statemachine. Efficient but may undermine linearizability */
      DEFAULT,

      /** Use ReadIndex (see Raft Paper section 6.4). Maintains linearizability */
      LINEARIZABLE
    }

    String OPTION_KEY = PREFIX + ".option";
    Option OPTION_DEFAULT = Option.DEFAULT;
    static Option option(RaftProperties properties) {
      Option option =  get(properties::getEnum, OPTION_KEY, OPTION_DEFAULT, getDefaultLog());
      if (option != Option.DEFAULT && option != Option.LINEARIZABLE) {
        throw new IllegalArgumentException("Unexpected read option: " + option);
      }
      return option;
    }
    static void setOption(RaftProperties properties, Option option) {
      set(properties::setEnum, OPTION_KEY, option);
    }

    String LEADER_LEASE_ENABLED_KEY = PREFIX + ".leader.lease.enabled";
    boolean LEADER_LEASE_ENABLED_DEFAULT = false;
    static boolean leaderLeaseEnabled(RaftProperties properties) {
      return getBoolean(properties::getBoolean, LEADER_LEASE_ENABLED_KEY,
          LEADER_LEASE_ENABLED_DEFAULT, getDefaultLog());
    }
    static void setLeaderLeaseEnabled(RaftProperties properties, boolean enabled) {
      setBoolean(properties::setBoolean, LEADER_LEASE_ENABLED_KEY, enabled);
    }

    String LEADER_LEASE_TIMEOUT_RATIO_KEY = PREFIX + ".leader.lease.timeout.ratio";
    double LEADER_LEASE_TIMEOUT_RATIO_DEFAULT = 0.9;
    static double leaderLeaseTimeoutRatio(RaftProperties properties) {
      return getDouble(properties::getDouble, LEADER_LEASE_TIMEOUT_RATIO_KEY,
          LEADER_LEASE_TIMEOUT_RATIO_DEFAULT, getDefaultLog(),
          requireMin(0.0), requireMax(1.0));
    }

    static void setLeaderLeaseTimeoutRatio(RaftProperties properties, double ratio) {
      setDouble(properties::setDouble, LEADER_LEASE_TIMEOUT_RATIO_KEY, ratio);
    }

    interface ReadAfterWriteConsistent {
      String PREFIX = RaftServerConfigKeys.PREFIX + ".read-after-write-consistent";

      String WRITE_INDEX_CACHE_EXPIRY_TIME_KEY = PREFIX + "write-index-cache.expiry-time";
      /** Must be larger than {@link Read#TIMEOUT_DEFAULT}. */
      TimeDuration WRITE_INDEX_CACHE_EXPIRY_TIME_DEFAULT = TimeDuration.valueOf(60, TimeUnit.SECONDS);

      static TimeDuration writeIndexCacheExpiryTime(RaftProperties properties) {
        return getTimeDuration(properties.getTimeDuration(WRITE_INDEX_CACHE_EXPIRY_TIME_DEFAULT.getUnit()),
            WRITE_INDEX_CACHE_EXPIRY_TIME_KEY, WRITE_INDEX_CACHE_EXPIRY_TIME_DEFAULT, getDefaultLog());
      }

      static void setWriteIndexCacheExpiryTime(RaftProperties properties, TimeDuration expiryTime) {
        setTimeDuration(properties::setTimeDuration, WRITE_INDEX_CACHE_EXPIRY_TIME_KEY, expiryTime);
      }
    }
  }

  interface Write {
    String PREFIX = RaftServerConfigKeys.PREFIX + ".write";

    String ELEMENT_LIMIT_KEY = PREFIX + ".element-limit";
    int ELEMENT_LIMIT_DEFAULT = 4096;

    static int elementLimit(RaftProperties properties) {
      return getInt(properties::getInt, ELEMENT_LIMIT_KEY, ELEMENT_LIMIT_DEFAULT, getDefaultLog(), requireMin(1));
    }
    static void setElementLimit(RaftProperties properties, int limit) {
      setInt(properties::setInt, ELEMENT_LIMIT_KEY, limit, requireMin(1));
    }

    String BYTE_LIMIT_KEY = PREFIX + ".byte-limit";
    SizeInBytes BYTE_LIMIT_DEFAULT = SizeInBytes.valueOf("64MB");
    static SizeInBytes byteLimit(RaftProperties properties) {
      return getSizeInBytes(properties::getSizeInBytes,
          BYTE_LIMIT_KEY, BYTE_LIMIT_DEFAULT, getDefaultLog(), requireMinSizeInByte(SizeInBytes.ONE_MB));
    }
    static void setByteLimit(RaftProperties properties, SizeInBytes byteLimit) {
      setSizeInBytes(properties::set, BYTE_LIMIT_KEY, byteLimit, requireMin(1L));
    }

    String FOLLOWER_GAP_RATIO_MAX_KEY = PREFIX + ".follower.gap.ratio.max";
    // The valid range is [1, 0) and -1, -1 means disable this feature
    double FOLLOWER_GAP_RATIO_MAX_DEFAULT = -1d;

    static double followerGapRatioMax(RaftProperties properties) {
      return getDouble(properties::getDouble, FOLLOWER_GAP_RATIO_MAX_KEY,
          FOLLOWER_GAP_RATIO_MAX_DEFAULT, getDefaultLog(), requireMax(1d));
    }
    static void setFollowerGapRatioMax(RaftProperties properties, float ratio) {
      setDouble(properties::setDouble, FOLLOWER_GAP_RATIO_MAX_KEY, ratio, requireMax(1d));
    }
  }

  interface Watch {
    String PREFIX = RaftServerConfigKeys.PREFIX + ".watch";

    String ELEMENT_LIMIT_KEY = PREFIX + ".element-limit";
    int ELEMENT_LIMIT_DEFAULT = 65536;
    static int elementLimit(RaftProperties properties) {
      return getInt(properties::getInt, ELEMENT_LIMIT_KEY, ELEMENT_LIMIT_DEFAULT, getDefaultLog(), requireMin(1));
    }
    static void setElementLimit(RaftProperties properties, int limit) {
      setInt(properties::setInt, ELEMENT_LIMIT_KEY, limit, requireMin(1));
    }

    String TIMEOUT_DENOMINATION_KEY = PREFIX + ".timeout.denomination";
    TimeDuration TIMEOUT_DENOMINATION_DEFAULT = TimeDuration.valueOf(1, TimeUnit.SECONDS);
    static TimeDuration timeoutDenomination(RaftProperties properties) {
      return getTimeDuration(properties.getTimeDuration(TIMEOUT_DENOMINATION_DEFAULT.getUnit()),
          TIMEOUT_DENOMINATION_KEY, TIMEOUT_DENOMINATION_DEFAULT, getDefaultLog(), requirePositive());
    }
    static void setTimeoutDenomination(RaftProperties properties, TimeDuration watchTimeout) {
      setTimeDuration(properties::setTimeDuration, TIMEOUT_DENOMINATION_KEY, watchTimeout);
    }

    /** Timeout for watch requests. */
    String TIMEOUT_KEY = PREFIX + ".timeout";
    TimeDuration TIMEOUT_DEFAULT = TimeDuration.valueOf(10, TimeUnit.SECONDS);
    static TimeDuration timeout(RaftProperties properties) {
      return getTimeDuration(properties.getTimeDuration(TIMEOUT_DEFAULT.getUnit()),
          TIMEOUT_KEY, TIMEOUT_DEFAULT, getDefaultLog(), requirePositive());
    }
    static void setTimeout(RaftProperties properties, TimeDuration watchTimeout) {
      setTimeDuration(properties::setTimeDuration, TIMEOUT_KEY, watchTimeout);
    }
  }

  interface Log {
    String PREFIX = RaftServerConfigKeys.PREFIX + ".log";

    String USE_MEMORY_KEY = PREFIX + ".use.memory";
    boolean USE_MEMORY_DEFAULT = false;
    static boolean useMemory(RaftProperties properties) {
      return getBoolean(properties::getBoolean, USE_MEMORY_KEY, USE_MEMORY_DEFAULT, getDefaultLog());
    }
    static void setUseMemory(RaftProperties properties, boolean useMemory) {
      setBoolean(properties::setBoolean, USE_MEMORY_KEY, useMemory);
    }

    String QUEUE_ELEMENT_LIMIT_KEY = PREFIX + ".queue.element-limit";
    int QUEUE_ELEMENT_LIMIT_DEFAULT = 4096;
    static int queueElementLimit(RaftProperties properties) {
      return getInt(properties::getInt, QUEUE_ELEMENT_LIMIT_KEY, QUEUE_ELEMENT_LIMIT_DEFAULT, getDefaultLog(),
          requireMin(1));
    }
    static void setQueueElementLimit(RaftProperties properties, int queueSize) {
      setInt(properties::setInt, QUEUE_ELEMENT_LIMIT_KEY, queueSize, requireMin(1));
    }

    String QUEUE_BYTE_LIMIT_KEY = PREFIX + ".queue.byte-limit";
    SizeInBytes QUEUE_BYTE_LIMIT_DEFAULT = SizeInBytes.valueOf("64MB");
    static SizeInBytes queueByteLimit(RaftProperties properties) {
      return getSizeInBytes(properties::getSizeInBytes,
          QUEUE_BYTE_LIMIT_KEY, QUEUE_BYTE_LIMIT_DEFAULT, getDefaultLog());
    }

    @Deprecated
    static void setQueueByteLimit(RaftProperties properties, int queueSize) {
      setInt(properties::setInt, QUEUE_BYTE_LIMIT_KEY, queueSize, requireMin(1));
    }
    static void setQueueByteLimit(RaftProperties properties, SizeInBytes byteLimit) {
      setSizeInBytes(properties::set, QUEUE_BYTE_LIMIT_KEY, byteLimit, requireMin(1L));
    }

    String PURGE_GAP_KEY = PREFIX + ".purge.gap";
    int PURGE_GAP_DEFAULT = 1024;
    static int purgeGap(RaftProperties properties) {
      return getInt(properties::getInt, PURGE_GAP_KEY, PURGE_GAP_DEFAULT, getDefaultLog(), requireMin(1));
    }
    static void setPurgeGap(RaftProperties properties, int purgeGap) {
      setInt(properties::setInt, PURGE_GAP_KEY, purgeGap, requireMin(1));
    }

    // Config to allow purging up to the snapshot index even if some other
    // peers are behind in their commit index.
    String PURGE_UPTO_SNAPSHOT_INDEX_KEY = PREFIX + ".purge.upto.snapshot.index";
    boolean PURGE_UPTO_SNAPSHOT_INDEX_DEFAULT = false;
    static boolean purgeUptoSnapshotIndex(RaftProperties properties) {
      return getBoolean(properties::getBoolean, PURGE_UPTO_SNAPSHOT_INDEX_KEY,
          PURGE_UPTO_SNAPSHOT_INDEX_DEFAULT, getDefaultLog());
    }
    static void setPurgeUptoSnapshotIndex(RaftProperties properties, boolean shouldPurgeUptoSnapshotIndex) {
      setBoolean(properties::setBoolean, PURGE_UPTO_SNAPSHOT_INDEX_KEY, shouldPurgeUptoSnapshotIndex);
    }

    String PURGE_PRESERVATION_LOG_NUM_KEY = PREFIX + ".purge.preservation.log.num";
    long PURGE_PRESERVATION_LOG_NUM_DEFAULT = 0L;
    static long purgePreservationLogNum(RaftProperties properties) {
      return getLong(properties::getLong, PURGE_PRESERVATION_LOG_NUM_KEY,
              PURGE_PRESERVATION_LOG_NUM_DEFAULT, getDefaultLog());
    }
    static void setPurgePreservationLogNum(RaftProperties properties, long purgePreserveLogNum) {
      setLong(properties::setLong, PURGE_PRESERVATION_LOG_NUM_KEY, purgePreserveLogNum);
    }

    String SEGMENT_SIZE_MAX_KEY = PREFIX + ".segment.size.max";
    SizeInBytes SEGMENT_SIZE_MAX_DEFAULT = SizeInBytes.valueOf("8MB");
    static SizeInBytes segmentSizeMax(RaftProperties properties) {
      return getSizeInBytes(properties::getSizeInBytes,
          SEGMENT_SIZE_MAX_KEY, SEGMENT_SIZE_MAX_DEFAULT, getDefaultLog());
    }
    static void setSegmentSizeMax(RaftProperties properties, SizeInBytes segmentSizeMax) {
      setSizeInBytes(properties::set, SEGMENT_SIZE_MAX_KEY, segmentSizeMax);
    }

    /**
     * Besides the open segment, the max number of segments caching log entries.
     */
    String SEGMENT_CACHE_NUM_MAX_KEY = PREFIX + ".segment.cache.num.max";
    int SEGMENT_CACHE_NUM_MAX_DEFAULT = 6;
    static int segmentCacheNumMax(RaftProperties properties) {
      return getInt(properties::getInt, SEGMENT_CACHE_NUM_MAX_KEY,
          SEGMENT_CACHE_NUM_MAX_DEFAULT, getDefaultLog(), requireMin(0));
    }
    static void setSegmentCacheNumMax(RaftProperties properties, int maxCachedSegmentNum) {
      setInt(properties::setInt, SEGMENT_CACHE_NUM_MAX_KEY, maxCachedSegmentNum);
    }

    String SEGMENT_CACHE_SIZE_MAX_KEY = PREFIX + ".segment.cache.size.max";
    SizeInBytes SEGMENT_CACHE_SIZE_MAX_DEFAULT = SizeInBytes.valueOf("200MB");
    static SizeInBytes segmentCacheSizeMax(RaftProperties properties) {
      return getSizeInBytes(properties::getSizeInBytes, SEGMENT_CACHE_SIZE_MAX_KEY,
          SEGMENT_CACHE_SIZE_MAX_DEFAULT, getDefaultLog());
    }
    static void setSegmentCacheSizeMax(RaftProperties properties, SizeInBytes maxCachedSegmentSize) {
      setSizeInBytes(properties::set, SEGMENT_CACHE_SIZE_MAX_KEY, maxCachedSegmentSize);
    }

    String PREALLOCATED_SIZE_KEY = PREFIX + ".preallocated.size";
    SizeInBytes PREALLOCATED_SIZE_DEFAULT = SizeInBytes.valueOf("4MB");
    static SizeInBytes preallocatedSize(RaftProperties properties) {
      return getSizeInBytes(properties::getSizeInBytes,
          PREALLOCATED_SIZE_KEY, PREALLOCATED_SIZE_DEFAULT, getDefaultLog());
    }
    static void setPreallocatedSize(RaftProperties properties, SizeInBytes preallocatedSize) {
      setSizeInBytes(properties::set, PREALLOCATED_SIZE_KEY, preallocatedSize);
    }

    String WRITE_BUFFER_SIZE_KEY = PREFIX + ".write.buffer.size";
    SizeInBytes WRITE_BUFFER_SIZE_DEFAULT =SizeInBytes.valueOf("64KB");
    static SizeInBytes writeBufferSize(RaftProperties properties) {
      return getSizeInBytes(properties::getSizeInBytes,
          WRITE_BUFFER_SIZE_KEY, WRITE_BUFFER_SIZE_DEFAULT, getDefaultLog());
    }
    static void setWriteBufferSize(RaftProperties properties, SizeInBytes writeBufferSize) {
      setSizeInBytes(properties::set, WRITE_BUFFER_SIZE_KEY, writeBufferSize);
    }

    String FORCE_SYNC_NUM_KEY = PREFIX + ".force.sync.num";
    int FORCE_SYNC_NUM_DEFAULT = 128;
    static int forceSyncNum(RaftProperties properties) {
      return getInt(properties::getInt,
          FORCE_SYNC_NUM_KEY, FORCE_SYNC_NUM_DEFAULT, getDefaultLog(), requireMin(0));
    }
    static void setForceSyncNum(RaftProperties properties, int forceSyncNum) {
      setInt(properties::setInt, FORCE_SYNC_NUM_KEY, forceSyncNum);
    }

    /** Unsafe-flush allow increasing flush index without waiting the actual flush to complete. */
    String UNSAFE_FLUSH_ENABLED_KEY = PREFIX + ".unsafe-flush.enabled";
    boolean UNSAFE_FLUSH_ENABLED_DEFAULT = false;
    static boolean unsafeFlushEnabled(RaftProperties properties) {
      return getBoolean(properties::getBoolean,
              UNSAFE_FLUSH_ENABLED_KEY, UNSAFE_FLUSH_ENABLED_DEFAULT, getDefaultLog());
    }
    static void setUnsafeFlushEnabled(RaftProperties properties, boolean unsafeFlush) {
      setBoolean(properties::setBoolean, UNSAFE_FLUSH_ENABLED_KEY, unsafeFlush);
    }

    /** Async-flush will increase flush index until the actual flush has completed. */
    String ASYNC_FLUSH_ENABLED_KEY = PREFIX + ".async-flush.enabled";
    boolean ASYNC_FLUSH_ENABLED_DEFAULT = false;
    static boolean asyncFlushEnabled(RaftProperties properties) {
      return getBoolean(properties::getBoolean,
          ASYNC_FLUSH_ENABLED_KEY, ASYNC_FLUSH_ENABLED_DEFAULT, getDefaultLog());
    }
    static void setAsyncFlushEnabled(RaftProperties properties, boolean asyncFlush) {
      setBoolean(properties::setBoolean, ASYNC_FLUSH_ENABLED_KEY, asyncFlush);
    }

    /** The policy to handle corrupted raft log. */
    enum CorruptionPolicy {
      /** Rethrow the exception. */
      EXCEPTION,
      /** Print a warn log message and return all uncorrupted log entries up to the corruption. */
      WARN_AND_RETURN;

      public static CorruptionPolicy getDefault() {
        return EXCEPTION;
      }

      public static <T> CorruptionPolicy get(T supplier, Function<T, CorruptionPolicy> getMethod) {
        return Optional.ofNullable(supplier).map(getMethod).orElse(getDefault());
      }
    }

    String CORRUPTION_POLICY_KEY = PREFIX + ".corruption.policy";
    CorruptionPolicy CORRUPTION_POLICY_DEFAULT = CorruptionPolicy.getDefault();
    static CorruptionPolicy corruptionPolicy(RaftProperties properties) {
      return get(properties::getEnum,
          CORRUPTION_POLICY_KEY, CORRUPTION_POLICY_DEFAULT, getDefaultLog());
    }
    static void setCorruptionPolicy(RaftProperties properties, CorruptionPolicy corruptionPolicy) {
      set(properties::setEnum, CORRUPTION_POLICY_KEY, corruptionPolicy);
    }

    interface StateMachineData {
      String PREFIX = Log.PREFIX + ".statemachine.data";

      String SYNC_KEY = PREFIX + ".sync";
      boolean SYNC_DEFAULT = true;
      static boolean sync(RaftProperties properties) {
        return getBoolean(properties::getBoolean,
            SYNC_KEY, SYNC_DEFAULT, getDefaultLog());
      }
      static void setSync(RaftProperties properties, boolean sync) {
        setBoolean(properties::setBoolean, SYNC_KEY, sync);
      }
      String CACHING_ENABLED_KEY = PREFIX + ".caching.enabled";
      boolean CACHING_ENABLED_DEFAULT = false;
      static boolean cachingEnabled(RaftProperties properties) {
        return getBoolean(properties::getBoolean,
            CACHING_ENABLED_KEY, CACHING_ENABLED_DEFAULT, getDefaultLog());
      }
      static void setCachingEnabled(RaftProperties properties, boolean enable) {
        setBoolean(properties::setBoolean, CACHING_ENABLED_KEY, enable);
      }

      String SYNC_TIMEOUT_KEY = PREFIX + ".sync.timeout";
      TimeDuration SYNC_TIMEOUT_DEFAULT = TimeDuration.valueOf(10, TimeUnit.SECONDS);
      static TimeDuration syncTimeout(RaftProperties properties) {
        return getTimeDuration(properties.getTimeDuration(SYNC_TIMEOUT_DEFAULT.getUnit()),
            SYNC_TIMEOUT_KEY, SYNC_TIMEOUT_DEFAULT, getDefaultLog());
      }
      static void setSyncTimeout(RaftProperties properties, TimeDuration syncTimeout) {
        setTimeDuration(properties::setTimeDuration, SYNC_TIMEOUT_KEY, syncTimeout);
      }

      /**
       * -1: retry indefinitely
       *  0: no retry
       * >0: the number of retries
       */
      String SYNC_TIMEOUT_RETRY_KEY = PREFIX + ".sync.timeout.retry";
      int SYNC_TIMEOUT_RETRY_DEFAULT = -1;
      static int syncTimeoutRetry(RaftProperties properties) {
        return getInt(properties::getInt, SYNC_TIMEOUT_RETRY_KEY, SYNC_TIMEOUT_RETRY_DEFAULT, getDefaultLog(),
            requireMin(-1));
      }
      static void setSyncTimeoutRetry(RaftProperties properties, int syncTimeoutRetry) {
        setInt(properties::setInt, SYNC_TIMEOUT_RETRY_KEY, syncTimeoutRetry, requireMin(-1));
      }

      String READ_TIMEOUT_KEY = PREFIX + ".read.timeout";
      TimeDuration READ_TIMEOUT_DEFAULT = TimeDuration.valueOf(1000, TimeUnit.MILLISECONDS);
      static TimeDuration readTimeout(RaftProperties properties) {
        return getTimeDuration(properties.getTimeDuration(READ_TIMEOUT_DEFAULT.getUnit()),
            READ_TIMEOUT_KEY, READ_TIMEOUT_DEFAULT, getDefaultLog());
      }
      static void setReadTimeout(RaftProperties properties, TimeDuration readTimeout) {
        setTimeDuration(properties::setTimeDuration, READ_TIMEOUT_KEY, readTimeout);
      }
    }

    interface Appender {
      String PREFIX = Log.PREFIX + ".appender";

      String BUFFER_ELEMENT_LIMIT_KEY = PREFIX + ".buffer.element-limit";
      /** 0 means no limit. */
      int BUFFER_ELEMENT_LIMIT_DEFAULT = 0;
      static int bufferElementLimit(RaftProperties properties) {
        return getInt(properties::getInt,
            BUFFER_ELEMENT_LIMIT_KEY, BUFFER_ELEMENT_LIMIT_DEFAULT, getDefaultLog(), requireMin(0));
      }
      static void setBufferElementLimit(RaftProperties properties, int bufferElementLimit) {
        setInt(properties::setInt, BUFFER_ELEMENT_LIMIT_KEY, bufferElementLimit);
      }

      String BUFFER_BYTE_LIMIT_KEY = PREFIX + ".buffer.byte-limit";
      SizeInBytes BUFFER_BYTE_LIMIT_DEFAULT = SizeInBytes.valueOf("4MB");
      static SizeInBytes bufferByteLimit(RaftProperties properties) {
        return getSizeInBytes(properties::getSizeInBytes,
            BUFFER_BYTE_LIMIT_KEY, BUFFER_BYTE_LIMIT_DEFAULT, getDefaultLog());
      }
      static void setBufferByteLimit(RaftProperties properties, SizeInBytes bufferByteLimit) {
        setSizeInBytes(properties::set, BUFFER_BYTE_LIMIT_KEY, bufferByteLimit);
      }

      String SNAPSHOT_CHUNK_SIZE_MAX_KEY = PREFIX + ".snapshot.chunk.size.max";
      SizeInBytes SNAPSHOT_CHUNK_SIZE_MAX_DEFAULT =SizeInBytes.valueOf("16MB");
      static SizeInBytes snapshotChunkSizeMax(RaftProperties properties) {
        return getSizeInBytes(properties::getSizeInBytes,
            SNAPSHOT_CHUNK_SIZE_MAX_KEY, SNAPSHOT_CHUNK_SIZE_MAX_DEFAULT, getDefaultLog());
      }
      static void setSnapshotChunkSizeMax(RaftProperties properties, SizeInBytes maxChunkSize) {
        setSizeInBytes(properties::set, SNAPSHOT_CHUNK_SIZE_MAX_KEY, maxChunkSize);
      }

      String INSTALL_SNAPSHOT_ENABLED_KEY = PREFIX + ".install.snapshot.enabled";
      boolean INSTALL_SNAPSHOT_ENABLED_DEFAULT = true;
      static boolean installSnapshotEnabled(RaftProperties properties) {
        return getBoolean(properties::getBoolean,
            INSTALL_SNAPSHOT_ENABLED_KEY, INSTALL_SNAPSHOT_ENABLED_DEFAULT, getDefaultLog());
      }
      static void setInstallSnapshotEnabled(RaftProperties properties, boolean shouldInstallSnapshot) {
        setBoolean(properties::setBoolean, INSTALL_SNAPSHOT_ENABLED_KEY, shouldInstallSnapshot);
      }

      String WAIT_TIME_MIN_KEY = PREFIX + ".wait-time.min";
      TimeDuration WAIT_TIME_MIN_DEFAULT = TimeDuration.valueOf(10, TimeUnit.MILLISECONDS);
      static TimeDuration waitTimeMin(RaftProperties properties) {
        return getTimeDuration(properties.getTimeDuration(WAIT_TIME_MIN_DEFAULT.getUnit()),
            WAIT_TIME_MIN_KEY, WAIT_TIME_MIN_DEFAULT, getDefaultLog());
      }
      static void setWaitTimeMin(RaftProperties properties, TimeDuration minDuration) {
        setTimeDuration(properties::setTimeDuration, WAIT_TIME_MIN_KEY, minDuration);
      }

      String RETRY_POLICY_KEY = PREFIX + ".retry.policy";
      /**
       * The min wait time as 1ms (0 is not allowed) for first 10,
       * (5 iteration with 2 times grpc client retry)
       * next wait 1sec for next 20 retry (10 iteration with 2 times grpc client)
       * further wait for 5sec for max times ((5sec*980)/2 times ~= 40min)
       */
      String RETRY_POLICY_DEFAULT = "1ms,10, 1s,20, 5s,1000";
      static String retryPolicy(RaftProperties properties) {
        return properties.get(RETRY_POLICY_KEY, RETRY_POLICY_DEFAULT);
      }
      static void setRetryPolicy(RaftProperties properties, String retryPolicy) {
        properties.set(RETRY_POLICY_KEY, retryPolicy);
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
          AUTO_TRIGGER_ENABLED_KEY, AUTO_TRIGGER_ENABLED_DEFAULT, getDefaultLog());
    }
    static void setAutoTriggerEnabled(RaftProperties properties, boolean autoTriggerEnabled) {
      setBoolean(properties::setBoolean, AUTO_TRIGGER_ENABLED_KEY, autoTriggerEnabled);
    }

    /** whether trigger snapshot when stop raft server */
    String TRIGGER_WHEN_STOP_ENABLED_KEY = PREFIX + ".trigger-when-stop.enabled";
    /** by default let the state machine to trigger snapshot when stop */
    boolean TRIGGER_WHEN_STOP_ENABLED_DEFAULT = true;
    static boolean triggerWhenStopEnabled(RaftProperties properties) {
      return getBoolean(properties::getBoolean,
          TRIGGER_WHEN_STOP_ENABLED_KEY, TRIGGER_WHEN_STOP_ENABLED_DEFAULT, getDefaultLog());
    }
    static void setTriggerWhenStopEnabled(RaftProperties properties, boolean triggerWhenStopEnabled) {
      setBoolean(properties::setBoolean, TRIGGER_WHEN_STOP_ENABLED_KEY, triggerWhenStopEnabled);
    }

    /** The log index gap between to two snapshot creations. */
    String CREATION_GAP_KEY = PREFIX + ".creation.gap";
    long CREATION_GAP_DEFAULT = 1024;
    static long creationGap(RaftProperties properties) {
      return getLong(
          properties::getLong, CREATION_GAP_KEY, CREATION_GAP_DEFAULT,
          getDefaultLog(), requireMin(1L));
    }
    static void setCreationGap(RaftProperties properties, long creationGap) {
      setLong(properties::setLong, CREATION_GAP_KEY, creationGap);
    }

    /** log size limit (in number of log entries) that triggers the snapshot */
    String AUTO_TRIGGER_THRESHOLD_KEY = PREFIX + ".auto.trigger.threshold";
    long AUTO_TRIGGER_THRESHOLD_DEFAULT = 400000L;
    static long autoTriggerThreshold(RaftProperties properties) {
      return getLong(properties::getLong,
          AUTO_TRIGGER_THRESHOLD_KEY, AUTO_TRIGGER_THRESHOLD_DEFAULT, getDefaultLog(), requireMin(0L));
    }
    static void setAutoTriggerThreshold(RaftProperties properties, long autoTriggerThreshold) {
      setLong(properties::setLong, AUTO_TRIGGER_THRESHOLD_KEY, autoTriggerThreshold);
    }

    String RETENTION_FILE_NUM_KEY = PREFIX + ".retention.file.num";
    int RETENTION_FILE_NUM_DEFAULT = -1;
    static int retentionFileNum(RaftProperties raftProperties) {
      return getInt(raftProperties::getInt, RETENTION_FILE_NUM_KEY, RETENTION_FILE_NUM_DEFAULT, getDefaultLog());
    }
    static void setRetentionFileNum(RaftProperties properties, int numSnapshotFilesRetained) {
      setInt(properties::setInt, RETENTION_FILE_NUM_KEY, numSnapshotFilesRetained);
    }
  }

  interface DataStream {
    String PREFIX = RaftServerConfigKeys.PREFIX + ".data-stream";

    String ASYNC_REQUEST_THREAD_POOL_CACHED_KEY = PREFIX + ".async.request.thread.pool.cached";
    boolean ASYNC_REQUEST_THREAD_POOL_CACHED_DEFAULT = false;

    static boolean asyncRequestThreadPoolCached(RaftProperties properties) {
      return getBoolean(properties::getBoolean, ASYNC_REQUEST_THREAD_POOL_CACHED_KEY,
          ASYNC_REQUEST_THREAD_POOL_CACHED_DEFAULT, getDefaultLog());
    }

    static void setAsyncRequestThreadPoolCached(RaftProperties properties, boolean useCached) {
      setBoolean(properties::setBoolean, ASYNC_REQUEST_THREAD_POOL_CACHED_KEY, useCached);
    }

    String ASYNC_REQUEST_THREAD_POOL_SIZE_KEY = PREFIX + ".async.request.thread.pool.size";
    int ASYNC_REQUEST_THREAD_POOL_SIZE_DEFAULT = 32;

    static int asyncRequestThreadPoolSize(RaftProperties properties) {
      return getInt(properties::getInt, ASYNC_REQUEST_THREAD_POOL_SIZE_KEY,
          ASYNC_REQUEST_THREAD_POOL_SIZE_DEFAULT, getDefaultLog(),
          requireMin(0), requireMax(65536));
    }

    static void setAsyncRequestThreadPoolSize(RaftProperties properties, int size) {
      setInt(properties::setInt, ASYNC_REQUEST_THREAD_POOL_SIZE_KEY, size);
    }

    String ASYNC_WRITE_THREAD_POOL_SIZE_KEY = PREFIX + ".async.write.thread.pool.size";
    int ASYNC_WRITE_THREAD_POOL_SIZE_DEFAULT = 16;

    static int asyncWriteThreadPoolSize(RaftProperties properties) {
      return getInt(properties::getInt, ASYNC_WRITE_THREAD_POOL_SIZE_KEY,
          ASYNC_WRITE_THREAD_POOL_SIZE_DEFAULT, getDefaultLog(),
          requireMin(0), requireMax(65536));
    }

    static void setAsyncWriteThreadPoolSize(RaftProperties properties, int size) {
      setInt(properties::setInt, ASYNC_WRITE_THREAD_POOL_SIZE_KEY, size);
    }

    String CLIENT_POOL_SIZE_KEY = PREFIX + ".client.pool.size";
    int CLIENT_POOL_SIZE_DEFAULT = 10;

    static int clientPoolSize(RaftProperties properties) {
      return getInt(properties::getInt, CLIENT_POOL_SIZE_KEY,
          CLIENT_POOL_SIZE_DEFAULT, getDefaultLog(),
          requireMin(0), requireMax(65536));
    }

    static void setClientPoolSize(RaftProperties properties, int num) {
      setInt(properties::setInt, CLIENT_POOL_SIZE_KEY, num);
    }
  }

  /** server rpc timeout related */
  interface Rpc {
    String PREFIX = RaftServerConfigKeys.PREFIX + ".rpc";

    String TIMEOUT_MIN_KEY = PREFIX + ".timeout.min";
    TimeDuration TIMEOUT_MIN_DEFAULT = TimeDuration.valueOf(150, TimeUnit.MILLISECONDS);
    static TimeDuration timeoutMin(RaftProperties properties, Consumer<String> logger) {
      return getTimeDuration(properties.getTimeDuration(TIMEOUT_MIN_DEFAULT.getUnit()),
          TIMEOUT_MIN_KEY, TIMEOUT_MIN_DEFAULT, logger);
    }
    static TimeDuration timeoutMin(RaftProperties properties) {
      return timeoutMin(properties, getDefaultLog());
    }
    static void setTimeoutMin(RaftProperties properties, TimeDuration minDuration) {
      setTimeDuration(properties::setTimeDuration, TIMEOUT_MIN_KEY, minDuration);
    }

    String TIMEOUT_MAX_KEY = PREFIX + ".timeout.max";
    TimeDuration TIMEOUT_MAX_DEFAULT = TimeDuration.valueOf(300, TimeUnit.MILLISECONDS);
    static TimeDuration timeoutMax(RaftProperties properties, Consumer<String> logger) {
      return getTimeDuration(properties.getTimeDuration(TIMEOUT_MAX_DEFAULT.getUnit()),
          TIMEOUT_MAX_KEY, TIMEOUT_MAX_DEFAULT, logger);
    }
    static TimeDuration timeoutMax(RaftProperties properties) {
      return timeoutMax(properties, getDefaultLog());
    }
    static void setTimeoutMax(RaftProperties properties, TimeDuration maxDuration) {
      setTimeDuration(properties::setTimeDuration, TIMEOUT_MAX_KEY, maxDuration);
    }

    /** separate first timeout so that the startup unavailable time can be reduced */
    String FIRST_ELECTION_TIMEOUT_MIN_KEY = PREFIX + ".first-election.timeout.min";
    TimeDuration FIRST_ELECTION_TIMEOUT_MIN_DEFAULT = null;
    static TimeDuration firstElectionTimeoutMin(RaftProperties properties) {
      final TimeDuration fallbackFirstElectionTimeoutMin = Rpc.timeoutMin(properties, null);
      return getTimeDuration(properties.getTimeDuration(fallbackFirstElectionTimeoutMin.getUnit()),
          FIRST_ELECTION_TIMEOUT_MIN_KEY, FIRST_ELECTION_TIMEOUT_MIN_DEFAULT,
          Rpc.TIMEOUT_MIN_KEY, fallbackFirstElectionTimeoutMin, getDefaultLog());
    }
    static void setFirstElectionTimeoutMin(RaftProperties properties, TimeDuration firstMinDuration) {
      setTimeDuration(properties::setTimeDuration, FIRST_ELECTION_TIMEOUT_MIN_KEY, firstMinDuration);
    }

    String FIRST_ELECTION_TIMEOUT_MAX_KEY = PREFIX + ".first-election.timeout.max";
    TimeDuration FIRST_ELECTION_TIMEOUT_MAX_DEFAULT = null;
    static TimeDuration firstElectionTimeoutMax(RaftProperties properties) {
      final TimeDuration fallbackFirstElectionTimeoutMax = Rpc.timeoutMax(properties, null);
      return getTimeDuration(properties.getTimeDuration(fallbackFirstElectionTimeoutMax.getUnit()),
          FIRST_ELECTION_TIMEOUT_MAX_KEY, FIRST_ELECTION_TIMEOUT_MAX_DEFAULT,
          Rpc.TIMEOUT_MAX_KEY, fallbackFirstElectionTimeoutMax, getDefaultLog());
    }
    static void setFirstElectionTimeoutMax(RaftProperties properties, TimeDuration firstMaxDuration) {
      setTimeDuration(properties::setTimeDuration, FIRST_ELECTION_TIMEOUT_MAX_KEY, firstMaxDuration);
    }

    String REQUEST_TIMEOUT_KEY = PREFIX + ".request.timeout";
    TimeDuration REQUEST_TIMEOUT_DEFAULT = TimeDuration.valueOf(3000, TimeUnit.MILLISECONDS);
    static TimeDuration requestTimeout(RaftProperties properties) {
      return getTimeDuration(properties.getTimeDuration(REQUEST_TIMEOUT_DEFAULT.getUnit()),
          REQUEST_TIMEOUT_KEY, REQUEST_TIMEOUT_DEFAULT, getDefaultLog());
    }
    static void setRequestTimeout(RaftProperties properties, TimeDuration timeoutDuration) {
      setTimeDuration(properties::setTimeDuration, REQUEST_TIMEOUT_KEY, timeoutDuration);
    }

    String SLEEP_TIME_KEY = PREFIX + ".sleep.time";
    TimeDuration SLEEP_TIME_DEFAULT = TimeDuration.valueOf(25, TimeUnit.MILLISECONDS);
    static TimeDuration sleepTime(RaftProperties properties) {
      return getTimeDuration(properties.getTimeDuration(SLEEP_TIME_DEFAULT.getUnit()),
          SLEEP_TIME_KEY, SLEEP_TIME_DEFAULT, getDefaultLog());
    }
    static void setSleepTime(RaftProperties properties, TimeDuration sleepTime) {
      setTimeDuration(properties::setTimeDuration, SLEEP_TIME_KEY, sleepTime);
    }

    String SLOWNESS_TIMEOUT_KEY = PREFIX + ".slowness.timeout";
    TimeDuration SLOWNESS_TIMEOUT_DEFAULT = TimeDuration.valueOf(60, TimeUnit.SECONDS);
    static TimeDuration slownessTimeout(RaftProperties properties) {
      return getTimeDuration(properties.getTimeDuration(SLOWNESS_TIMEOUT_DEFAULT.getUnit()),
          SLOWNESS_TIMEOUT_KEY, SLOWNESS_TIMEOUT_DEFAULT, getDefaultLog());
    }
    static void setSlownessTimeout(RaftProperties properties, TimeDuration expiryTime) {
      setTimeDuration(properties::setTimeDuration, SLOWNESS_TIMEOUT_KEY, expiryTime);
    }
  }

  /** server retry cache related */
  interface RetryCache {
    String PREFIX = RaftServerConfigKeys.PREFIX + ".retrycache";

    /** We should set expiry time longer than total client retry to guarantee exactly-once semantic */
    String EXPIRY_TIME_KEY = PREFIX + ".expirytime";
    TimeDuration EXPIRY_TIME_DEFAULT = TimeDuration.valueOf(60, TimeUnit.SECONDS);
    static TimeDuration expiryTime(RaftProperties properties) {
      return getTimeDuration(properties.getTimeDuration(EXPIRY_TIME_DEFAULT.getUnit()),
          EXPIRY_TIME_KEY, EXPIRY_TIME_DEFAULT, getDefaultLog());
    }
    static void setExpiryTime(RaftProperties properties, TimeDuration expiryTime) {
      setTimeDuration(properties::setTimeDuration, EXPIRY_TIME_KEY, expiryTime);
    }

    String STATISTICS_EXPIRY_TIME_KEY = PREFIX + ".statistics.expirytime";
    TimeDuration STATISTICS_EXPIRY_TIME_DEFAULT = TimeDuration.valueOf(100, TimeUnit.MICROSECONDS);
    static TimeDuration statisticsExpiryTime(RaftProperties properties) {
      return getTimeDuration(properties.getTimeDuration(STATISTICS_EXPIRY_TIME_DEFAULT.getUnit()),
          STATISTICS_EXPIRY_TIME_KEY, STATISTICS_EXPIRY_TIME_DEFAULT, getDefaultLog());
    }
    static void setStatisticsExpiryTime(RaftProperties properties, TimeDuration expiryTime) {
      setTimeDuration(properties::setTimeDuration, STATISTICS_EXPIRY_TIME_KEY, expiryTime);
    }
  }

  interface Notification {
    String PREFIX = RaftServerConfigKeys.PREFIX + "." + JavaUtils.getClassSimpleName(Notification.class).toLowerCase();

    /** Timeout value to notify the state machine when there is no leader. */
    String NO_LEADER_TIMEOUT_KEY = PREFIX + ".no-leader.timeout";
    TimeDuration NO_LEADER_TIMEOUT_DEFAULT = TimeDuration.valueOf(60, TimeUnit.SECONDS);
    static TimeDuration noLeaderTimeout(RaftProperties properties) {
      return getTimeDuration(properties.getTimeDuration(NO_LEADER_TIMEOUT_DEFAULT.getUnit()),
          NO_LEADER_TIMEOUT_KEY, NO_LEADER_TIMEOUT_DEFAULT, getDefaultLog());
    }
    static void setNoLeaderTimeout(RaftProperties properties, TimeDuration noLeaderTimeout) {
      setTimeDuration(properties::setTimeDuration, NO_LEADER_TIMEOUT_KEY, noLeaderTimeout);
    }
  }

  interface LeaderElection {
    String PREFIX = RaftServerConfigKeys.PREFIX
        + "." + JavaUtils.getClassSimpleName(LeaderElection.class).toLowerCase();

    String LEADER_STEP_DOWN_WAIT_TIME_KEY = PREFIX + ".leader.step-down.wait-time";
    TimeDuration LEADER_STEP_DOWN_WAIT_TIME_DEFAULT = TimeDuration.valueOf(10, TimeUnit.SECONDS);
    static TimeDuration leaderStepDownWaitTime(RaftProperties properties) {
      return getTimeDuration(properties.getTimeDuration(LEADER_STEP_DOWN_WAIT_TIME_DEFAULT.getUnit()),
          LEADER_STEP_DOWN_WAIT_TIME_KEY, LEADER_STEP_DOWN_WAIT_TIME_DEFAULT, getDefaultLog());
    }
    static void setLeaderStepDownWaitTime(RaftProperties properties, TimeDuration leaderStepDownWaitTime) {
      setTimeDuration(properties::setTimeDuration, LEADER_STEP_DOWN_WAIT_TIME_KEY, leaderStepDownWaitTime);
    }

    String PRE_VOTE_KEY = PREFIX + ".pre-vote";
    boolean PRE_VOTE_DEFAULT = true;
    static boolean preVote(RaftProperties properties) {
      return getBoolean(properties::getBoolean, PRE_VOTE_KEY, PRE_VOTE_DEFAULT, getDefaultLog());
    }

    static void setPreVote(RaftProperties properties, boolean enablePreVote) {
      setBoolean(properties::setBoolean, PRE_VOTE_KEY, enablePreVote);
    }
  }

  static void main(String[] args) {
    printAll(RaftServerConfigKeys.class);
  }
}
