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
package org.apache.ratis.server.raftlog;

import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.util.MemoizedSupplier;
import org.apache.ratis.util.SizeInBytes;

import java.util.function.Supplier;

/**
 * {@link RaftLog} related {@link RaftServerConfigKeys}.
 */
public final class RaftLogConf {
  private static final Supplier<RaftLogConf> DEFAULT = MemoizedSupplier.valueOf(() -> get(new RaftProperties()));

  /** @return the default conf. */
  public static RaftLogConf get() {
    return DEFAULT.get();
  }

  /** @return the conf from the given properties. */
  public static RaftLogConf get(RaftProperties properties) {
    return new RaftLogConf(properties);
  }

  private final long segmentMaxSize;

  private final int maxCachedSegments;
  private final long maxSegmentCacheSize;
  private final SizeInBytes maxOpSize;
  private final boolean unsafeFlush;

  private RaftLogConf(RaftProperties properties) {
    this.segmentMaxSize = RaftServerConfigKeys.Log.segmentSizeMax(properties).getSize();
    this.maxCachedSegments = RaftServerConfigKeys.Log.segmentCacheNumMax(properties);
    this.maxSegmentCacheSize = RaftServerConfigKeys.Log.segmentCacheSizeMax(properties).getSize();
    this.unsafeFlush = RaftServerConfigKeys.Log.unsafeFlushEnabled(properties);

    this.maxOpSize = RaftServerConfigKeys.Log.Appender.bufferByteLimit(properties);
  }

  public long getSegmentMaxSize() {
    return segmentMaxSize;
  }

  public int getMaxCachedSegments() {
    return maxCachedSegments;
  }

  public long getMaxSegmentCacheSize() {
    return maxSegmentCacheSize;
  }

  public SizeInBytes getMaxOpSize() {
    return maxOpSize;
  }

  public boolean isUnsafeFlush() {
    return unsafeFlush;
  }
}
