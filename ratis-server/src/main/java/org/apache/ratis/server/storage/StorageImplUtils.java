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
package org.apache.ratis.server.storage;

import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.util.IOUtils;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.TimeDuration;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

public final class StorageImplUtils {

  private StorageImplUtils() {
    //Never constructed
  }

  /** Create a {@link RaftStorageImpl}. */
  public static RaftStorageImpl newRaftStorage(File dir, RaftServerConfigKeys.Log.CorruptionPolicy logCorruptionPolicy,
      RaftStorage.StartupOption option, long storageFeeSpaceMin) throws IOException {
    RaftStorage.LOG.debug("newRaftStorage: {}, {}, {}, {}",dir, logCorruptionPolicy, option, storageFeeSpaceMin);

    final TimeDuration sleepTime = TimeDuration.valueOf(500, TimeUnit.MILLISECONDS);
    final RaftStorageImpl raftStorage;
    try {
      // attempt multiple times to avoid temporary bind exception
      raftStorage = JavaUtils.attemptRepeatedly(
          () -> new RaftStorageImpl(dir, logCorruptionPolicy, option, storageFeeSpaceMin),
          5, sleepTime, "new RaftStorageImpl", RaftStorage.LOG);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw IOUtils.toInterruptedIOException(
          "Interrupted when creating RaftStorage " + dir, e);
    }
    return raftStorage;
  }
}
