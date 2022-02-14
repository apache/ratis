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
package org.apache.ratis.server.leader;

import org.apache.ratis.proto.RaftProtos.AppendEntriesRequestProto;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.util.JavaUtils;

import java.util.List;

/**
 * States for leader only.
 */
public interface LeaderState {
  /** The reasons that this leader steps down and becomes a follower. */
  enum StepDownReason {
    HIGHER_TERM, HIGHER_PRIORITY, LOST_MAJORITY_HEARTBEATS, STATE_MACHINE_EXCEPTION, JVM_PAUSE, FORCE;

    private final String longName = JavaUtils.getClassSimpleName(getClass()) + ":" + name();

    @Override
    public String toString() {
      return longName;
    }
  }

  /** Restart the given {@link LogAppender}. */
  void restart(LogAppender appender);

  /** @return a new {@link AppendEntriesRequestProto} object. */
  AppendEntriesRequestProto newAppendEntriesRequestProto(FollowerInfo follower,
      List<LogEntryProto> entries, TermIndex previous, long callId);

  /** Check if the follower is healthy. */
  void checkHealth(FollowerInfo follower);

  /** Handle the event that the follower has replied a term. */
  boolean onFollowerTerm(FollowerInfo follower, long followerTerm);

  /** Handle the event that the follower has replied a commit index. */
  void onFollowerCommitIndex(FollowerInfo follower, long commitIndex);

  /** Handle the event that the follower has replied a success append entries. */
  void onFollowerSuccessAppendEntries(FollowerInfo follower);

  /** Check if a follower is bootstrapping. */
  boolean isFollowerBootstrapping(FollowerInfo follower);

}
