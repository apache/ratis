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
package org.apache.hadoop.raft;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public interface RaftProtocol {
  class Response {
    final String id;
    final long term;
    final boolean success;

    Response(String id, long term, boolean success) {
      this.id = id;
      this.term = term;
      this.success = success;
    }

    @Override
    public String toString() {
      return "s" + id + "-t" + term + ":" + success;
    }
  }

  Response requestVote(String candidateId, long term,
      RaftLog.TermIndex lastCommitted) throws IOException;

  Response appendEntries(String leaderId, long term, RaftLog.TermIndex previous,
      long leaderCommit, RaftLog.Entry... entries) throws IOException;
}
