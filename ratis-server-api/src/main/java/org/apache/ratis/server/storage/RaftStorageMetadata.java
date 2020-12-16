/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.ratis.server.storage;

import org.apache.ratis.proto.RaftProtos.TermIndexProto;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.util.JavaUtils;

import java.util.Objects;
import java.util.Optional;

/**
 * The metadata for a raft storage.
 *
 * This is a value-based class.
 */
public final class RaftStorageMetadata {
  private static final RaftStorageMetadata DEFAULT = valueOf(
      TermIndexProto.getDefaultInstance().getTerm(), RaftPeerId.valueOf(""));

  public static RaftStorageMetadata getDefault() {
    return DEFAULT;
  }

  public static RaftStorageMetadata valueOf(long term, RaftPeerId votedFor) {
    return new RaftStorageMetadata(term, votedFor);
  }

  private final long term;
  private final RaftPeerId votedFor;

  private RaftStorageMetadata(long term, RaftPeerId votedFor) {
    this.term = term;
    this.votedFor = Optional.ofNullable(votedFor).orElseGet(() -> getDefault().getVotedFor());
  }

  /** @return the term. */
  public long getTerm() {
    return term;
  }

  /** @return the server it voted for. */
  public RaftPeerId getVotedFor() {
    return votedFor;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    } else if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    final RaftStorageMetadata that = (RaftStorageMetadata) obj;
    return this.term == that.term && Objects.equals(this.votedFor, that.votedFor);
  }

  @Override
  public int hashCode() {
    return Objects.hash(term, votedFor);
  }

  @Override
  public String toString() {
    return JavaUtils.getClassSimpleName(getClass()) + "{term=" + term + ", votedFor=" + votedFor + '}';
  }
}
