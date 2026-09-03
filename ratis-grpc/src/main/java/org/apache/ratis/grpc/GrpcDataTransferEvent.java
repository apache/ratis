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
package org.apache.ratis.grpc;

import org.apache.ratis.protocol.RaftPeerId;

import java.time.Instant;
import java.util.Objects;

/** Information about the outcome of a data transfer between Ratis peers. */
public final class GrpcDataTransferEvent {
  /** The protection method used by the peer connection. */
  public enum ProtectionMethod {
    TLS,
    NONE
  }

  /** The transfer outcome. */
  public enum Result {
    SUCCESS,
    FAILURE
  }

  private final Instant timestamp;
  private final RaftPeerId source;
  private final RaftPeerId destination;
  private final ProtectionMethod protectionMethod;
  private final Result result;
  private final Throwable error;

  private GrpcDataTransferEvent(Instant timestamp, RaftPeerId source, RaftPeerId destination,
      ProtectionMethod protectionMethod, Result result, Throwable error) {
    this.timestamp = Objects.requireNonNull(timestamp, "timestamp");
    this.source = Objects.requireNonNull(source, "source");
    this.destination = Objects.requireNonNull(destination, "destination");
    this.protectionMethod = Objects.requireNonNull(protectionMethod, "protectionMethod");
    this.result = Objects.requireNonNull(result, "result");
    this.error = error;
  }

  public static GrpcDataTransferEvent success(RaftPeerId source, RaftPeerId destination,
      ProtectionMethod protectionMethod) {
    return new GrpcDataTransferEvent(Instant.now(), source, destination,
        protectionMethod, Result.SUCCESS, null);
  }

  public static GrpcDataTransferEvent failure(RaftPeerId source, RaftPeerId destination,
      ProtectionMethod protectionMethod, Throwable error) {
    return new GrpcDataTransferEvent(Instant.now(), source, destination,
        protectionMethod, Result.FAILURE, Objects.requireNonNull(error, "error"));
  }

  public Instant getTimestamp() {
    return timestamp;
  }

  public RaftPeerId getSource() {
    return source;
  }

  public RaftPeerId getDestination() {
    return destination;
  }

  public ProtectionMethod getProtectionMethod() {
    return protectionMethod;
  }

  public Result getResult() {
    return result;
  }

  public Throwable getError() {
    return error;
  }
}
