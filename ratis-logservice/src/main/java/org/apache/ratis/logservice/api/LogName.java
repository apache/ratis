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
package org.apache.ratis.logservice.api;

import static java.util.Objects.requireNonNull;

import java.util.Objects;

import org.apache.ratis.logservice.proto.LogServiceProtos;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;

/**
 * Identifier to uniquely identify a {@link LogStream}.
 */
public class LogName {
  // It's pretty likely that what uniquely defines a LogStream
  // to change over time. We should account for this by making an
  // API which can naturally evolve.
  private final String name;

  private LogName(String name) {
    this.name = requireNonNull(name);
  }

  /**
   * Returns the unique name which identifies a LogStream.
   *
   * Impl Note: This class uses a String to uniquely identify this LogName (and the corresponding LogStream)
   * from others. This is purely an implementation detail; the intent is that any data should be capable
   * of identifying one LogStream/LogName from another. Users need only know how to construct a {@link LogName}
   * and then use that in their application.
   */
  public String getName() {
    return name;
  }

  @Override public boolean equals(Object o) {
    if (!(o instanceof LogName)) {
      return false;
    }
    return Objects.equals(name, ((LogName) o).getName());
  }

  @Override public int hashCode() {
    return name.hashCode();
  }

  @Override public String toString() {
    return "LogName['" + name + "']";
  }

  /**
   * Length of a log's name
   * @return length
   */
  public int getLength() {
    return name.length();
  }

  /**
   * Creates a {@link LogName} given the provided string.
   */
  public static LogName of(String name) {
    // TODO Limit allowed characters in the name?
    return new LogName(name);
  }

  public static LogName parseFrom(ByteString logName)
      throws InvalidProtocolBufferException {
    LogServiceProtos.LogNameProto logNameProto = LogServiceProtos.LogNameProto.parseFrom(logName);
    return new LogName(logNameProto.getName());
  }
}
