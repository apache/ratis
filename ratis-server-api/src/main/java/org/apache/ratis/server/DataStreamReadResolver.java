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

import org.apache.ratis.conf.Parameters;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.statemachine.StateMachine;

import java.io.IOException;

/** Resolves read-only data stream requests independently of a Raft division. */
@FunctionalInterface
public interface DataStreamReadResolver {
  String PARAMETER_KEY = DataStreamReadResolver.class.getName();

  /** Set the resolver in the given server parameters. */
  static void set(Parameters parameters, DataStreamReadResolver resolver) {
    parameters.put(PARAMETER_KEY, resolver, DataStreamReadResolver.class);
  }

  /** Get the resolver from the given server parameters, if configured. */
  static DataStreamReadResolver get(Parameters parameters) {
    return parameters != null
        ? parameters.get(PARAMETER_KEY, DataStreamReadResolver.class)
        : null;
  }

  /**
   * @return the data API handling this request, or null to use the Raft division associated with the request.
   */
  StateMachine.DataApi resolve(RaftClientRequest request) throws IOException;
}
