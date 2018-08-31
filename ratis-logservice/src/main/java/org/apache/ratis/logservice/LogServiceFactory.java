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
package org.apache.ratis.logservice;

import org.apache.ratis.client.RaftClient;
import org.apache.ratis.logservice.api.LogService;

public class LogServiceFactory {
  private static final LogServiceFactory INSTANCE = new LogServiceFactory();

  private LogServiceFactory() {}

  /**
   * Creates an implementation of {@link LogService} using the given {@link RaftClient}.
   *
   * @param raftClient The client to a Raft quorum.
   */
  public LogService createLogService(RaftClient raftClient) {
    //TODO return new LogServiceImpl();
    return null;
  }

  /**
   * Returns an instance of the factory to create {@link LogService} instances.
   */
  public static LogServiceFactory getInstance() {
    return INSTANCE;
  }
}
