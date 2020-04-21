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
package org.apache.ratis.hadooprpc;

public final class HadoopConstants {
  private HadoopConstants() {
    //Never constructed
  }
  public static final String RAFT_SERVER_KERBEROS_PRINCIPAL_KEY = "raft.server.kerberos.principal";
  public static final String RAFT_CLIENT_KERBEROS_PRINCIPAL_KEY = "raft.client.kerberos.principal";
  public static final String RAFT_SERVER_PROTOCOL_NAME = "org.apache.hadoop.raft.server.protocol.RaftServerProtocol";
  public static final String COMBINED_CLIENT_PROTOCOL_NAME = "org.apache.ratis.hadooprpc.client.CombinedClientProtocol";
}
