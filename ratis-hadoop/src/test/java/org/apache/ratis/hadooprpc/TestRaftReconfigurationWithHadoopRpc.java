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

import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.Client;
import org.apache.log4j.Level;
import org.apache.ratis.MiniRaftCluster;
import org.apache.ratis.server.impl.RaftReconfigurationBaseTest;

import java.io.IOException;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_TIMEOUT_KEY;

public class TestRaftReconfigurationWithHadoopRpc
    extends RaftReconfigurationBaseTest {
  static {
    ((Log4JLogger) Client.LOG).getLogger().setLevel(Level.ERROR);
  }

  @Override
  public MiniRaftCluster getCluster(int peerNum) throws IOException {
    final Configuration hadoopConf = new Configuration();
    hadoopConf.setInt(IPC_CLIENT_CONNECT_TIMEOUT_KEY, 1000);
    hadoopConf.setInt(IPC_CLIENT_CONNECT_MAX_RETRIES_KEY, 0);
    return MiniRaftClusterWithHadoopRpc.FACTORY.newCluster(peerNum, prop, hadoopConf);
  }
}
