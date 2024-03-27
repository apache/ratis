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
package org.apache.ratis.shell.cli.sh;

import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.proto.RaftProtos.RaftConfigurationProto;
import org.apache.ratis.proto.RaftProtos.RaftPeerProto;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LocalCommandIntegrationTest {

  private static final String RAFT_META_CONF = "raft-meta.conf";
  private static final String NEW_RAFT_META_CONF = "new-raft-meta.conf";

  @Test
  public void testRunMethod(@TempDir Path tempDir) throws Exception {
    int index = 1;
    generateRaftConf(tempDir.resolve(RAFT_META_CONF), index);

    final StringPrintStream out = new StringPrintStream();
    RatisShell shell = new RatisShell(out.getPrintStream());
    String updatedPeersList = "peer1_Id|host1:9872,peer2_id|host2:9872,peer3_id|host3:9872";
    int ret = shell.run("local", "raftMetaConf", "-peers", updatedPeersList, "-path", tempDir.toString());
    Assertions.assertEquals(0, ret);

    // verify the contents of the new-raft-meta.conf file
    long indexFromNewConf;
    List<RaftPeerProto> peers;
    try (InputStream in = Files.newInputStream(tempDir.resolve(NEW_RAFT_META_CONF))) {
      LogEntryProto logEntry = LogEntryProto.newBuilder().mergeFrom(in).build();
      indexFromNewConf = logEntry.getIndex();
      peers = logEntry.getConfigurationEntry().getPeersList();
    }

    Assertions.assertEquals(index + 1, indexFromNewConf);

    StringBuilder sb = new StringBuilder();
    peers.stream().forEach(peer ->
        sb.append(peer.getId().toStringUtf8()).append("|").append(peer.getAddress()).append(","));
    sb.deleteCharAt(sb.length() - 1); // delete trailing comma

    Assertions.assertEquals(updatedPeersList, sb.toString());
  }


  void generateRaftConf(Path path, int index) throws IOException {
    Map<String, String> map = new HashMap<>();
    map.put("peer1_Id", "host1:9872");
    map.put("peer2_Id", "host2:9872");
    map.put("peer3_Id", "host3:9872");
    map.put("peer4_Id", "host4:9872");
    List<RaftProtos.RaftPeerProto> raftPeerProtos = new ArrayList<>();
    for (Map.Entry<String, String> en : map.entrySet()) {
      raftPeerProtos.add(RaftPeerProto.newBuilder()
          .setId(ByteString.copyFrom(en.getKey().getBytes(StandardCharsets.UTF_8))).setAddress(en.getValue())
          .setStartupRole(RaftPeerRole.FOLLOWER).build());
    }

    LogEntryProto generateLogEntryProto = LogEntryProto.newBuilder()
        .setConfigurationEntry(RaftConfigurationProto.newBuilder().addAllPeers(raftPeerProtos).build())
        .setIndex(index).build();
    try (OutputStream out = Files.newOutputStream(path)) {
      generateLogEntryProto.writeTo(out);
    }
  }

}
