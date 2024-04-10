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

import org.apache.ratis.proto.RaftProtos.RaftPeerRole;
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
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class LocalCommandIntegrationTest {

  private static final String RAFT_META_CONF = "raft-meta.conf";
  private static final String NEW_RAFT_META_CONF = "new-raft-meta.conf";
  private static Pattern p = Pattern.compile("(?:\\w+\\|\\w+:\\d+,?)+");


  @Test
  public void testDuplicatedPeerAddresses() throws Exception {
    String[] duplicatedAddressesList = {"peer1_ID1|host1:9872,peer2_ID|host2:9872,peer1_ID2|host1:9872",
        "host1:9872,host2:9872,host1:9872"};

    testDuplicatedPeers(duplicatedAddressesList, "address", "host1:9872");
  }

  @Test
  public void testDuplicatedPeerIds() throws Exception {
    String[] duplicatedIdsList = {"peer1_ID1|host1:9872,peer2_ID|host2:9872,peer1_ID1|host3:9872"};

    testDuplicatedPeers(duplicatedIdsList, "ID", "peer1_ID1");
  }

  private void testDuplicatedPeers(String[] peersList, String expectedErrorMessagePart, String expectedDuplicatedValue) throws Exception {
    for (String peersStr : peersList) {
      StringPrintStream out = new StringPrintStream();
      RatisShell shell = new RatisShell(out.getPrintStream());
      int ret = shell.run("local", "raftMetaConf", "-peers", peersStr, "-path", "test");
      Assertions.assertEquals(-1, ret);
      String message = out.toString().trim();
      Assertions.assertEquals(String.format("Found duplicated %s: %s. Please make sure the %s of peer have no duplicated value.",
          expectedErrorMessagePart, expectedDuplicatedValue, expectedErrorMessagePart), message);
    }
  }

  @Test
  public void testRunMethod(@TempDir Path tempDir) throws Exception {
    int index = 1;
    generateRaftConf(tempDir.resolve(RAFT_META_CONF), index);

     String[] testPeersListArray = {"peer1_ID|host1:9872,peer2_ID|host2:9872,peer3_ID|host3:9872",
      "host1:9872,host2:9872,host3:9872"};

    for (String peersListStr : testPeersListArray) {
      generateRaftConf(tempDir, index);
      StringPrintStream out = new StringPrintStream();
      RatisShell shell = new RatisShell(out.getPrintStream());
      int ret = shell.run("local", "raftMetaConf", "-peers", peersListStr, "-path", tempDir.toString());
      Assertions.assertEquals(0, ret);

      // read & verify the contents of the new-raft-meta.conf file
      long indexFromNewConf;
      List<RaftPeerProto> peers;
      try (InputStream in = Files.newInputStream(tempDir.resolve(NEW_RAFT_META_CONF))) {
        LogEntryProto logEntry = LogEntryProto.newBuilder().mergeFrom(in).build();
        indexFromNewConf = logEntry.getIndex();
        peers = logEntry.getConfigurationEntry().getPeersList();
      }

      Assertions.assertEquals(index + 1, indexFromNewConf);

      String peersListStrFromNewMetaConf;
      if (containsPeerId(peersListStr)) {
        peersListStrFromNewMetaConf = peers.stream()
            .map(peer -> peer.getId().toStringUtf8() + "|" + peer.getAddress())
            .collect(Collectors.joining(","));
      } else {
        peersListStrFromNewMetaConf = peers.stream().map(RaftPeerProto::getAddress)
            .collect(Collectors.joining(","));
      }

      Assertions.assertEquals(peersListStr, peersListStrFromNewMetaConf);
    }
  }


  private void generateRaftConf(Path path, int index) throws IOException {
    Map<String, String> map = new HashMap<>();
    map.put("peer1_ID", "host1:9872");
    map.put("peer2_ID", "host2:9872");
    map.put("peer3_ID", "host3:9872");
    map.put("peer4_ID", "host4:9872");
    List<RaftPeerProto> raftPeerProtos = new ArrayList<>();
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

  private boolean containsPeerId(String str) {
    return p.matcher(str).find();
  }

}
