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
package org.apache.ratis.shell.cli.sh.local;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.proto.RaftProtos.RaftConfigurationProto;
import org.apache.ratis.proto.RaftProtos.RaftPeerProto;
import org.apache.ratis.proto.RaftProtos.RaftPeerRole;
import org.apache.ratis.shell.cli.RaftUtils;
import org.apache.ratis.shell.cli.sh.command.AbstractCommand;
import org.apache.ratis.shell.cli.sh.command.Context;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * Command for generate a new raft-meta.conf file based on original raft-meta.conf and new peers,
 * which is used to move a raft node to a new node.
 */
public class RaftMetaConfCommand extends AbstractCommand {
  public static final String PEER_OPTION_NAME = "peers";
  public static final String PATH_OPTION_NAME = "path";

  private static final String RAFT_META_CONF = "raft-meta.conf";
  private static final String NEW_RAFT_META_CONF = "new-raft-meta.conf";

  /**
   * @param context command context
   */
  public RaftMetaConfCommand(Context context) {
    super(context);
  }

  @Override
  public String getCommandName() {
    return "raftMetaConf";
  }

  @Override
  public int run(CommandLine cl) throws IOException {
    String peersStr = cl.getOptionValue(PEER_OPTION_NAME);
    String path = cl.getOptionValue(PATH_OPTION_NAME);
    if (peersStr == null || path == null || peersStr.isEmpty() || path.isEmpty()) {
      printf("peers or path can't be empty.");
      return -1;
    }
    List<RaftPeerProto> raftPeerProtos = new ArrayList<>();
    for (String address : peersStr.split(",")) {
      String peerId = RaftUtils.getPeerId(parseInetSocketAddress(address)).toString();
      raftPeerProtos.add(RaftPeerProto.newBuilder()
          .setId(ByteString.copyFrom(peerId.getBytes(StandardCharsets.UTF_8))).setAddress(address)
          .setStartupRole(RaftPeerRole.FOLLOWER).build());
    }
    try (InputStream in = Files.newInputStream(Paths.get(path, RAFT_META_CONF));
         OutputStream out = Files.newOutputStream(Paths.get(path, NEW_RAFT_META_CONF))) {
      long index = LogEntryProto.newBuilder().mergeFrom(in).build().getIndex();
      println("Index in the original file is: " + index);
      LogEntryProto generateLogEntryProto = LogEntryProto.newBuilder()
          .setConfigurationEntry(RaftConfigurationProto.newBuilder()
              .addAllPeers(raftPeerProtos).build())
          .setIndex(index + 1).build();
      printf("Generate new LogEntryProto info is:\n" + generateLogEntryProto);
      generateLogEntryProto.writeTo(out);
    }
    return 0;
  }

  @Override
  public String getUsage() {
    return String.format("%s"
            + " -%s <PEER0_HOST:PEER0_PORT,PEER1_HOST:PEER1_PORT,PEER2_HOST:PEER2_PORT>"
            + " -%s <PARENT_PATH_OF_RAFT_META_CONF>",
        getCommandName(), PEER_OPTION_NAME, PATH_OPTION_NAME);
  }

  @Override
  public String getDescription() {
    return description();
  }

  @Override
  public Options getOptions() {
    return new Options()
        .addOption(
            Option.builder()
                .option(PEER_OPTION_NAME)
                .hasArg()
                .required()
                .desc("Peer addresses seperated by comma")
            .build())
        .addOption(
            Option.builder()
                .option(PATH_OPTION_NAME)
                .hasArg()
                .required()
                .desc("The parent path of raft-meta.conf")
            .build());
  }

  /**
   * @return command's description
   */
  public static String description() {
    return "Generate a new raft-meta.conf file based on original raft-meta.conf and new peers.";
  }
}

