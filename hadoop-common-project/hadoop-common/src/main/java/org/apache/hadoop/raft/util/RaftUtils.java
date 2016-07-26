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
package org.apache.hadoop.raft.util;

import com.google.protobuf.ByteString;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.raft.proto.RaftProtos.ClientMessageEntryProto;
import org.apache.hadoop.raft.proto.RaftProtos.LogEntryProto;
import org.apache.hadoop.raft.proto.RaftProtos.RaftConfigurationProto;
import org.apache.hadoop.raft.proto.RaftProtos.RaftPeerProto;
import org.apache.hadoop.raft.protocol.Message;
import org.apache.hadoop.raft.protocol.RaftPeer;
import org.apache.hadoop.raft.server.RaftConfiguration;
import org.apache.hadoop.util.ExitUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public abstract class RaftUtils {
  private static final Logger LOG = LoggerFactory.getLogger(RaftUtils.class);

  public static InterruptedIOException toInterruptedIOException(
      String message, InterruptedException e) {
    final InterruptedIOException iioe = new InterruptedIOException(message);
    iioe.initCause(e);
    return iioe;
  }

  public static InetSocketAddress newInetSocketAddress(String address) {
    try {
      return NetUtils.createSocketAddr(address);
    } catch (Exception e) {
      return null;
    }
  }

  public static ByteString getByteString(byte[] bytes) {
    // return singleton to reduce object allocation
    return (bytes.length == 0) ? ByteString.EMPTY : ByteString.copyFrom(bytes);
  }

  public static Iterable<RaftPeerProto> convertPeersToProtos(
      Collection<RaftPeer> peers) {
    List<RaftPeerProto> protos = new ArrayList<>(peers.size());
    for (RaftPeer p : peers) {
      protos.add(RaftPeerProto.newBuilder().setId(p.getId()).build());
    }
    return protos;
  }

  public static RaftConfigurationProto convertConfToProto(RaftConfiguration conf) {
    return RaftConfigurationProto.newBuilder()
        .addAllPeers(convertPeersToProtos(conf.getPeersInConf()))
        .addAllOldPeers(convertPeersToProtos(conf.getPeersInOldConf()))
        .build();
  }

  public static RaftPeer convertProtoToRaftPeer(RaftPeerProto proto) {
    return new RaftPeer(proto.getId());
  }

  public static RaftPeer[] convertProtoToRaftPeerArray(List<RaftPeerProto> protos) {
    RaftPeer[] peers = new RaftPeer[protos.size()];
    for (int i = 0; i < peers.length; i++) {
      peers[i] = convertProtoToRaftPeer(protos.get(i));
    }
    return peers;
  }

  public static LogEntryProto convertConfToLogEntryProto(RaftConfiguration conf,
      long term, long index) {
    RaftConfigurationProto confProto = convertConfToProto(conf);
    return LogEntryProto.newBuilder().setTerm(term).setIndex(index)
        .setType(LogEntryProto.Type.CONFIGURATION)
        .setConfigurationEntry(confProto).build();
  }

  public static LogEntryProto convertRequestToLogEntryProto(Message message,
      long term, long index) {
    ClientMessageEntryProto m = ClientMessageEntryProto.newBuilder()
        .setContent(getByteString(message.getContent())).build();
    return LogEntryProto.newBuilder().setTerm(term).setIndex(index)
        .setType(LogEntryProto.Type.CLIENT_MESSAGE)
        .setClientMessageEntry(m).build();
  }

  public static RaftConfiguration convertProtoToConf(long index,
      RaftConfigurationProto proto) {
    RaftPeer[] peers = convertProtoToRaftPeerArray(proto.getPeersList());
    if (proto.getOldPeersCount() > 0) {
      RaftPeer[] oldPeers = convertProtoToRaftPeerArray(proto.getPeersList());
      return RaftConfiguration.composeOldNewConf(peers, oldPeers, index);
    } else {
      return RaftConfiguration.composeConf(peers, index);
    }
  }

  public static boolean isConfigurationLogEntry(LogEntryProto entry) {
    return entry.getType() == LogEntryProto.Type.CONFIGURATION;
  }

  public static void truncateFile(File f, long target) throws IOException {
    try (FileOutputStream out = new FileOutputStream(f, true)) {
      out.getChannel().truncate(target);
    }
  }

  public static void terminate(Throwable t, String message, Logger LOG) {
    LOG.error(message, t);
    ExitUtil.terminate(1, message);
  }

  /**
   * Interprets the passed string as a URI. In case of error it
   * assumes the specified string is a file.
   *
   * @param s the string to interpret
   * @return the resulting URI
   */
  public static URI stringAsURI(String s) throws IOException {
    URI u = null;
    // try to make a URI
    try {
      u = new URI(s);
    } catch (URISyntaxException e){
      LOG.error("Syntax error in URI " + s
          + ". Please check hdfs configuration.", e);
    }

    // if URI is null or scheme is undefined, then assume it's file://
    if(u == null || u.getScheme() == null){
      LOG.warn("Path " + s + " should be specified as a URI "
          + "in configuration files. Please update configuration.");
      u = fileAsURI(new File(s));
    }
    return u;
  }

  /**
   * Converts the passed File to a URI. This method trims the trailing slash if
   * one is appended because the underlying file is in fact a directory that
   * exists.
   *
   * @param f the file to convert
   * @return the resulting URI
   */
  public static URI fileAsURI(File f) throws IOException {
    URI u = f.getCanonicalFile().toURI();

    // trim the trailing slash, if it's present
    if (u.getPath().endsWith("/")) {
      String uriAsString = u.toString();
      try {
        u = new URI(uriAsString.substring(0, uriAsString.length() - 1));
      } catch (URISyntaxException e) {
        throw new IOException(e);
      }
    }

    return u;
  }
}
