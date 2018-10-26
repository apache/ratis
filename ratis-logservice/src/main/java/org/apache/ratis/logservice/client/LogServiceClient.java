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

package org.apache.ratis.logservice.client;

import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.logservice.api.LogInfo;
import org.apache.ratis.logservice.api.LogName;
import org.apache.ratis.logservice.api.LogServiceConfiguration;
import org.apache.ratis.logservice.api.LogStream;
import org.apache.ratis.logservice.api.LogStream.State;
import org.apache.ratis.logservice.common.Constants;
import org.apache.ratis.logservice.impl.LogStreamImpl;
import org.apache.ratis.logservice.proto.MetaServiceProtos.*;
import org.apache.ratis.logservice.util.MetaServiceProtoUtil;
import org.apache.ratis.protocol.*;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.ratis.logservice.util.LogServiceUtils.getPeersFromQuorum;


/**
 * LogServiceClient is responsible for all meta service communications such as create/get/list logs.
 * Initialized by the metaQuorum string that has list of masters as "server:port' separated by a comma.
 * An example: 'server1.example.com:9999,server2.example.com:9999,server3.example.com:9999
 */

public class LogServiceClient implements AutoCloseable {


    // the raft client for meta quorum. All DML operations are going using this client.
    final private RaftClient client;
    final private LogServiceConfiguration config;

    /**
     * Constuctor. Build raft client for meta quorum
     * @param metaQuorum
     */
    public LogServiceClient(String metaQuorum) {
        this(metaQuorum, new LogServiceConfiguration());
    }

    /**
     * Constuctor (with configuration). Build raft client for meta quorum
     * @param metaQuorum
     * @param config log serice configuration
     */
    public LogServiceClient(String metaQuorum, LogServiceConfiguration config) {
        Set<RaftPeer> peers = getPeersFromQuorum(metaQuorum);
        RaftProperties properties = new RaftProperties();
        RaftGroup meta = RaftGroup.valueOf(Constants.metaGroupID, peers);
        client = RaftClient.newBuilder()
                .setRaftGroup(meta)
                .setClientId(ClientId.randomId())
                .setProperties(properties)
                .build();
        this.config = config;
    }

    /**
     * Create a new Log request.
     * @param logName the name of the log to create
     * @return
     * @throws IOException
     */
    public LogStream createLog(LogName logName) throws IOException {
        RaftClientReply reply = client.sendReadOnly(
                () -> MetaServiceProtoUtil.toCreateLogRequestProto(logName).toByteString());
        CreateLogReplyProto message = CreateLogReplyProto.parseFrom(reply.getMessage().getContent());
        if (message.hasException()) {
            throw MetaServiceProtoUtil.toMetaServiceException(message.getException());
        }
        LogInfo info = MetaServiceProtoUtil.toLogInfo(message.getLog());
        return new LogStreamImpl(logName, getRaftClient(info), config);
    }

    /**
     * Get log request.
     * @param logName the name of the log to get
     * @return
     * @throws IOException
     */
    public LogStream getLog(LogName logName) throws IOException {
        RaftClientReply reply = client.sendReadOnly
                (() -> MetaServiceProtoUtil.toGetLogRequestProto(logName).toByteString());
        GetLogReplyProto message = GetLogReplyProto.parseFrom(reply.getMessage().getContent());
        if(message.hasException()) {
            throw MetaServiceProtoUtil.toMetaServiceException(message.getException());
        }
        LogInfo info = MetaServiceProtoUtil.toLogInfo(message.getLog());
        return new LogStreamImpl(logName, getRaftClient(info), config);
    }


    public void deleteLog(LogName logName) throws IOException {
        RaftClientReply reply = client.sendReadOnly
                (() -> MetaServiceProtoUtil.toDeleteLogRequestProto(logName).toByteString());
        DeleteLogReplyProto message = DeleteLogReplyProto.parseFrom(reply.getMessage().getContent());
        if(message.hasException()) {
            throw MetaServiceProtoUtil.toMetaServiceException(message.getException());
        }
    }

    /**
     * Return the list of available logs
     * @return
     * @throws IOException
     */
    public List<LogInfo> listLogs() throws IOException {
        RaftClientReply reply = client.sendReadOnly
                (() -> MetaServiceProtoUtil.toListLogRequestProto().toByteString());
        ListLogsReplyProto message = ListLogsReplyProto.parseFrom(reply.getMessage().getContent());
        List<LogInfoProto> infoProtos = message.getLogsList();
        List<LogInfo> infos = infoProtos.stream()
                .map(proto -> MetaServiceProtoUtil.toLogInfo(proto))
                .collect(Collectors.toList());
        return infos;
    }

    @Override
    public void close() throws Exception {
        client.close();
    }

    // Internal methods

    /**
     * Build a raft client for the particular log. Temporary here. TODO: Should be moved to LogService part
     * @param logInfo
     * @return
     */
    private RaftClient getRaftClient(LogInfo logInfo) {
        RaftProperties properties = new RaftProperties();
        return RaftClient.newBuilder().setRaftGroup(logInfo.getRaftGroup()).setProperties(properties).build();

    }

    /**
     * Archives the given log out of the state machine and into a configurable long-term storage. A log must be
     * in {@link State#CLOSED} to archive it.
     *
     * @param name The name of the log to archive.
     */
    void archiveLog(LogName name) throws IOException {
      // TODO: write me
    }

    /**
     * Moves the {@link LogStream} identified by the {@code name} from {@link State.OPEN} to {@link State.CLOSED}.
     * If the log is not {@link State#OPEN}, this method returns an error.
     *
     * @param name The name of the log to close
     */
    // TODO this name sucks, confusion WRT the Java Closeable interface.
    void closeLog(LogName name) throws IOException {
      //TODO: write me
    }

    /**
     * Updates a log with the new configuration object, overriding
     * the previous configuration.
     *
     * @param config The new configuration object
     */
    void updateConfiguration(LogName name, LogServiceConfiguration config) {
      //TODO: write me
    }

}
