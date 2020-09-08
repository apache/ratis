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

package org.apache.ratis.logservice.common;

import org.apache.ratis.protocol.RaftGroupId;

import java.util.UUID;

public final class Constants {
    private Constants() {
    }

    public static final UUID META_GROUP_UUID = new UUID(0,1);
    public static final RaftGroupId META_GROUP_ID = RaftGroupId.valueOf(META_GROUP_UUID);

    public static final UUID SERVERS_GROUP_UUID = new UUID(0,2);
    public static final RaftGroupId SERVERS_GROUP_ID =
        RaftGroupId.valueOf(SERVERS_GROUP_UUID);
    public static final String META_SERVER_PORT_KEY = "logservice.metaserver.port";
    public static final String META_SERVER_HOSTNAME_KEY = "logservice.metaserver.hostname";
    public static final String META_SERVER_WORKDIR_KEY = "logservice.metaserver.workdir";
    public static final String LOG_SERVER_PORT_KEY = "logservice.logserver.port";
    public static final String LOG_SERVER_HOSTNAME_KEY = "logservice.logserver.hostname";
    public static final String LOG_SERVER_WORKDIR_KEY = "logservice.logserver.workdir";
    public static final String LOG_SERVICE_METAQUORUM_KEY = "logservice.metaquorum";
    public static final String LOG_SERVICE_META_SERVER_GROUPID_KEY =
        "logservice.metaserver.groupid";
    public static final String LOG_SERVICE_LOG_SERVER_GROUPID_KEY = "logservice.logserver.groupid";
    public static final String LOG_SERVICE_ARCHIVAL_LOCATION_KEY = "logservice.archival.location";
    /*
     * Raft properties
     */
    public static final String LOG_SERVICE_LEADER_ELECTION_TIMEOUT_MIN_KEY =
        "logservice.raft.leader.election.timeout.min"; // in ms
    public static final String LOG_SERVICE_LEADER_ELECTION_TIMEOUT_MAX_KEY =
        "logservice.raft.leader.election.timeout.max"; // in ms
    public static final String LOG_SERVICE_RPC_TIMEOUT_KEY =
        "logservice.raft.rpc.timeout"; // in ms

    public static final long DEFAULT_LOG_SERVICE_LEADER_ELECTION_TIMEOUT_MIN = 1000;
    public static final long DEFAULT_LOG_SERVICE_LEADER_ELECTION_TIMEOUT_MAX = 1200;
    public static final long DEFAULT_RPC_TIMEOUT = 100000;// 100 sec (?)

    public static final String RATIS_RAFT_SEGMENT_SIZE_KEY = "ratis.raft.segment.size";
    public static final long DEFAULT_RATIS_RAFT_SEGMENT_SIZE = 32 * 1024 * 1024L;// 32MB

    public static final String LOG_SERVICE_HEARTBEAT_INTERVAL_KEY =
            "logservice.heartbeat.interval"; // in ms
    public static final long DEFAULT_HEARTBEAT_INTERVAL = 3000;// 3 seconds

    public static final String LOG_SERVICE_PEER_FAILURE_DETECTION_PERIOD_KEY =
            "logservice.peer.failure.detection.period"; // in ms
    public static final long DEFAULT_PEER_FAILURE_DETECTION_PERIOD = 60000;// 1 min.

}
