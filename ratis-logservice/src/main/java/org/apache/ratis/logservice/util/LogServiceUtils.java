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
package org.apache.ratis.logservice.util;

import org.apache.hadoop.fs.Path;
import org.apache.ratis.logservice.api.LogName;
import org.apache.ratis.protocol.RaftPeer;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class LogServiceUtils {
    private LogServiceUtils() {
    }

    public static Set<RaftPeer> getPeersFromIds(String identity) {

        return Stream.of(identity.split(",")).map(elem ->
                RaftPeer.newBuilder().setId(elem).setAddress(elem.replace('_', ':')).build()
        ).collect(Collectors.toSet());
    }

    public static Set<RaftPeer> getPeersFromQuorum(String identity) {
        return Stream.of(identity.split(",")).map(elem ->
                RaftPeer.newBuilder().setId(elem.replace(':', '_')).setAddress(elem).build()
        ).collect(Collectors.toSet());
    }

    public static String getHostName() {
        try (DatagramSocket socket = new DatagramSocket()) {
            socket.connect(InetAddress.getByName("8.8.8.8"), 10002);
            return socket.getLocalAddress().getHostName();
        } catch (IOException e) {
            return "localhost";
        }

    }

    public static String getArchiveLocationForLog(String location, LogName logName) {
        return location + "/" + logName.getName();
    }

    public static String getRolledPathForArchiveWriter(Path path, long lastWrittenId) {
        return path + "_recordId_" + lastWrittenId;
    }

    public static Integer getRecordIdFromRolledArchiveFile(Path path) {
        String[] splits = path.getName().split("_recordId_");
        if (splits.length != 2) {
            //currently written file, should be read last
            return Integer.MAX_VALUE;
        }
        return Integer.parseInt(splits[1]);
    }

}
