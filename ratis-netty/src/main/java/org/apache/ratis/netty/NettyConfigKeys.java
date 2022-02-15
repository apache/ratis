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
package org.apache.ratis.netty;

import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.thirdparty.io.netty.util.NettyRuntime;
import org.apache.ratis.util.TimeDuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;

import static org.apache.ratis.conf.ConfUtils.*;

public interface NettyConfigKeys {
  String PREFIX = "raft.netty";

  interface Server {
    Logger LOG = LoggerFactory.getLogger(Server.class);
    static Consumer<String> getDefaultLog() {
      return LOG::info;
    }

    String PREFIX = NettyConfigKeys.PREFIX + ".server";

    String PORT_KEY = PREFIX + ".port";
    int PORT_DEFAULT = 0;

    static int port(RaftProperties properties) {
      return getInt(properties::getInt,
          PORT_KEY, PORT_DEFAULT, getDefaultLog(), requireMin(0), requireMax(65536));
    }

    static void setPort(RaftProperties properties, int port) {
      setInt(properties::setInt, PORT_KEY, port);
    }
  }

  interface DataStream {
    Logger LOG = LoggerFactory.getLogger(Server.class);
    static Consumer<String> getDefaultLog() {
      return LOG::info;
    }

    String PREFIX = NettyConfigKeys.PREFIX + ".dataStream";

    String PORT_KEY = PREFIX + ".port";
    int PORT_DEFAULT = 0;

    static int port(RaftProperties properties) {
      return getInt(properties::getInt,
          PORT_KEY, PORT_DEFAULT, getDefaultLog(), requireMin(0), requireMax(65536));
    }

    static void setPort(RaftProperties properties, int port) {
      setInt(properties::setInt, PORT_KEY, port);
    }

    String CLIENT_WORKER_GROUP_SIZE_KEY = PREFIX + ".client.worker-group.size";
    int CLIENT_WORKER_GROUP_SIZE_DEFAULT = Math.max(1, NettyRuntime.availableProcessors() * 2);

    static int clientWorkerGroupSize(RaftProperties properties) {
      return getInt(properties::getInt, CLIENT_WORKER_GROUP_SIZE_KEY,
          CLIENT_WORKER_GROUP_SIZE_DEFAULT, getDefaultLog(), requireMin(1), requireMax(65536));
    }

    static void setClientWorkerGroupSize(RaftProperties properties, int clientWorkerGroupSize) {
      setInt(properties::setInt, CLIENT_WORKER_GROUP_SIZE_KEY, clientWorkerGroupSize);
    }

    String CLIENT_WORKER_GROUP_SHARE_KEY = PREFIX + ".client.worker-group.share";
    boolean CLIENT_WORKER_GROUP_SHARE_DEFAULT = false;

    static boolean clientWorkerGroupShare(RaftProperties properties) {
      return getBoolean(properties::getBoolean, CLIENT_WORKER_GROUP_SHARE_KEY,
          CLIENT_WORKER_GROUP_SHARE_DEFAULT, getDefaultLog());
    }

    static void setClientWorkerGroupShare(RaftProperties properties, boolean clientWorkerGroupShare) {
      setBoolean(properties::setBoolean, CLIENT_WORKER_GROUP_SHARE_KEY, clientWorkerGroupShare);
    }

    String CLIENT_REPLY_QUEUE_GRACE_PERIOD_KEY = PREFIX + ".client.reply.queue.grace-period";
    TimeDuration CLIENT_REPLY_QUEUE_GRACE_PERIOD_DEFAULT = TimeDuration.ONE_SECOND;

    static TimeDuration clientReplyQueueGracePeriod(RaftProperties properties) {
      return getTimeDuration(properties.getTimeDuration(CLIENT_REPLY_QUEUE_GRACE_PERIOD_DEFAULT.getUnit()),
          CLIENT_REPLY_QUEUE_GRACE_PERIOD_KEY, CLIENT_REPLY_QUEUE_GRACE_PERIOD_DEFAULT, getDefaultLog());
    }

    static void setClientReplyQueueGracePeriod(RaftProperties properties, TimeDuration timeoutDuration) {
      setTimeDuration(properties::setTimeDuration, CLIENT_REPLY_QUEUE_GRACE_PERIOD_KEY, timeoutDuration);
    }

    interface Server {
      String PREFIX = NettyConfigKeys.PREFIX + ".server";

      String USE_EPOLL_KEY = PREFIX + ".use-epoll";
      boolean USE_EPOLL_DEFAULT = false;
      static boolean useEpoll(RaftProperties properties) {
        return getBoolean(properties::getBoolean, USE_EPOLL_KEY, USE_EPOLL_DEFAULT, getDefaultLog());
      }
      static void setUseEpoll(RaftProperties properties, boolean enable) {
        setBoolean(properties::setBoolean, USE_EPOLL_KEY, enable);
      }

      String BOSS_GROUP_SIZE_KEY = PREFIX + ".boss-group.size";
      int BOSS_GROUP_SIZE_DEFAULT = 0;
      static int bossGroupSize(RaftProperties properties) {
        return getInt(properties::getInt, BOSS_GROUP_SIZE_KEY, BOSS_GROUP_SIZE_DEFAULT, getDefaultLog(),
            requireMin(0), requireMax(65536));
      }
      static void setBossGroupSize(RaftProperties properties, int num) {
        setInt(properties::setInt, BOSS_GROUP_SIZE_KEY, num);
      }

      String WORKER_GROUP_SIZE_KEY = PREFIX + ".worker-group.size";
      int WORKER_GROUP_SIZE_DEFAULT = 0;
      static int workerGroupSize(RaftProperties properties) {
        return getInt(properties::getInt, WORKER_GROUP_SIZE_KEY, WORKER_GROUP_SIZE_DEFAULT, getDefaultLog(),
            requireMin(0), requireMax(65536));
      }
      static void setWorkerGroupSize(RaftProperties properties, int num) {
        setInt(properties::setInt, WORKER_GROUP_SIZE_KEY, num);
      }
    }
  }

  static void main(String[] args) {
    printAll(NettyConfigKeys.class);
  }
}
