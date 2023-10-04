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

import org.apache.ratis.conf.ConfUtils;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.security.TlsConf;
import org.apache.ratis.thirdparty.io.netty.util.NettyRuntime;
import org.apache.ratis.util.TimeDuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
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

    String HOST_KEY = PREFIX + ".host";
    String HOST_DEFAULT = null;

    String PORT_KEY = PREFIX + ".port";
    int PORT_DEFAULT = 0;

    String USE_EPOLL_KEY = PREFIX + ".use-epoll";
    boolean USE_EPOLL_DEFAULT = true;

    static String host(RaftProperties properties) {
      return get(properties::get, HOST_KEY, HOST_DEFAULT, getDefaultLog());
    }

    static void setHost(RaftProperties properties, String host) {
      set(properties::set, HOST_KEY, host);
    }

    static int port(RaftProperties properties) {
      return getInt(properties::getInt,
          PORT_KEY, PORT_DEFAULT, getDefaultLog(), requireMin(0), requireMax(65536));
    }

    static void setPort(RaftProperties properties, int port) {
      setInt(properties::setInt, PORT_KEY, port);
    }

    static boolean useEpoll(RaftProperties properties) {
      return getBoolean(properties::getBoolean, USE_EPOLL_KEY, USE_EPOLL_DEFAULT, getDefaultLog());
    }
    static void setUseEpoll(RaftProperties properties, boolean enable) {
      setBoolean(properties::setBoolean, USE_EPOLL_KEY, enable);
    }
  }

  interface DataStream {
    Logger LOG = LoggerFactory.getLogger(DataStream.class);
    static Consumer<String> getDefaultLog() {
      return LOG::info;
    }

    String PREFIX = NettyConfigKeys.PREFIX + ".dataStream";

    String HOST_KEY = PREFIX + ".host";
    String HOST_DEFAULT = null;

    String PORT_KEY = PREFIX + ".port";
    int PORT_DEFAULT = 0;

    static String host(RaftProperties properties) {
      return get(properties::get, HOST_KEY, HOST_DEFAULT, getDefaultLog());
    }

    static void setHost(RaftProperties properties, String host) {
      set(properties::set, HOST_KEY, host);
    }

    static int port(RaftProperties properties) {
      return getInt(properties::getInt,
          PORT_KEY, PORT_DEFAULT, getDefaultLog(), requireMin(0), requireMax(65536));
    }

    static void setPort(RaftProperties properties, int port) {
      setInt(properties::setInt, PORT_KEY, port);
    }

    interface Client {
      String PREFIX = DataStream.PREFIX + ".client";

      String TLS_CONF_PARAMETER = PREFIX + ".tls.conf";
      Class<TlsConf> TLS_CONF_CLASS = TlsConf.class;
      static TlsConf tlsConf(Parameters parameters) {
        return getTlsConf(key -> parameters.get(key, TLS_CONF_CLASS), TLS_CONF_PARAMETER, getDefaultLog());
      }
      static void setTlsConf(Parameters parameters, TlsConf conf) {
        LOG.info("setTlsConf " + conf);
        ConfUtils.setTlsConf((key, value) -> parameters.put(key, value, TLS_CONF_CLASS), TLS_CONF_PARAMETER, conf);
      }

      String USE_EPOLL_KEY = PREFIX + ".use-epoll";
      boolean USE_EPOLL_DEFAULT = true;
      static boolean useEpoll(RaftProperties properties) {
        return getBoolean(properties::getBoolean, USE_EPOLL_KEY, USE_EPOLL_DEFAULT, getDefaultLog());
      }
      static void setUseEpoll(RaftProperties properties, boolean enable) {
        setBoolean(properties::setBoolean, USE_EPOLL_KEY, enable);
      }

      String WORKER_GROUP_SIZE_KEY = PREFIX + ".worker-group.size";
      int WORKER_GROUP_SIZE_DEFAULT = Math.max(1, NettyRuntime.availableProcessors() * 2);
      static int workerGroupSize(RaftProperties properties) {
        return getInt(properties::getInt, WORKER_GROUP_SIZE_KEY,
            WORKER_GROUP_SIZE_DEFAULT, getDefaultLog(), requireMin(1), requireMax(65536));
      }
      static void setWorkerGroupSize(RaftProperties properties, int clientWorkerGroupSize) {
        setInt(properties::setInt, WORKER_GROUP_SIZE_KEY, clientWorkerGroupSize);
      }

      String WORKER_GROUP_SHARE_KEY = PREFIX + ".worker-group.share";
      boolean WORKER_GROUP_SHARE_DEFAULT = false;
      static boolean workerGroupShare(RaftProperties properties) {
        return getBoolean(properties::getBoolean, WORKER_GROUP_SHARE_KEY,
            WORKER_GROUP_SHARE_DEFAULT, getDefaultLog());
      }
      static void setWorkerGroupShare(RaftProperties properties, boolean clientWorkerGroupShare) {
        setBoolean(properties::setBoolean, WORKER_GROUP_SHARE_KEY, clientWorkerGroupShare);
      }

      String REPLY_QUEUE_GRACE_PERIOD_KEY = PREFIX + ".reply.queue.grace-period";
      TimeDuration REPLY_QUEUE_GRACE_PERIOD_DEFAULT = TimeDuration.ONE_SECOND;
      static TimeDuration replyQueueGracePeriod(RaftProperties properties) {
        return getTimeDuration(properties.getTimeDuration(REPLY_QUEUE_GRACE_PERIOD_DEFAULT.getUnit()),
            REPLY_QUEUE_GRACE_PERIOD_KEY, REPLY_QUEUE_GRACE_PERIOD_DEFAULT, getDefaultLog());
      }
      static void setReplyQueueGracePeriod(RaftProperties properties, TimeDuration timeoutDuration) {
        setTimeDuration(properties::setTimeDuration, REPLY_QUEUE_GRACE_PERIOD_KEY, timeoutDuration);
      }
    }

    interface Server {
      String PREFIX = DataStream.PREFIX + ".server";

      String TLS_CONF_PARAMETER = PREFIX + ".tls.conf";
      Class<TlsConf> TLS_CONF_CLASS = TlsConf.class;
      static TlsConf tlsConf(Parameters parameters) {
        return getTlsConf(key -> parameters.get(key, TLS_CONF_CLASS), TLS_CONF_PARAMETER, getDefaultLog());
      }
      static void setTlsConf(Parameters parameters, TlsConf conf) {
        LOG.info("setTlsConf " + conf);
        ConfUtils.setTlsConf((key, value) -> parameters.put(key, value, TLS_CONF_CLASS), TLS_CONF_PARAMETER, conf);
      }

      String USE_EPOLL_KEY = PREFIX + ".use-epoll";
      boolean USE_EPOLL_DEFAULT = true;
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

      String CHANNEL_INACTIVE_GRACE_PERIOD_KEY = PREFIX + ".channel.inactive.grace-period";
      TimeDuration CHANNEL_INACTIVE_GRACE_PERIOD_DEFAULT = TimeDuration.valueOf(10, TimeUnit.MINUTES);
      static TimeDuration channelInactiveGracePeriod(RaftProperties properties) {
        return getTimeDuration(properties.getTimeDuration(CHANNEL_INACTIVE_GRACE_PERIOD_DEFAULT.getUnit()),
            CHANNEL_INACTIVE_GRACE_PERIOD_KEY, CHANNEL_INACTIVE_GRACE_PERIOD_DEFAULT, getDefaultLog());
      }
      static void setChannelInactiveGracePeriod(RaftProperties properties, TimeDuration timeoutDuration) {
        setTimeDuration(properties::setTimeDuration, CHANNEL_INACTIVE_GRACE_PERIOD_KEY, timeoutDuration);
      }
    }
  }

  static void main(String[] args) {
    printAll(NettyConfigKeys.class);
  }
}
