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
package org.apache.ratis.logservice.api;

import java.net.URL;
import java.util.UUID;

import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.logservice.common.Constants;
import org.apache.ratis.logservice.server.MetadataServer;
import org.apache.ratis.logservice.server.ServerOpts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * An encapsulation of configuration for a LogService.
 * The base configuration is defined in logservice.xml file,
 * which is expected to reside in a class path.
 */
public final class LogServiceConfiguration extends RaftProperties {
  private static final Logger LOG = LoggerFactory.getLogger(MetadataServer.class);
  public static final String RESOURCE_NAME = "logservice.xml";


  /**
   * Creates default log service configuration object
   * @return configuration object
   */
  public static LogServiceConfiguration create() {
    return new LogServiceConfiguration();
  }

  /**
   * Creates log service configuration object
   * with a custom configuration file (found on classpath)
   * @param resourceName name of configuration file
   * @return configuration object
   */
  public static LogServiceConfiguration create(String resourceName) {
    return new LogServiceConfiguration(resourceName);
  }

  /**
   * Creates log service configuration object
   * with a custom configuration URL
   * @param resourcePath path of configuration file
   * @return configuration object
   */
  public static LogServiceConfiguration create(URL resourcePath) {
    return new LogServiceConfiguration(resourcePath);
  }

  private LogServiceConfiguration() {
    super();
    addResource(RESOURCE_NAME);
  }

  private LogServiceConfiguration(String resourceName) {
    super();
    addResource(resourceName);
  }

  private LogServiceConfiguration(URL resourcePath) {
    super();
    addResource(resourcePath);
  }

  /**
   * Adds configuration options from logservice.xml
   * for LogServer
   * @param opts server options
   * @return server options
   */
  public ServerOpts addLogServerOpts(ServerOpts opts) {
    if (!opts.isHostSet()) {
      String val = get(Constants.LOG_SERVER_HOSTNAME_KEY, "localhost");
      if (val != null) {
        opts.setHost(val);
      }
    }

    if (!opts.isPortSet()) {
      String val = get(Constants.LOG_SERVER_PORT_KEY);
      if (val != null) {
        try {
          opts.setPort(Integer.parseInt(val));
        } catch (Exception e) {
          LOG.warn("Config value {} for {} is invaild", val, Constants.LOG_SERVER_PORT_KEY);
        }
      }
    }

    if (!opts.isWorkingDirSet()) {
      String val = get(Constants.LOG_SERVER_WORKDIR_KEY);
      if (val != null) {
        opts.setWorkingDir(val);
      }
    }

    if (!opts.isMetaQuorumSet()) {
      String val = get(Constants.LOG_SERVICE_METAQUORUM_KEY);
      if (val != null) {
        opts.setMetaQuorum(val);
      }
    }

    if (!opts.isLogServerGroupIdSet()) {
      String val = get(Constants.LOG_SERVICE_LOG_SERVER_GROUPID_KEY);
      if (val != null) {
        try {
          opts.setLogServerGroupId(UUID.fromString(val));
        } catch (IllegalArgumentException e) {
          LOG.warn("Config value {} for {} is invaild", val,
            Constants.LOG_SERVICE_LOG_SERVER_GROUPID_KEY);
        }
      } else {
        opts.setLogServerGroupId(Constants.SERVERS_GROUP_UUID);
      }
    }
    return opts;
  }


  /**
   * Adds configuration options from logservice.xml
   * for MetadataServer
   * @param opts server options
   * @return server options
   */
  public ServerOpts addMetaServerOpts (ServerOpts opts) {
    if (!opts.isHostSet()) {
      String val = get(Constants.META_SERVER_HOSTNAME_KEY);
      if (val != null) {
        opts.setHost(val);
      }
    }

    if (!opts.isPortSet()) {
      String val = get(Constants.META_SERVER_PORT_KEY);
      if (val != null) {
        try {
          opts.setPort(Integer.parseInt(val));
        } catch (Exception e) {
          LOG.warn("Config value {} for {} is invaild", val,
            Constants.META_SERVER_PORT_KEY);
        }
      }
    }

    if (!opts.isWorkingDirSet()) {
      String val = get(Constants.META_SERVER_WORKDIR_KEY);
      if (val != null) {
        opts.setWorkingDir(val);
      }
    }

    if (!opts.isMetaQuorumSet()) {
      String val = get(Constants.LOG_SERVICE_METAQUORUM_KEY);
      if (val != null) {
        opts.setMetaQuorum(val);
      }
    }

    if (!opts.isMetaServerGroupIdSet()) {
      String val = get(Constants.LOG_SERVICE_META_SERVER_GROUPID_KEY);
      if (val != null) {
        try {
          opts.setMetaGroupId(UUID.fromString(val));
        } catch (IllegalArgumentException e) {
          LOG.warn("Config value {} for {} is invaild", val,
            Constants.LOG_SERVICE_META_SERVER_GROUPID_KEY);
        }
      } else {
        opts.setMetaGroupId(Constants.META_GROUP_UUID);
      }
    }
    return opts;
  }

}
