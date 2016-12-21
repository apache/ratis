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
package org.apache.raft.util;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public abstract class NetUtils {
  public static final Logger LOG = LoggerFactory.getLogger(NetUtils.class);

  public static abstract class StaticResolution {
    /** Host -> resolved name */
    private static final Map<String, String> hostToResolved = new ConcurrentHashMap<>();

    /** Adds a static resolution for host. */
    public static void put(String host, String resolvedName) {
      hostToResolved.put(host, resolvedName);
    }

    /** @return the resolved name, or null if the host is not found. */
    public static String get(String host) {
      return hostToResolved.get(host);
    }
  }

  public static InetSocketAddress newInetSocketAddress(String address) {
    if (address.charAt(0) == '/') {
      address = address.substring(1);
    }
    try {
      return createSocketAddr(address);
    } catch (Exception e) {
      LOG.trace("", e);
      return null;
    }
  }

  /**
   * Util method to build socket addr from either:
   *   <host>:<port>
   *   <fs>://<host>:<port>/<path>
   */
  public static InetSocketAddress createSocketAddr(String target) {
    return createSocketAddr(target, -1);
  }

  /**
   * Util method to build socket addr from either:
   *   <host>
   *   <host>:<port>
   *   <fs>://<host>:<port>/<path>
   */
  public static InetSocketAddress createSocketAddr(String target, int defaultPort) {
    return createSocketAddr(target, defaultPort, null);
  }

  /**
   * Create an InetSocketAddress from the given target string and
   * default port. If the string cannot be parsed correctly, the
   * <code>configName</code> parameter is used as part of the
   * exception message, allowing the user to better diagnose
   * the misconfiguration.
   *
   * @param target a string of either "host" or "host:port"
   * @param defaultPort the default port if <code>target</code> does not
   *                    include a port number
   * @param propertyName the name of the configuration from which
   *                   <code>target</code> was loaded. This is used in the
   *                   exception message in the case that parsing fails.
   */
  public static InetSocketAddress createSocketAddr(
      String target, int defaultPort, String propertyName) {
    final String helpText = propertyName == null? ""
        : " (property '" + propertyName + "')";
    Preconditions.checkNotNull(target, "Target address cannot be null.%s", helpText);

    target = target.trim();
    boolean hasScheme = target.contains("://");
    final URI uri;
    try {
      uri = hasScheme ? URI.create(target) : URI.create("dummyscheme://"+target);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Invalid host:port authority: " + target + helpText, e);
    }

    final String host = uri.getHost();
    int port = uri.getPort();
    if (port == -1) {
      port = defaultPort;
    }
    final String path = uri.getPath();

    if (host == null || port < 0
        || (!hasScheme && path != null && !path.isEmpty())) {
      throw new IllegalArgumentException(
          "Invalid host:port authority: " + target + helpText);
    }
    return createSocketAddrForHost(host, port);
  }

  /**
   * Create a socket address with the given host and port.  The hostname
   * might be replaced with another host that was set via
   * {@link StaticResolution#put(String, String)}.  The value of
   * hadoop.security.token.service.use_ip will determine whether the
   * standard java host resolver is used, or if the fully qualified resolver
   * is used.
   * @param host the hostname or IP use to instantiate the object
   * @param port the port number
   * @return InetSocketAddress
   */
  public static InetSocketAddress createSocketAddrForHost(String host, int port) {
    String staticHost = StaticResolution.get(host);
    String resolveHost = (staticHost != null) ? staticHost : host;

    InetSocketAddress addr;
    try {
      InetAddress iaddr = InetAddress.getByName(resolveHost);
      // if there is a static entry for the host, make the returned
      // address look like the original given host
      if (staticHost != null) {
        iaddr = InetAddress.getByAddress(host, iaddr.getAddress());
      }
      addr = new InetSocketAddress(iaddr, port);
    } catch (UnknownHostException e) {
      addr = InetSocketAddress.createUnresolved(host, port);
    }
    return addr;
  }
}
