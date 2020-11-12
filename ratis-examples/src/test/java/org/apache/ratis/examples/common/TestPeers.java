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

package org.apache.ratis.examples.common;

import static org.apache.ratis.server.impl.RaftServerConstants.IPV6PATTERN;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.Assert;
import org.junit.Test;

public class TestPeers {

  static String[] ipv6Address = new String[]{
    "2001:0250:0207:0001:0000:0000:0000:ff02",
    "2001:250:207:1:0:0:0:ff02",
    "2001::ff02",
    "2001::0000:ff02",
    "2001::0000:0000:ff02",
    "2001::0000:0000:0000:ff02",
    "2001::0001:0000:0000:0000:ff02",
    "2001::0207:0001:0000:0000:0000:ff02",
    "::0250:0207:0001:0000:0000:0000:ff02",
    "::1",
    "::255.255.255.255",
    "::ffff:255.255.255.255",
    "::ffff:0:255.255.255.255",
    "2001:db8:3:4::192.0.2.33",
    "64:ff9b::192.0.2.33",
  };

  static String[] expectedPeers = new String[]{
    "nc:[0:250:207:1:0:0:0:ff02]:6000",
    "nc:[0:0:0:0:ffff:0:ffff:ffff]:6000",
    "nc:[2001:0:0:0:0:0:0:ff02]:6000",
    "nc:[2001:0:0:1:0:0:0:ff02]:6000",
    "nc:[2001:250:207:1:0:0:0:ff02]:6000",
    "nc:[2001:0:207:1:0:0:0:ff02]:6000",
    "nc:[2001:db8:3:4:0:0:c000:221]:6000",
    "nc:[0:0:0:0:0:0:ffff:ffff]:6000",
    "nc:[64:ff9b:0:0:0:0:c000:221]:6000",
    "nc:[fe80:0:0:0:ccc:d6:1d51:db51]:6000"
  };

  @Test
  public void testPatternIpv6() {
    Assert.assertEquals(
      Arrays.stream(ipv6Address).filter(s -> IPV6PATTERN.matcher(s).find()).count(),
      ipv6Address.length);
  }

  @Test
  public void testParsePeers() {
    List<String> actualPeers = Arrays
      .stream(SubCommandBase.parsePeers(Stream.of(expectedPeers).collect(Collectors.joining(","))))
      .map(s -> s.getId().toString() + ":" + s.getAddress()).collect(Collectors.toList());
    Assert.assertArrayEquals(expectedPeers, actualPeers.toArray());
  }
}
