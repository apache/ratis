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
package org.apache.ratis.server.impl;

import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ratis.server.raftlog.RaftLog;

public final class RaftServerConstants {
  /** @deprecated use {@link RaftLog#INVALID_LOG_INDEX}. */
  @Deprecated
  public static final long INVALID_LOG_INDEX = RaftLog.INVALID_LOG_INDEX;
  public static final long DEFAULT_CALLID = 0;
  public static final Pattern IPV6PATTERN = Pattern.compile(Stream.of("(",
    "([0-9a-fA-F]{1,4}:){7,7}[0-9a-fA-F]{1,4}|",          // 2001:0250:0207:0001:0000:0000:0000:ff02
    "([0-9a-fA-F]{1,4}:){1,7}:|",                         // 1::
    "([0-9a-fA-F]{1,4}:){1,6}:[0-9a-fA-F]{1,4}|",         // 2001::ff02
    "([0-9a-fA-F]{1,4}:){1,5}(:[0-9a-fA-F]{1,4}){1,2}|",  // 2001::0000:ff02
    "([0-9a-fA-F]{1,4}:){1,4}(:[0-9a-fA-F]{1,4}){1,3}|",  // 2001::0000:0000:ff02
    "([0-9a-fA-F]{1,4}:){1,3}(:[0-9a-fA-F]{1,4}){1,4}|",  // 2001::0000:0000:0000:ff02
    "([0-9a-fA-F]{1,4}:){1,2}(:[0-9a-fA-F]{1,4}){1,5}|",  // 2001::0001:0000:0000:0000:ff02
    "[0-9a-fA-F]{1,4}:((:[0-9a-fA-F]{1,4}){1,6})|",       // 2001::0207:0001:0000:0000:0000:ff02
    ":((:[0-9a-fA-F]{1,4}){1,7}|:)|",                     // ::0250:0207:0001:0000:0000:0000:ff02
    "::(ffff(:0{1,4}){0,1}:){0,1}",
    "((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\\.){3,3}",
    "(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])|",
    // ::ffff:255.255.255.255 ::ffff:0:255.255.255.255
    "([0-9a-fA-F]{1,4}:){1,4}:",
    "((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\\.){3,3}",
    "(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])",
    // 2001:db8:3:4::192.0.2.33  64:ff9b::192.0.2.33
    ")").collect(Collectors.joining()));
  private RaftServerConstants() {
    //Never constructed
  }

  public enum StartupOption {
    FORMAT("format"),
    REGULAR("regular");

    private final String option;

    StartupOption(String arg) {
      this.option = arg;
    }

    public static StartupOption getOption(String arg) {
      for (StartupOption s : StartupOption.values()) {
        if (s.option.equals(arg)) {
          return s;
        }
      }
      return REGULAR;
    }
  }
}
