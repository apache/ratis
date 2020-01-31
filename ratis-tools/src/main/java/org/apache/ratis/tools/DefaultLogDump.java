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

package org.apache.ratis.tools;

import java.io.File;

/**
 * Default log dump tool to dump log entries for any state machine.
 */
public final class DefaultLogDump {
  private DefaultLogDump() {

  }

  public static void main(String[] args) throws Exception {
    String filePath = args[0];
    System.out.println("file path is " + filePath);
    File logFile = new File(filePath);

    ParseRatisLog.Builder builder =  new ParseRatisLog.Builder();
    ParseRatisLog prl = builder.setSegmentFile(logFile)
        .build();

    prl.dumpSegmentFile();
  }
}
