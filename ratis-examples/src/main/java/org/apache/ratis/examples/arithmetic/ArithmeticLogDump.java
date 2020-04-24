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

package org.apache.ratis.examples.arithmetic;

import java.io.File;

import org.apache.ratis.proto.RaftProtos.StateMachineLogEntryProto;
import org.apache.ratis.tools.ParseRatisLog;

/**
 * Utility to dump log segments for Arithmetic State Machine.
 */
public final class ArithmeticLogDump {
  private ArithmeticLogDump() {

  }

  public static void main(String[] args) throws Exception {
    String filePath = args[0];
    System.out.println("file path is " + filePath);
    File logFile = new File(filePath);

    ParseRatisLog.Builder builder =  new ParseRatisLog.Builder();
    ParseRatisLog prl = builder.setSegmentFile(logFile)
        .setSMLogToString(ArithmeticLogDump::smToArithmeticLogString)
        .build();

    prl.dumpSegmentFile();
  }

  private static String smToArithmeticLogString(StateMachineLogEntryProto logEntryProto) {
    AssignmentMessage message = new AssignmentMessage(logEntryProto.getLogData());
    return message.toString();
  }
}
