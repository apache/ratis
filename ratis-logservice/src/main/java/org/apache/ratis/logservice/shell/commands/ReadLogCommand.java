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
package org.apache.ratis.logservice.shell.commands;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.ratis.logservice.api.LogName;
import org.apache.ratis.logservice.api.LogReader;
import org.apache.ratis.logservice.api.LogStream;
import org.apache.ratis.logservice.api.LogServiceClient;
import org.apache.ratis.logservice.shell.Command;
import org.jline.reader.LineReader;
import org.jline.terminal.Terminal;

public class ReadLogCommand implements Command {

  @Override public String getHelpMessage() {
    return "`read` - Reads all values from the given log";
  }

  @Override
  @SuppressFBWarnings("REC_CATCH_EXCEPTION")
  public void run(Terminal terminal, LineReader lineReader, LogServiceClient client, String[] args) {
    if (args.length != 1) {
      terminal.writer().println("ERROR - Usage: read <name>");
      return;
    }
    String logName = args[0];
    try (LogStream stream = client.getLog(LogName.of(logName));
        LogReader reader = stream.createReader()) {
      long firstId = stream.getStartRecordId();
      long lastId = stream.getLastRecordId();
      StringBuilder sb = new StringBuilder();
      List<ByteBuffer> records = reader.readBulk((int) (lastId - firstId));
      for (ByteBuffer record : records) {
        if (sb.length() > 0) {
          sb.append(", ");
        }
        sb.append("\"");
        if (record != null) {
          String strData = new String(record.array(), record.arrayOffset(), record.remaining(), StandardCharsets.UTF_8);
          sb.append(strData);
        }
        sb.append("\"");
      }
      sb.insert(0, "[");
      sb.append("]");
      terminal.writer().println(sb.toString());
    } catch (Exception e) {
      terminal.writer().println("Failed to read from log");
      e.printStackTrace(terminal.writer());
    }
  }
}
