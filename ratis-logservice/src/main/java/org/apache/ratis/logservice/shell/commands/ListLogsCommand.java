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

import java.io.IOException;
import java.util.List;

import org.apache.ratis.logservice.api.LogInfo;
import org.apache.ratis.logservice.api.LogServiceClient;
import org.apache.ratis.logservice.shell.Command;
import org.jline.reader.LineReader;
import org.jline.terminal.Terminal;

public class ListLogsCommand implements Command {

  @Override
  public String getHelpMessage() {
    return "`list` - List the available logs";
  }

  @Override
  public void run(Terminal terminal, LineReader lineReader, LogServiceClient client, String[] args) {
    if (args.length != 0) {
      terminal.writer().println("ERROR - Usage: list");
      return;
    }

    try {
      List<LogInfo> logs = client.listLogs();
      StringBuilder sb = new StringBuilder();
      for (LogInfo log : logs) {
        if (sb.length() > 0) {
          sb.append("\n");
        }
        sb.append(log.getLogName().getName());
      }
      terminal.writer().println(sb.toString());
    } catch (IOException e) {
      terminal.writer().println("Failed to list available logs");
      e.printStackTrace(terminal.writer());
    }
  }

}
