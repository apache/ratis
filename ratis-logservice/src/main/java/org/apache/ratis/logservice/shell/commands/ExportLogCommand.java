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

import org.apache.ratis.logservice.api.LogName;
import org.apache.ratis.logservice.api.LogServiceClient;
import org.apache.ratis.logservice.shell.Command;
import org.jline.reader.LineReader;
import org.jline.terminal.Terminal;

public class ExportLogCommand implements Command {

  @Override public String getHelpMessage() {
    return "`export` - export the given log at given location starting from provided recordId";
  }

  @Override
  public void run(Terminal terminal, LineReader lineReader, LogServiceClient client, String[] args) {
    if (args.length != 3) {
      terminal.writer().println("ERROR - Usage: export <name> <location> <recordId>");
      return;
    }
    String logName = args[0];
    String location = args[1];
    long recordId = Long.parseLong(args[2]);
    try {
      client.exportLog(LogName.of(logName), location,recordId);
      terminal.writer().println("Export Log request is submitted successfully!!");
    } catch (Exception e) {
      terminal.writer().println("Failed to export log!!");
      e.printStackTrace(terminal.writer());
    }
  }
}
