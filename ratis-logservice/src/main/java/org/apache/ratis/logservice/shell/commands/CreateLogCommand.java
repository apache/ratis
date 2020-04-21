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
import org.apache.ratis.logservice.api.LogStream;
import org.apache.ratis.logservice.api.LogServiceClient;
import org.apache.ratis.logservice.shell.Command;
import org.jline.reader.LineReader;
import org.jline.terminal.Terminal;

public class CreateLogCommand implements Command {

  @Override public String getHelpMessage() {
    return "`create` - Creates a new log with the given name";
  }

  @Override
  public void run(Terminal terminal, LineReader lineReader, LogServiceClient client, String[] args) {
    if (args.length != 1) {
      terminal.writer().println("ERROR - Usage: create <name>");
      return;
    }
    String logName = args[0];
    try (LogStream stream = client.createLog(LogName.of(logName))) {
      terminal.writer().println("Created log '" + logName + "'");
    } catch (Exception e) {
      terminal.writer().println("Error creating log '" + logName + "'");
      e.printStackTrace(terminal.writer());
    }
  }
}
