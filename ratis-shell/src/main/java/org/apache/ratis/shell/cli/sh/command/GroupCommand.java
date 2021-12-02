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
package org.apache.ratis.shell.cli.sh.command;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.ratis.shell.cli.Command;
import org.apache.ratis.shell.cli.sh.group.GroupInfoCommand;
import org.apache.ratis.shell.cli.sh.group.GroupListCommand;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * Command for the ratis group
 */
public class GroupCommand extends AbstractRatisCommand {

  private static final Map<String, Function<Context, ? extends Command>>
          SUB_COMMANDS = new HashMap<>();

  static {
    SUB_COMMANDS.put("info", GroupInfoCommand::new);
    SUB_COMMANDS.put("list", GroupListCommand::new);
  }

  private Map<String, Command> mSubCommands = new HashMap<>();

  /**
   * @param context command context
   */
  public GroupCommand(Context context) {
    super(context);
    SUB_COMMANDS.forEach((name, constructor) -> {
      mSubCommands.put(name, constructor.apply(context));
    });
  }

  @Override
  public String getCommandName() {
    return "group";
  }

  @Override
  public String getUsage() {

    StringBuilder usage = new StringBuilder(getCommandName());
    for (String cmd : SUB_COMMANDS.keySet()) {
      usage.append(" [").append(cmd).append("]");
    }
    return usage.toString();
  }

  @Override
  public String getDescription() {
    return description();
  }

  @Override
  public boolean hasSubCommand() {
    return true;
  }

  @Override
  public Map<String, Command> getSubCommands() {
    return mSubCommands;
  }

  @Override
  public Options getOptions() {
    return super.getOptions().addOption(
        Option.builder()
            .option(GROUPID_OPTION_NAME)
            .hasArg()
            .required()
            .desc("the group id")
            .build());
  }

  /**
   * @return command's description
   */
  public static String description() {
    return "Manage the ratis gropu, See sub-commands' descriptions for more details.";
  }
}
