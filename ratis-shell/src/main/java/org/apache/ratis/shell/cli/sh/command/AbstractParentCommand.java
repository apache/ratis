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

import org.apache.ratis.shell.cli.Command;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public abstract class AbstractParentCommand implements Command {
  private final Map<String, Command> subs;

  public AbstractParentCommand(Context context, List<Function<Context, Command>> subCommandConstructors) {
    this.subs = Collections.unmodifiableMap(subCommandConstructors.stream()
        .map(constructor -> constructor.apply(context))
        .collect(Collectors.toMap(Command::getCommandName, Function.identity(),
        (a, b) -> {
          throw new IllegalStateException("Found duplicated commands: " + a + " and " + b);
          }, LinkedHashMap::new)));
  }

  @Override
  public final Map<String, Command> getSubCommands() {
    return subs;
  }

  @Override
  public final String getUsage() {
    final StringBuilder usage = new StringBuilder(getCommandName());
    for (String cmd : getSubCommands().keySet()) {
      usage.append(" [").append(cmd).append("]");
    }
    return usage.toString();
  }
}
