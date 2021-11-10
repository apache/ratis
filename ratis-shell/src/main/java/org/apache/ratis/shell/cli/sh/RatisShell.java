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
package org.apache.ratis.shell.cli.sh;

import org.apache.ratis.shell.cli.AbstractShell;
import org.apache.ratis.shell.cli.Command;
import org.apache.ratis.shell.cli.sh.command.Context;
import org.apache.ratis.util.ReflectionUtils;
import org.reflections.Reflections;

import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;

/**
 * Shell for manage ratis group.
 */
public class RatisShell extends AbstractShell {

  /**
   * Manage ratis shell command.
   *
   * @param args array of arguments given by the user's input from the terminal
   */
  public static void main(String[] args) {
    RatisShell extensionShell = new RatisShell();
    System.exit(extensionShell.run(args));
  }

  @Override
  protected String getShellName() {
    return "sh";
  }

  @Override
  protected Map<String, Command> loadCommands() {
    Context adminContext = new Context(System.out);
    return loadCommands(RatisShell.class.getPackage().getName(),
        new Class[] {Context.class},
        new Object[] {getCloser().register(adminContext)});
  }

  /**
   * Get instances of all subclasses of {@link Command} in a sub-package called "command" the given
   * package.
   *
   * @param pkgName package prefix to look in
   * @param classArgs type of args to instantiate the class
   * @param objectArgs args to instantiate the class
   * @return a mapping from command name to command instance
   */
  public static Map<String, Command> loadCommands(String pkgName, Class[] classArgs,
      Object[] objectArgs) {
    Map<String, Command> commandsMap = new HashMap<>();
    Reflections reflections = new Reflections(pkgName);
    for (Class<? extends Command> cls : reflections.getSubTypesOf(Command.class)) {
      // Add commands from <pkgName>.command.*
      if (cls.getPackage().getName().equals(pkgName + ".command")
          && !Modifier.isAbstract(cls.getModifiers())) {
        // Only instantiate a concrete class
        final Command cmd = ReflectionUtils.newInstance(cls, classArgs, objectArgs);
        commandsMap.put(cmd.getCommandName(), cmd);
      }
    }
    return commandsMap;
  }
}
