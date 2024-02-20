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

import org.apache.ratis.BaseTest;
import org.apache.ratis.shell.cli.Command;
import org.apache.ratis.shell.cli.sh.command.AbstractParentCommand;
import org.apache.ratis.shell.cli.sh.command.Context;
import org.apache.ratis.shell.cli.sh.command.ElectionCommand;
import org.apache.ratis.shell.cli.sh.command.GroupCommand;
import org.apache.ratis.shell.cli.sh.command.LocalCommand;
import org.apache.ratis.shell.cli.sh.command.PeerCommand;
import org.apache.ratis.shell.cli.sh.command.SnapshotCommand;
import org.apache.ratis.util.ReflectionUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.reflections.Reflections;

import java.io.PrintStream;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Test {@link RatisShell}
 */
public class TestRatisShell extends BaseTest {
  static final PrintStream OUT = System.out;
  static final Class<?>[] ARG_CLASSES = new Class[] {Context.class};

  static void assertCommand(String message, Command expected, Command computed) {
    Assertions.assertEquals(expected.getClass(), computed.getClass(), message);
    Assertions.assertEquals(expected.getCommandName(), computed.getCommandName(), message);
  }

  static void assertCommands(List<Command> expected, List<Command> computed) {
    Assertions.assertEquals(expected.size(), computed.size());
    for(int i = 0; i < expected.size(); i++) {
      assertCommand("Command " + i, expected.get(i), computed.get(i));
    }
  }

  @Test
  public void testFullParentCommandList() throws Exception {
    final List<Command> expected = new ArrayList<>(loadCommands(RatisShell.class.getPackage().getName() + ".command"));
    Collections.sort(expected);

    try(RatisShell shell = new RatisShell(OUT)) {
      final List<Command> computed = new ArrayList<>(shell.getCommands());
      Collections.sort(computed);

      assertCommands(expected, computed);
    }
  }

  @Test
  public void testFullPeerCommandList() {
    runTestFullCommandList(PeerCommand::new);
  }

  @Test
  public void testFullGroupCommandList() {
    runTestFullCommandList(GroupCommand::new);
  }

  @Test
  public void testFullElectionCommandList() {
    runTestFullCommandList(ElectionCommand::new);
  }

  @Test
  public void testFullSnapshotCommandList() {
    runTestFullCommandList(SnapshotCommand::new);
  }

  @Test
  public void testFullLocalCommandList() {
    runTestFullCommandList(LocalCommand::new);
  }

  static void runTestFullCommandList(Function<Context, AbstractParentCommand> parentCommandConstructor) {
    final AbstractParentCommand parent = parentCommandConstructor.apply(new Context(OUT));
    final List<Command> computed = new ArrayList<>(parent.getSubCommands().values());
    Collections.sort(computed);

    Assertions.assertFalse(computed.isEmpty());

    final Package pkg = computed.iterator().next().getClass().getPackage();
    final List<Command> expected = new ArrayList<>(loadCommands(pkg));
    Collections.sort(expected);

    assertCommands(expected, computed);
  }

  static Collection<Command> loadCommands(Package pkg) {
    return loadCommands(pkg.getName());
  }

  static Collection<Command> loadCommands(String pkgName) {
    return oldLoadCommands(pkgName, ARG_CLASSES, new Object[]{new Context(OUT)}).values();
  }

  /**
   * Get instances of all subclasses of {@link Command} in the given package.
   *
   * @param pkgName package prefix to look in
   * @param classArgs type of args to instantiate the class
   * @param objectArgs args to instantiate the class
   * @return a mapping from command name to command instance
   */
  static Map<String, Command> oldLoadCommands(String pkgName, Class<?>[] classArgs, Object[] objectArgs) {
    Map<String, Command> commandsMap = new HashMap<>();
    Reflections reflections = new Reflections(pkgName);
    for (Class<? extends Command> cls : reflections.getSubTypesOf(Command.class)) {
      // Add commands from <pkgName>.*
      if (cls.getPackage().getName().equals(pkgName)
          && !Modifier.isAbstract(cls.getModifiers())) {
        // Only instantiate a concrete class
        final Command cmd = ReflectionUtils.newInstance(cls, classArgs, objectArgs);
        commandsMap.put(cmd.getCommandName(), cmd);
      }
    }
    return commandsMap;
  }
}
