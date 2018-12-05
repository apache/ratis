package org.apache.ratis.logservice.shell;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.apache.ratis.logservice.shell.commands.CreateLogCommand;
import org.apache.ratis.logservice.shell.commands.DeleteLogCommand;
import org.apache.ratis.logservice.shell.commands.ExitCommand;
import org.apache.ratis.logservice.shell.commands.HelpCommand;
import org.apache.ratis.logservice.shell.commands.ListLogsCommand;
import org.apache.ratis.logservice.shell.commands.PutToLogCommand;
import org.apache.ratis.logservice.shell.commands.ReadLogCommand;

public class CommandFactory {
  private static final Map<String,Command> KNOWN_COMMANDS = cacheCommands();

  private static Map<String,Command> cacheCommands() {
    Map<String,Command> commands = new HashMap<>();
    ExitCommand exitCommand = new ExitCommand();

    // Ensure all keys are lowercase -- see the same logic in #create(String).
    commands.put("create", new CreateLogCommand());
    commands.put("put", new PutToLogCommand());
    commands.put("read", new ReadLogCommand());
    commands.put("delete", new DeleteLogCommand());
    commands.put("exit", exitCommand);
    commands.put("quit", exitCommand);
    commands.put("help", new HelpCommand());
    commands.put("list", new ListLogsCommand());

    return Collections.unmodifiableMap(commands);
  }

  public static Map<String,Command> getCommands() {
    return KNOWN_COMMANDS;
  }

  private CommandFactory() {}

  /**
   * Returns an instance of the command to run given the name, else null.
   * Be aware that Command instances are singletons and do not retain state.
   */
  public static Command create(String commandName) {
    // Normalize the command name to lowercase
    return KNOWN_COMMANDS.get(Objects.requireNonNull(commandName).toLowerCase());
  }
}