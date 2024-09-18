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

import java.io.PrintStream;

/**
 * The base class for all the ratis shell {@link Command} classes.
 */
public abstract class AbstractCommand implements Command {

  private final Context context;

  protected AbstractCommand(Context context) {
    this.context = context;
  }

  protected Context getContext() {
    return context;
  }

  protected PrintStream getPrintStream() {
    return getContext().getPrintStream();
  }

  protected void printf(String format, Object... args) {
    getPrintStream().printf(format, args);
  }

  protected void println(Object message) {
    getPrintStream().println(message);
  }
}
