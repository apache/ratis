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

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

class StringPrintStream {
  private Charset encoding = StandardCharsets.UTF_8;
  private final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
  private final PrintStream printStream;

  StringPrintStream() {
    try {
      printStream = new PrintStream(bytes, true, encoding.name());
    } catch (UnsupportedEncodingException e) {
      throw new IllegalStateException(e);
    }
  }

  public PrintStream getPrintStream() {
    return printStream;
  }

  @Override
  public String toString() {
    return bytes.toString();
  }
}
