/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ratis.examples.arithmetic.cli;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.examples.arithmetic.AssignmentMessage;
import org.apache.ratis.examples.arithmetic.expression.DoubleValue;
import org.apache.ratis.examples.arithmetic.expression.Expression;
import org.apache.ratis.examples.arithmetic.expression.Variable;
import org.apache.ratis.protocol.RaftClientReply;

import java.io.IOException;

/**
 * Subcommand to get value from the state machine.
 */
@Parameters(commandDescription = "Assign value to a variable.")
public class Get extends Client {

  @Parameter(names = {
      "--name"}, description = "Name of the variable to set", required = true)
  String name;

  @Override
  protected void operation(RaftClient client) throws IOException {
    RaftClientReply getValue =
        client.sendReadOnly(Expression.Utils.toMessage(new Variable(name)));
    Expression response =
        Expression.Utils.bytes2Expression(getValue.getMessage().getContent().toByteArray(), 0);
    System.out.println(String.format("%s=%s", name, (DoubleValue) response).toString());
  }
}
