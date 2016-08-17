/**
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
package org.apache.raft.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Inject code for testing. */
public class CodeInjectionForTesting {
  static final Logger LOG = LoggerFactory.getLogger(CodeInjectionForTesting.class);

  /** Code to be injected. */
  public interface Code {
    Logger LOG = CodeInjectionForTesting.LOG;

    Object execute(Object... args) throws IOException;
  }

  private static final Map<String, Code> INJECTION_POINTS
      = new ConcurrentHashMap<>();

  /** Put an injection point. */
  public static void put(String injectionPoint, Code code) {
    LOG.debug("put: {}, {}", injectionPoint, code);
    INJECTION_POINTS.put(injectionPoint, code);
  }

  /** Execute the injected code, if there is any. */
  public static Object execute(String injectionPoint, Object... args)
      throws IOException {
    final Code code = INJECTION_POINTS.get(injectionPoint);
    if (code == null) {
      return null;
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("execute: {}, {}, args={}",
          injectionPoint, code, Arrays.toString(args));
    }
    return code.execute(args);
  }
}
