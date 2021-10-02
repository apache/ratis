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
package org.apache.ratis.ratisshell.util.io;

import com.google.common.base.CharMatcher;
import com.google.common.base.Preconditions;

import java.io.File;

/**
 * Utilities related to local file paths.
 */
public final class PathUtils {
  private static final CharMatcher SEPARATOR_MATCHER =
      CharMatcher.is(File.separator.charAt(0));

  /**
   * Joins two path elements, separated by {@link File#separator}.
   * <p>
   * Note that empty element in base or paths is ignored.
   *
   * @param base base path
   * @param path path element to concatenate
   * @return joined path
   */
  public static String concatPath(Object base, Object path) {
    Preconditions.checkNotNull(base, "base");
    Preconditions.checkNotNull(path, "path");
    String trimmedBase = SEPARATOR_MATCHER.trimTrailingFrom(base.toString());
    String trimmedPath = SEPARATOR_MATCHER.trimFrom(path.toString());

    StringBuilder output = new StringBuilder(trimmedBase.length() + trimmedPath.length() + 1);
    output.append(trimmedBase);
    if (!trimmedPath.isEmpty()) {
      output.append(File.separator);
      output.append(trimmedPath);
    }

    if (output.length() == 0) {
      // base must be "[/]+"
      return File.separator;
    }
    return output.toString();
  }

  private PathUtils() {} // prevent instantiation
}
