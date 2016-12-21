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

public class NativeLibraryChecker {
  public static final Logger LOG = LoggerFactory.getLogger(NativeLibraryChecker.class);

  /**
   * A tool to test native library availability, 
   */
  public static void main(String[] args) {
    String usage = "NativeLibraryChecker [-a|-h]\n"
        + "  -a  use -a to check all libraries are available\n"
        + "      by default just check raft library (and\n"
        + "      winutils.exe on Windows OS) is available\n"
        + "      exit with error code 1 if check failed\n"
        + "  -h  print this message\n";
    if (args.length > 1 ||
        (args.length == 1 &&
            !(args[0].equals("-a") || args[0].equals("-h")))) {
      System.err.println(usage);
      ExitUtils.terminate(1, "Illegal arguments.");
    }
    if (args.length == 1) {
      if (args[0].equals("-h")) {
        System.out.println(usage);
        return;
      }
    }
    boolean nativeRaftLoaded = NativeCodeLoader.isNativeCodeLoaded();
    String raftLibraryName = "";

    if (nativeRaftLoaded) {
      raftLibraryName = NativeCodeLoader.getLibraryName();
    }

    System.out.println("Native library checking:");
    System.out.printf("raft:  %b %s%n", nativeRaftLoaded, raftLibraryName);

    if (!nativeRaftLoaded) {
      // return 1 to indicated check failed
      ExitUtils.terminate(1, "Failed to load native library.");
    }
  }
}
