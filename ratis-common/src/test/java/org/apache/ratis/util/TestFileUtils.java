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
package org.apache.ratis.util;

import org.apache.ratis.BaseTest;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

/** Test methods of {@link FileUtils}. */
public class TestFileUtils extends BaseTest {

  @Test
  public void testRenameToCorrupt() throws IOException {
    final File dir = getClassTestDir();
    Assert.assertTrue(dir.mkdirs());
    try {
      runTestRenameToCorrupt(dir);
    } finally {
      FileUtils.deleteFully(dir);
    }
    Assert.assertFalse(dir.exists());
  }

  static void runTestRenameToCorrupt(File dir) throws IOException {
    final File srcFile = new File(dir, "snapshot.1_20");
    Assert.assertFalse(srcFile.exists());
    Assert.assertTrue(srcFile.createNewFile());
    Assert.assertTrue(srcFile.exists());

    final File renamed = FileUtils.move(srcFile, ".corrupt");
    Assert.assertNotNull(renamed);
    Assert.assertTrue(renamed.exists());
    Assert.assertFalse(srcFile.exists());

    FileUtils.deleteFully(renamed);
    Assert.assertFalse(renamed.exists());
  }
}
