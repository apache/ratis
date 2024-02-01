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
package org.apache.ratis.conf;


import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

public class TestRaftProperties {
  enum Type {APPEND_ENTRIES}

  static class Request_Vote {
  }

  static final String KEY = "key";

  static void setUnderscoreValue(RaftProperties p, String valueWithUnderscore) {
    Assertions.assertTrue(valueWithUnderscore.contains("_"));
    p.set(KEY, valueWithUnderscore);
  }

  static void setNonUnderscoreValue(RaftProperties p, String valueWithoutUnderscore) {
    Assertions.assertFalse(valueWithoutUnderscore.contains("_"));
    p.set(KEY, valueWithoutUnderscore);
  }

  @Test
  @Timeout(value = 1000)
  public void testUnderscore() {
    final RaftProperties p = new RaftProperties();

    { // boolean
      Assertions.assertNull(p.getBoolean(KEY, null));
      setNonUnderscoreValue(p, "true");
      Assertions.assertTrue(p.getBoolean(KEY, null));
      setNonUnderscoreValue(p, "false");
      Assertions.assertFalse(p.getBoolean(KEY, null));
      setUnderscoreValue(p, "fa_lse");
      Assertions.assertNull(p.getBoolean(KEY, null));
      p.unset(KEY);
    }

    { //int
      final Integer expected = 1000000;
      Assertions.assertNull(p.getInt(KEY, null));
      setUnderscoreValue(p, "1_000_000");
      Assertions.assertEquals(expected, p.getInt(KEY, null));
      setNonUnderscoreValue(p, "1000000");
      Assertions.assertEquals(expected, p.getInt(KEY, null));
      p.unset(KEY);
    }

    { // long
      final Long expected = 1_000_000_000_000L;
      Assertions.assertNull(p.getLong(KEY, null));
      setUnderscoreValue(p, "1_000_000_000_000");
      Assertions.assertEquals(expected, p.getLong(KEY, null));
      setNonUnderscoreValue(p, "1000000000000");
      Assertions.assertEquals(expected, p.getLong(KEY, null));
      p.unset(KEY);
    }

    { // File
      final String expected = "1_000_000";
      Assertions.assertNull(p.getFile(KEY, null));
      setUnderscoreValue(p, expected);
      Assertions.assertEquals(expected, p.getFile(KEY, null).getName());
      p.unset(KEY);
    }

    { // class
      final Type expected = Type.APPEND_ENTRIES;
      Assertions.assertNull(p.getEnum(KEY, Type.class, null));
      setUnderscoreValue(p, expected.name());
      Assertions.assertEquals(expected, p.getEnum(KEY, Type.class, null));
      p.unset(KEY);
    }

    { // enum
      final Class<Request_Vote> expected = Request_Vote.class;
      Assertions.assertNull(p.getClass(KEY, null));
      setUnderscoreValue(p, expected.getName());
      Assertions.assertEquals(expected, p.getClass(KEY, null));
      p.unset(KEY);
    }
  }
}
