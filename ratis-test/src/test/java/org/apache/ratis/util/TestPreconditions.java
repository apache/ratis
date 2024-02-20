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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TestPreconditions extends BaseTest {
  @Test
  @Timeout(value = 1000)
  public void testAssertUnique() {
    final Set<Integer> empty = Collections.emptySet();
    Preconditions.assertUnique(empty);
    Preconditions.assertUnique(empty, empty);

    final Set<Integer> one = Collections.singleton(1);
    Preconditions.assertUnique(one);
    Preconditions.assertUnique(empty, one);
    testFailureCase("add [1] to [1]", () -> Preconditions.assertUnique(one, one), IllegalStateException.class);

    final List<Integer> three = Arrays.asList(1, 2, 3);
    testFailureCase("add [1, 2, 3] to [1]", () -> Preconditions.assertUnique(three, one), IllegalStateException.class);
    testFailureCase("add [1] to [1, 2, 3]", () -> Preconditions.assertUnique(one, three), IllegalStateException.class);

    final List<Integer> duplicated = Arrays.asList(3, 2, 3);
    testFailureCase("check [3, 2, 3]", () -> Preconditions.assertUnique(duplicated), IllegalStateException.class);
    testFailureCase("add [1] to [3, 2, 3]", () -> Preconditions.assertUnique(duplicated, one),
        IllegalStateException.class);
    testFailureCase("add [3, 2, 3] to [1]", () -> Preconditions.assertUnique(one, duplicated),
        IllegalStateException.class);

    Preconditions.assertUnique(three, Arrays.asList(4, 5, 6));
  }

  @Test
  @Timeout(value = 1000)
  public void testAssertNull() {
    final Map<String, String> map = new HashMap<>();
    final String key = "abc1234";
    // putNew will call Preconditions.assertNull(..) to assert the entry does not exist in the map
    // putNew the first time should work
    CollectionUtils.putNew(key, key, map, () -> "m");
    Preconditions.assertTrue(map.containsKey(key));

    // putNew the second time should fail
    final Throwable e = testFailureCase("put " + key + " again",
        () -> CollectionUtils.putNew(key, key, map, () -> "m"),
        IllegalStateException.class);
    // The message should contain the key name
    Preconditions.assertTrue(e.getMessage().contains(key));
  }
}
