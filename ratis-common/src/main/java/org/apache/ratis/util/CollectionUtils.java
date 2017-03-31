/*
 * *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.ratis.util;

import java.util.Iterator;
import java.util.Objects;
import java.util.function.Function;

public interface CollectionUtils {
  /**
   *  @return the next element in the iteration right after the given element;
   *          if the given element is not in the iteration, return the first one
   */
  static <T> T next(final T given, final Iterable<T> iteration) {
    Objects.requireNonNull(given, "given == null");
    final Iterator<T> i = Objects.requireNonNull(iteration, "iteration == null").iterator();
    Preconditions.assertTrue(i.hasNext(), "iteration is empty.");

    final T first = i.next();
    for(T current = first; i.hasNext(); ) {
      final T next = i.next();
      if (given.equals(current)) {
        return next;
      }
      current = next;
    }
    return first;
  }

  static <INPUT, OUTPUT> Iterable<OUTPUT> as(
      Iterable<INPUT> iteration, Function<INPUT, OUTPUT> converter) {
    return () -> new Iterator<OUTPUT>() {
      final Iterator<INPUT> i = iteration.iterator();
      @Override
      public boolean hasNext() {
        return i.hasNext();
      }

      @Override
      public OUTPUT next() {
        return converter.apply(i.next());
      }
    };
  }
}
