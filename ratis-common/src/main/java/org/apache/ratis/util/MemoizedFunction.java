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

import java.util.Objects;
import java.util.function.Function;

/**
 * A memoized function is a {@link Function}
 * which returns a value by invoking its initializer once
 * and then keeps returning the same value as its result.
 * <p>
 * This class is similar to {@link MemoizedSupplier} except that
 * the initializer takes a parameter.
 * <p>
 * This class is thread safe.
 *
 * @param <RETURN> The function result type.
 */
public final class MemoizedFunction<PARAMETER, RETURN>
    extends MemoizedBase<RETURN, RuntimeException>
    implements Function<PARAMETER, RETURN> {
  /**
   * @param function to supply at most one non-null value.
   * @return a {@link MemoizedFunction} with the given function.
   */
  public static <P, R> MemoizedFunction<P, R> valueOf(Function<P, R> function) {
    return function instanceof MemoizedFunction ?
        (MemoizedFunction<P, R>) function : new MemoizedFunction<>(function);
  }

  private final Function<PARAMETER, RETURN> initializer;

  /**
   * Create a memoized function.
   * @param initializer to supply at most one non-null value.
   */
  private MemoizedFunction(Function<PARAMETER, RETURN> initializer) {
    Objects.requireNonNull(initializer, "initializer == null");
    this.initializer = initializer;
  }

  /**
   * @param parameter for passing to the initializer.
   *                  Since the returned function is memoized, the parameter is only used at the first call.
   *                  The parameter in the subsequent calls is ignored.
   *
   * @return the lazily initialized object.
   */
  @Override
  public RETURN apply(PARAMETER parameter) {
    return init(() -> initializer.apply(parameter));
  }
}
