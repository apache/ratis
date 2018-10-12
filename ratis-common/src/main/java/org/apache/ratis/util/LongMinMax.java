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
package org.apache.ratis.util;

/**
 * Min and max values in long.
 *
 * This class is mutable.
 * This class is NOT thread safe.
 */
public class LongMinMax {
  private long min;
  private long max;
  private boolean initialized = false;

  /** @return the min */
  public long getMin() {
    Preconditions.assertTrue(initialized, "This LongMinMax object is uninitialized.");
    return min;
  }

  /** @return the max */
  public long getMax() {
    Preconditions.assertTrue(initialized, "This LongMinMax object is uninitialized.");
    return max;
  }

  public boolean isInitialized() {
    return initialized;
  }

  /** Update min and max with the given number. */
  public void accumulate(long n) {
    if (!initialized) {
      min = max = n;
      initialized = true;
    } else if (n < min) {
      min = n;
    } else if (n > max) {
      max = n;
    }
  }

  /** Combine that to this. */
  public void combine(LongMinMax that) {
    if (that.initialized) {
      if (!this.initialized) {
        this.min = that.min;
        this.max = that.max;
        this.initialized = true;
      } else {
        if (that.min < this.min) {
          this.min = that.min;
        }
        if (that.max > this.max) {
          this.max = that.max;
        }
      }
    }
  }
}
