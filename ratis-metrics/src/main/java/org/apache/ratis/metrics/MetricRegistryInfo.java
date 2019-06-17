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

package org.apache.ratis.metrics;


import java.util.Objects;

import com.codahale.metrics.MetricRegistry;

/**
 *
 * This class holds the name and description and JMX related context names for such group of
 * metrics.
 */
public class MetricRegistryInfo {

  protected final String metricsClassName;
  protected final String metricsDescription;
  protected final String metricsContext;
  private final String fullName;

  /**
   * @param metricsClassName   className or component name this metric registry collects metric for
   * @param metricsDescription description of the metrics collected by this registry
   * @param metricsContext     context needs to be in small case as it is used for hadoop2metrics
   */
  public MetricRegistryInfo(String metricsClassName, String metricsDescription,
      String metricsContext) {
    this.metricsClassName = metricsClassName;
    this.metricsDescription = metricsDescription;
    this.metricsContext = metricsContext;
    this.fullName = MetricRegistry.name(metricsClassName, metricsContext);
  }

  /**
   *
   * @return The string context
   */
  public String getMetricsContext() {
    return metricsContext;
  }

  /**
   * Get the description of what this metric registry exposes.
   */
  public String getMetricsDescription() {
    return metricsDescription;
  }

  /**
   * Get the name of the metrics that are being exported by this registry.
   */
  public String getMetricsClassName() {
    return metricsClassName;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof MetricRegistryInfo) {
      return this.hashCode() == obj.hashCode();
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(metricsClassName, metricsDescription, metricsContext);
  }

  public String getName() {
    return fullName;
  }
}
