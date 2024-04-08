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

package org.apache.ratis.metrics;

import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.MemoizedSupplier;

import java.util.Objects;
import java.util.function.Supplier;

/**
 * This class holds the name, description and JMX related context names for such group of metrics.
 * <p>
 * This class is immutable.
 */
public class MetricRegistryInfo {
  private final String prefix;
  private final String metricsDescription;
  private final String metricsComponentName;
  private final String applicationName;

  private final Supplier<Integer> hash = MemoizedSupplier.valueOf(this::computeHash);

  /**
   * @param prefix   className or component name this metric registry collects metric for
   * @param applicationName application Name needs to be in small case as it is used for hadoop2metrics
   * @param metricsComponentName component name needs to be in small case as it is used for hadoop2metrics
   * @param metricsDescription description of the metrics collected by this registry
   *
   */
  public MetricRegistryInfo(String prefix, String applicationName, String metricsComponentName,
      String metricsDescription) {
    this.prefix = prefix;
    this.applicationName = applicationName;
    this.metricsComponentName = metricsComponentName;
    this.metricsDescription = metricsDescription;
  }

  public String getApplicationName() {
    return this.applicationName;
  }

  /**
   *
   * @return component name for which Metric is getting collected
   */
  public String getMetricsComponentName() {
    return metricsComponentName;
  }

  /**
   * Get the description of what this metric registry exposes.
   */
  public String getMetricsDescription() {
    return metricsDescription;
  }

  /**
   * Get the unique prefix for metrics that are being exported by this registry.
   */
  public String getPrefix() {
    return prefix;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    } else if (!(obj instanceof MetricRegistryInfo)) {
      return false;
    }
    final MetricRegistryInfo that = (MetricRegistryInfo) obj;
    return Objects.equals(prefix, that.prefix)
        && Objects.equals(metricsDescription, that.metricsDescription)
        && Objects.equals(metricsComponentName, that.metricsComponentName)
        && Objects.equals(applicationName, that.applicationName);
  }

  @Override
  public int hashCode() {
    return hash.get();
  }

  private Integer computeHash() {
    return Objects.hash(prefix, metricsDescription, metricsComponentName);
  }

  @Override
  public String toString() {
    return JavaUtils.getClassSimpleName(getClass())
        + ": applicationName=" + getApplicationName()
        + ", metricsComponentName=" + getMetricsComponentName()
        + ", prefix=" + getPrefix()
        + ", metricsDescription=" + getMetricsDescription();
  }
}
