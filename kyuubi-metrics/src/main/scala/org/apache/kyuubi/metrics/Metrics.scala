/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kyuubi.metrics

import java.util.concurrent.ConcurrentHashMap

import com.google.common.util.concurrent.AtomicDouble
import io.micrometer.core.instrument._

import org.apache.kyuubi.metrics.micrometer.MicrometerMetricsService

object Metrics {

  final val percentiles = Array[Double](0.5, 0.75, 0.95, 0.99, 0.999)

  private[metrics] var maybeMetricsService: Option[MicrometerMetricsService] = None
  private[metrics] var enableHistogram: Boolean = true

  private[metrics] val gaugeHolder = new ConcurrentHashMap[Meter.Id, AtomicDouble]

  /**
   * Increase counter. Counter is a single, monotonically increasing, cumulative metric.
   *
   * @param name  Name of the gauge being registered.
   * @param tags  Sequence of dimensions for breaking down the name.
   * @param value Amount to add to the counter, which must be positive.
   */
  def count(name: String, tags: String*)
           (value: Double = 1.0): Unit = metrics { ms =>
    ms.registry.counter(name, Tags.of(tags: _*)).increment(value)
  }

  /**
   * Increase the value of gauge
   *
   * @param name  Name of the gauge being registered.
   * @param tags  Sequence of dimensions for breaking down the name.
   * @param value Amount to add to the gauge
   */
  def inc(name: String, tags: String*)
         (value: Double = 1.0): Unit = metrics { ms =>
    getOrCreateGauge(ms.registry, name, tags: _*).addAndGet(value)
  }

  /**
   * Decrease the value of value
   *
   * @param name  Name of the gauge being registered.
   * @param tags  Sequence of dimensions for breaking down the name.
   * @param value Amount to subtract to the gauge
   */
  def dec(name: String, tags: String*)
         (value: Double = 1.0): Unit = metrics { ms =>
    getOrCreateGauge(ms.registry, name, tags: _*).addAndGet(-value)
  }

  /**
   * Update the value of gauge.
   *
   * @param name  Name of the gauge being registered.
   * @param tags  Sequence of dimensions for breaking down the name.
   * @param value Amount for an event being measured.
   */
  def gauge(name: String, tags: String*)
           (value: Double): Unit = metrics { ms =>
    getOrCreateGauge(ms.registry, name, tags: _*).set(value)
  }

  /**
   * Register a gauge that reports the value of the object after the function.
   *
   * @param name Name of the gauge being registered.
   * @param tags Sequence of dimensions for breaking down the name.
   * @param func Function that produces an instantaneous gauge value from the state object.
   */
  def registerGauge(name: String, tags: String*)
                   (func: () => Double): Unit = metrics { ms =>
    ms.registry.gauge(name, Tags.of(tags: _*), func, (func: () => Double) => func())
  }

  /**
   * Updates the statistics kept by the summary with the specified amount.
   *
   * @param name  Name of the gauge being registered.
   * @param tags  Sequence of dimensions for breaking down the name.
   * @param value Amount for an event being measured.
   */
  def summary(name: String, tags: String*)
             (value: Double): Unit = metrics { ms =>
    DistributionSummary.builder(name)
      .tags(Tags.of(tags: _*))
      .publishPercentiles(percentiles: _*)
      .publishPercentileHistogram(enableHistogram)
      .register(ms.registry)
      .record(value)
  }

  /**
   * Executes the callable `func` and records the time taken.
   *
   * @param name Name of the gauge being registered.
   * @param tags Sequence of dimensions for breaking down the name.
   * @param func Function to execute and measure the execution time.
   * @tparam R he return type of the `func`.
   * @return The return value of `func`.
   */
  def time[R](name: String, tags: String*)
             (func: => R): R = maybeMetricsService match {
    case Some(ms) =>
      Timer.builder(name)
        .tags(Tags.of(tags: _*))
        .publishPercentiles(percentiles: _*)
        .publishPercentileHistogram(enableHistogram)
        .register(ms.registry)
        .recordCallable(() => func)
    case None => func
  }

  def longRun[R](name: String, tags: String*)
                (func: => R): R = maybeMetricsService match {
    case Some(ms) =>
      ms.registry.more().longTaskTimer(name, Tags.of(tags: _*)).recordCallable(() => func)
    case None => func
  }

  private def metrics(func: MicrometerMetricsService => Unit): Unit = {
    maybeMetricsService.foreach(func(_))
  }

  private def getOrCreateGauge(registry: MeterRegistry,
                               name: String, tags: String*): AtomicDouble = {
    val _tags = Tags.of(tags: _*)
    val id = new Meter.Id(name, _tags, null, null, Meter.Type.GAUGE)
    gaugeHolder.computeIfAbsent(id, _ => {
      val refValue = new AtomicDouble
      registry.gauge(name, _tags, refValue)
      refValue
    })
  }
}
