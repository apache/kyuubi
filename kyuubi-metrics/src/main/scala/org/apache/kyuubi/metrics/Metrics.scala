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

import java.time.Duration
import java.util.concurrent.ConcurrentHashMap

import com.google.common.util.concurrent.AtomicDouble
import io.micrometer.core.instrument._

import org.apache.kyuubi.metrics.micrometer.MicrometerMetricsService

object Metrics {

  final val percentiles = Array[Double](0.5, 0.75, 0.95, 0.99, 0.999)

  private[metrics] var maybe: Option[MicrometerMetricsService] = None

  private[metrics] val gaugeHolder = new ConcurrentHashMap[Meter.Id, AtomicDouble]

  /**
   * Increase counter. Counter is a single, monotonically increasing, cumulative metric.
   *
   * @param name  Name of the gauge being registered.
   * @param tags  Sequence of dimensions for breaking down the name.
   * @param value Amount to add to the counter, which must be positive.
   * @param unit  Base unit of the counter.
   * @param desc  Description text of the counter.
   */
  def count(name: String, tags: String*)
           (value: Double = 1.0,
            unit: Option[String] = None,
            desc: Option[String] = None): Unit = metrics { ms =>
    Counter.builder(name)
      .tags(Tags.of(tags: _*))
      .baseUnit(unit.orNull)
      .description(desc.orNull)
      .register(ms.registry)
      .increment(value)
  }

  /**
   * Increase the value of gauge
   *
   * @param name  Name of the gauge being registered.
   * @param tags  Sequence of dimensions for breaking down the name.
   * @param value Amount to add to the gauge.
   * @param unit  Base unit of the gauge.
   * @param desc  Description text of the gauge.
   */
  def inc(name: String, tags: String*)
         (value: Double = 1.0,
          unit: Option[String] = None,
          desc: Option[String] = None): Unit = metrics { ms =>
    getOrCreateGauge(ms.registry, name, tags: _*)(unit, desc).addAndGet(value)
  }

  /**
   * Decrease the value of value
   *
   * @param name  Name of the gauge being registered.
   * @param tags  Sequence of dimensions for breaking down the name.
   * @param value Amount to subtract to the gauge
   * @param unit  Base unit of the gauge.
   * @param desc  Description text of the gauge.
   */
  def dec(name: String, tags: String*)
         (value: Double = 1.0,
          unit: Option[String] = None,
          desc: Option[String] = None): Unit = metrics { ms =>
    getOrCreateGauge(ms.registry, name, tags: _*)(unit, desc).addAndGet(-value)
  }

  /**
   * Update the value of gauge.
   *
   * @param name  Name of the gauge being registered.
   * @param tags  Sequence of dimensions for breaking down the name.
   * @param value Amount for an event being measured.
   * @param unit  Base unit of the gauge.
   * @param desc  Description text of the gauge.
   */
  def gauge(name: String, tags: String*)
           (value: Double,
            unit: Option[String] = None,
            desc: Option[String] = None): Unit = metrics { ms =>
    getOrCreateGauge(ms.registry, name, tags: _*)(unit, desc).set(value)
  }

  /**
   * Register a gauge that reports the value of the object after the function.
   *
   * @param name Name of the gauge being registered.
   * @param tags Sequence of dimensions for breaking down the name.
   * @param unit Base unit of the gauge.
   * @param desc Description text of the gauge.
   * @param func Function that produces an instantaneous gauge value from the state object.
   */
  def registerGauge(name: String, tags: String*)
                   (unit: Option[String] = None, desc: Option[String] = None)
                   (func: () => Double): Unit = metrics { ms =>
    Gauge.builder(name, func, (func: () => Double) => func())
      .tags(Tags.of(tags: _*))
      .baseUnit(unit.orNull)
      .description(desc.orNull)
      .register(ms.registry)
  }

  /**
   * Updates the statistics kept by the summary with the specified amount.
   *
   * @param name  Name of the gauge being registered.
   * @param tags  Sequence of dimensions for breaking down the name.
   * @param value Amount for an event being measured.
   * @param unit  Base unit of the distributionSummary.
   * @param desc  Description text of the distributionSummary.
   */
  def summary(name: String, tags: String*)
             (value: Double,
              unit: Option[String] = None,
              desc: Option[String] = None): Unit = metrics { ms =>
    DistributionSummary.builder(name)
      .tags(Tags.of(tags: _*))
      .baseUnit(unit.orNull)
      .description(desc.orNull)
      .publishPercentileHistogram()
      .publishPercentiles(percentiles: _*)
      .register(ms.registry)
      .record(value)
  }

  /**
   * Executes the callable `func` and records the time taken. It's for tracking of a large number
   * of short running events which typically will complete under a minute.
   *
   * @param name Name of the gauge being registered.
   * @param tags Sequence of dimensions for breaking down the name.
   * @param desc Description text of the Timer.
   * @param func Function to execute and measure the execution time.
   * @tparam R The return type of the `func`.
   * @return The return value of `func`.
   */
  def time[R](name: String, tags: String*)
             (desc: Option[String] = None)
             (func: () => R): R = maybe match {
    case Some(ms) =>
      Timer.builder(name)
        .tags(Tags.of(tags: _*))
        .description(desc.orNull)
        .publishPercentileHistogram()
        .publishPercentiles(percentiles: _*)
        .register(ms.registry)
        .recordCallable(() => func())
    case None => func()
  }

  /**
   * A long task timer is used to track the total duration of all in-flight long-running tasks
   * and the number of such tasks.
   *
   * @param name        Name of the gauge being registered.
   * @param tags        Sequence of dimensions for breaking down the name.
   * @param minDuration Sets a lower bound on histogram buckets that are shipped to monitoring
   *                    systems that support aggregable percentile approximations.
   * @param maxDuration Sets an upper bound on histogram buckets that are shipped to monitoring
   *                    systems that support aggregable percentile approximations.
   * @param desc        Description text of the LongTaskTimer.
   * @return If [[MetricsConf.METRICS_ENABLED]] enabled, a timing sample with start time recorded,
   *         otherwise [[None]].
   */
  def longTaskTimer(name: String, tags: String*)
                   (minDuration: Duration = Duration.ZERO,
                    maxDuration: Duration = Duration.ofHours(24),
                    desc: Option[String] = None): Option[LongTaskTimer.Sample] = maybe.map { ms =>
    LongTaskTimer.builder(name)
      .tags(Tags.of(tags: _*))
      .description(desc.orNull)
      .publishPercentileHistogram()
      .publishPercentiles(percentiles: _*)
      .minimumExpectedValue(minDuration)
      .maximumExpectedValue(maxDuration)
      .register(ms.registry)
      .start()
  }

  private def metrics[R](func: MicrometerMetricsService => R): Unit = {
    maybe.foreach(func(_))
  }

  private def getOrCreateGauge(registry: MeterRegistry, name: String, tags: String*)
                              (unit: Option[String] = None,
                               desc: Option[String] = None): AtomicDouble = {
    val _tags = Tags.of(tags: _*)
    val id = new Meter.Id(name, _tags, null, null, Meter.Type.GAUGE)
    gaugeHolder.computeIfAbsent(id, _ => {
      val refValue = new AtomicDouble
      Gauge.builder(name, refValue, (r: AtomicDouble) => r.get)
        .tags(_tags)
        .baseUnit(unit.orNull)
        .description(desc.orNull)
        .register(registry)
      refValue
    })
  }
}
