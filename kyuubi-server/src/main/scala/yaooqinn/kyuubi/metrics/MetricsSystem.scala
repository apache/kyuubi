/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package yaooqinn.kyuubi.metrics

import java.io.Closeable
import java.lang.management.ManagementFactory
import java.util.concurrent.TimeUnit

import com.codahale.metrics._
import com.codahale.metrics.jvm._
import org.apache.kyuubi.Logging
import org.apache.spark.{KyuubiSparkUtil, SparkConf}
import org.apache.spark.KyuubiConf._

private[kyuubi] class MetricsSystem(conf: SparkConf) extends Logging {

  private val registry = new MetricRegistry

  registry.registerAll(new GarbageCollectorMetricSet)
  registry.registerAll(new MemoryUsageGaugeSet)
  registry.registerAll(new BufferPoolMetricSet(ManagementFactory.getPlatformMBeanServer))
  registry.registerAll(new ThreadStatesGaugeSet)
  registry.registerAll(new ClassLoadingGaugeSet)

  private val reportInterval = KyuubiSparkUtil.timeStringAsMs(conf.get(METRICS_REPORT_INTERVAL))

  private val reporter: Array[Closeable] =
    conf.get(METRICS_REPORTER).split(",").map(_.trim.toUpperCase).flatMap {
    case "CONSOLE" =>
      val reporter = ConsoleReporter.forRegistry(registry)
        .convertDurationsTo(TimeUnit.SECONDS)
        .convertDurationsTo(TimeUnit.MILLISECONDS)
        .build()
      reporter.start(reportInterval, TimeUnit.MILLISECONDS)
      Some(reporter)
    case "JMX" =>
      val reporter = JmxReporter.forRegistry(registry)
        .convertRatesTo(TimeUnit.SECONDS)
        .convertDurationsTo(TimeUnit.MILLISECONDS)
        .build()
      reporter.start()
      Some(reporter)
    case "JSON" =>
      val reporter = new JsonFileReporter(conf, registry)
      reporter.start()
      Some(reporter)
    case other =>
      warn(s"$other as a metrics report is not support yet")
      None
  }

  def registerGauge[T](name: String, value: => T, default: T): Unit = {
    registry.register(MetricRegistry.name(name), new Gauge[T] {
      override def getValue: T = Option(value).getOrElse(default)
    })
  }

  def close(): Unit = {
    reporter.foreach(_.close())
  }

  val OPEN_CONNECTIONS: Counter = registry.counter(MetricRegistry.name("open_connections"))
  val OPEN_OPERATIONS: Counter = registry.counter(MetricRegistry.name("open_operations"))
  val TOTAL_CONNECTIONS: Counter = registry.counter(MetricRegistry.name("total_connections"))
  val RUNNING_QUERIES: Counter = registry.counter(MetricRegistry.name("running_queries"))
  val ERROR_QUERIES: Counter = registry.counter(MetricRegistry.name("error_queries"))
  val TOTAL_QUERIES: Counter = registry.counter(MetricRegistry.name("total_queries"))
}

object MetricsSystem {

  private var maybeSystem: Option[MetricsSystem] = None

  def init(conf: SparkConf): Option[MetricsSystem] = {
    if (conf.get(METRICS_ENABLE).toBoolean) {
      val system = new MetricsSystem(conf)
      maybeSystem = Some(system)
      maybeSystem
    } else {
      None
    }
  }

  def get: Option[MetricsSystem] = maybeSystem

  def close(): Unit = {
    maybeSystem.foreach(_.close())
    maybeSystem = None
  }
}
