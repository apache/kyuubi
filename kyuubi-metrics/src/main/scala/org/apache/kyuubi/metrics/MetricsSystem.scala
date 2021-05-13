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

import java.lang.management.ManagementFactory

import com.codahale.metrics.{Gauge, MetricRegistry}
import com.codahale.metrics.jvm._

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.metrics.MetricsConf.METRICS_REPORTERS
import org.apache.kyuubi.metrics.MetricsSystem.maybeSystem
import org.apache.kyuubi.metrics.ReporterType._
import org.apache.kyuubi.service.CompositeService

class MetricsSystem extends CompositeService("MetricsSystem") {

  private val registry = new MetricRegistry

  def incAndGetCount(key: String): Long = synchronized {
    val counter = registry.counter(key)
    counter.inc(1L)
    counter.getCount
  }

  def decAndGetCount(key: String): Long = synchronized {
    val counter = registry.counter(key)
    counter.dec(1L)
    counter.getCount
  }

  def registerGauge[T](name: String, value: => T, default: T): Unit = {
    registry.register(MetricRegistry.name(name), new Gauge[T] {
      override def getValue: T = Option(value).getOrElse(default)
    })
  }

  override def initialize(conf: KyuubiConf): Unit = synchronized {
    registry.registerAll(new GarbageCollectorMetricSet)
    registry.registerAll(new MemoryUsageGaugeSet)
    registry.registerAll(new BufferPoolMetricSet(ManagementFactory.getPlatformMBeanServer))
    registry.registerAll(new ThreadStatesGaugeSet)

    conf.get(METRICS_REPORTERS).map(ReporterType.withName).foreach {
      case JSON => addService(new JsonReporterService(registry))
      case SLF4J => addService(new Slf4jReporterService(registry))
      case CONSOLE => addService(new ConsoleReporterService(registry))
      case JMX => addService(new JMXReporterService(registry))
      case PROMETHEUS => addService(new PrometheusReporterService(registry))
    }
    super.initialize(conf)
  }

  override def start(): Unit = synchronized {
    maybeSystem = Some(this)
    super.start()
  }

  override def stop(): Unit = synchronized {
    maybeSystem = None
    super.stop()
  }
}

object MetricsSystem {

  private var maybeSystem: Option[MetricsSystem] = None

  def tracing[T](func: MetricsSystem => T): Unit = {
    maybeSystem.foreach(func(_))
  }
}
