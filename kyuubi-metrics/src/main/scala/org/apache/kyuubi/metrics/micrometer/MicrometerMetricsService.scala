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

package org.apache.kyuubi.metrics.micrometer

import io.micrometer.core.instrument.binder.jvm.{ClassLoaderMetrics, JvmGcMetrics, JvmMemoryMetrics, JvmThreadMetrics}
import io.micrometer.core.instrument.binder.system.{FileDescriptorMetrics, ProcessorMetrics}
import io.micrometer.core.instrument.composite.CompositeMeterRegistry

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.metrics.{ExporterType, Metrics}
import org.apache.kyuubi.metrics.ExporterType._
import org.apache.kyuubi.metrics.MetricsConf.{METRICS_EXPORTERS, METRICS_HISTOGRAM}
import org.apache.kyuubi.service.CompositeService

class MicrometerMetricsService
  extends CompositeService("MicrometerMetricsSystem") with Logging {

  private[metrics] val registry: CompositeMeterRegistry = new CompositeMeterRegistry

  override def initialize(conf: KyuubiConf): Unit = synchronized {
    new ClassLoaderMetrics().bindTo(registry)
    new ProcessorMetrics().bindTo(registry)
    new JvmMemoryMetrics().bindTo(registry)
    new JvmGcMetrics().bindTo(registry)
    new JvmThreadMetrics().bindTo(registry)
    new FileDescriptorMetrics().bindTo(registry)

    conf.get(METRICS_EXPORTERS).map(ExporterType.withName).foreach {
      case JSON => addService(new JsonMetricsExporterService(registry))
      case SLF4J => addService(new Slf4jMetricsExporterService(registry))
      case PROMETHEUS => addService(new PrometheusMetricsExporterService(registry))
    }
    super.initialize(conf)
  }

  override def start(): Unit = synchronized {
    Metrics.maybeMetricsService = Some(this)
    Metrics.enableHistogram = conf.get(METRICS_HISTOGRAM)
    super.start()
  }

  override def stop(): Unit = synchronized {
    Metrics.maybeMetricsService = None
    super.stop()
  }
}
