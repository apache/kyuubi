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

import com.codahale.metrics.MetricRegistry
import com.google.common.annotations.VisibleForTesting
import io.prometheus.client.CollectorRegistry
import io.prometheus.client.dropwizard.DropwizardExports
import io.prometheus.client.exporter.MetricsServlet
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.servlet.{ServletContextHandler, ServletHolder}

import org.apache.kyuubi.KyuubiException
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.service.AbstractService

class PrometheusReporterService(registry: MetricRegistry)
  extends AbstractService("PrometheusReporterService") {

  private val bridgeRegistry = new CollectorRegistry

  @VisibleForTesting
  private[metrics] var httpServer: Server = _
  @volatile protected var isStarted = false

  override def initialize(conf: KyuubiConf): Unit = {
    val port = conf.get(MetricsConf.METRICS_PROMETHEUS_PORT)
    val contextPath = conf.get(MetricsConf.METRICS_PROMETHEUS_PATH)
    httpServer = new Server(port)
    val context = new ServletContextHandler
    context.setContextPath("/")
    httpServer.setHandler(context)

    new DropwizardExports(registry).register(bridgeRegistry)
    val metricsServlet = new MetricsServlet(bridgeRegistry)
    context.addServlet(new ServletHolder(metricsServlet), contextPath)

    super.initialize(conf)
  }

  override def start(): Unit = synchronized {
    if (!isStarted) {
      try {
        httpServer.start()
      } catch {
        case rethrow: Exception =>
          stopHttpServer()
          throw new KyuubiException("Cannot start prometheus metrics HTTP server", rethrow)
      }
      isStarted = true
    }
    super.start()
  }

  override def stop(): Unit = synchronized {
    if (isStarted) {
      stopHttpServer()
      isStarted = false
    }
    super.stop()
  }

  private def stopHttpServer(): Unit = {
    if (httpServer != null) {
      try {
        httpServer.stop()
      } catch {
        case err: Exception => error("Cannot safely stop prometheus metrics HTTP server", err)
      } finally {
        httpServer = null
      }
    }
  }
}
