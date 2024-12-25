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

import javax.servlet.http.{HttpServlet, HttpServletRequest, HttpServletResponse}

import com.codahale.metrics.MetricRegistry
import org.eclipse.jetty.server.{HttpConfiguration, HttpConnectionFactory, Server, ServerConnector}
import org.eclipse.jetty.servlet.{ServletContextHandler, ServletHolder}

import org.apache.kyuubi.KyuubiException
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.FRONTEND_JETTY_SEND_VERSION_ENABLED
import org.apache.kyuubi.service.AbstractService
import org.apache.kyuubi.util.JavaUtils

class PrometheusReporterService(registry: MetricRegistry)
  extends AbstractService("PrometheusReporterService") {

  // VisibleForTesting
  private[metrics] var httpServer: Server = _
  private[metrics] var httpServerConnector: ServerConnector = _
  @volatile protected var isStarted = false

  private var instance: String = _

  override def initialize(conf: KyuubiConf): Unit = {
    val port = conf.get(MetricsConf.METRICS_PROMETHEUS_PORT)
    val contextPath = conf.get(MetricsConf.METRICS_PROMETHEUS_PATH)
    val jettyVersionEnabled = conf.get(FRONTEND_JETTY_SEND_VERSION_ENABLED)

    val httpConf = new HttpConfiguration()
    httpConf.setSendServerVersion(jettyVersionEnabled)
    httpServer = new Server()
    httpServerConnector = new ServerConnector(httpServer, new HttpConnectionFactory(httpConf))
    httpServerConnector.setPort(port)
    httpServer.addConnector(httpServerConnector)

    val context = new ServletContextHandler
    context.setContextPath("/")
    httpServer.setHandler(context)

    context.addServlet(new ServletHolder(createPrometheusServlet()), contextPath)

    instance = s"${JavaUtils.findLocalInetAddress.getCanonicalHostName}:$port"

    super.initialize(conf)
  }

  override def start(): Unit = synchronized {
    if (!isStarted) {
      try {
        httpServer.start()
        httpServerConnector.start()
        info(s"Prometheus metrics HTTP server has started at ${httpServer.getURI}.")
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
        httpServerConnector.stop()
        httpServer.stop()
        info("Prometheus metrics HTTP server has stopped.")
      } catch {
        case err: Exception => error("Cannot safely stop prometheus metrics HTTP server", err)
      } finally {
        httpServer = null
        httpServerConnector = null
      }
    }
  }

  private def createPrometheusServlet(): HttpServlet = {
    new HttpServlet {
      override def doGet(request: HttpServletRequest, response: HttpServletResponse): Unit = {
        try {
          response.setContentType("text/plain;charset=utf-8")
          response.setStatus(HttpServletResponse.SC_OK)
          response.getWriter.print(getMetricsSnapshot())
        } catch {
          case e: IllegalArgumentException =>
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, e.getMessage)
          case e: Exception =>
            warn(s"GET ${request.getRequestURI} failed: $e", e)
            throw e
        }
      }
      // ensure TRACE is not supported
      override protected def doTrace(req: HttpServletRequest, res: HttpServletResponse): Unit = {
        res.sendError(HttpServletResponse.SC_METHOD_NOT_ALLOWED)
      }
    }
  }

  // scalastyle:off line.size.limit
  private def getMetricsSnapshot(): String = {
    import scala.collection.JavaConverters._

    val sb = new StringBuilder()
    registry.getGauges.asScala.foreach { case (k, v) =>
      if (!v.getValue.isInstanceOf[String]) {
        sb.append(s"""${normalizeKey(k)}{type="gauges",instance="$instance"} ${v.getValue}\n""")
      }
    }
    registry.getCounters.asScala.foreach { case (k, v) =>
      sb.append(s"""${normalizeKey(k)}{type="counters",instance="$instance"} ${v.getCount}\n""")
    }
    registry.getHistograms.asScala.foreach { case (k, h) =>
      val snapshot = h.getSnapshot
      val prefix = normalizeKey(k)
      sb.append(s"""${prefix}_count{type="histograms",instance="$instance"} ${h.getCount()}\n""")
      sb.append(
        s"""${prefix}{quantile="0.5",type="histograms",instance="$instance"} ${snapshot.getMedian()}\n""")
      sb.append(
        s"""${prefix}{quantile="0.75",type="histograms",instance="$instance"} ${snapshot.get75thPercentile()}\n""")
      sb.append(
        s"""${prefix}{quantile="0.95",type="histograms",instance="$instance"} ${snapshot.get95thPercentile()}\n""")
      sb.append(
        s"""${prefix}{quantile="0.98",type="histograms",instance="$instance"} ${snapshot.get98thPercentile()}\n""")
      sb.append(
        s"""${prefix}{quantile="0.99",type="histograms",instance="$instance"} ${snapshot.get99thPercentile()}\n""")
      sb.append(
        s"""${prefix}{quantile="0.999",type="histograms",instance="$instance"} ${snapshot.get999thPercentile()}\n""")
    }
    registry.getMeters.entrySet.iterator.asScala.foreach { kv =>
      val prefix = normalizeKey(kv.getKey)
      val meter = kv.getValue
      sb.append(s"""${prefix}{type="counters",instance="$instance"} ${meter.getCount}\n""")
    }
    registry.getTimers.entrySet.iterator.asScala.foreach { kv =>
      val prefix = normalizeKey(kv.getKey)
      val timer = kv.getValue
      val snapshot = timer.getSnapshot
      sb.append(s"""${prefix}_count{type="timers",instance="$instance"} ${timer.getCount()}\n""")
      sb.append(
        s"""${prefix}{quantile="0.5",type="timers",instance="$instance"} ${snapshot.getMedian()}\n""")
      sb.append(s"""${prefix}{quantile="0.75",type="timers",instance="$instance"} ${snapshot.get75thPercentile()}\n""")
      sb.append(s"""${prefix}{quantile="0.95",type="timers",instance="$instance"} ${snapshot.get95thPercentile()}\n""")
      sb.append(s"""${prefix}{quantile="0.98",type="timers",instance="$instance"} ${snapshot.get98thPercentile()}\n""")
      sb.append(s"""${prefix}{quantile="0.99",type="timers",instance="$instance"} ${snapshot.get99thPercentile()}\n""")
      sb.append(s"""${prefix}{quantile="0.999",type="timers",instance="$instance"} ${snapshot.get999thPercentile()}\n""")
    }
    sb.toString()
  }
  // scalastyle:on line.size.limit

  private def normalizeKey(key: String): String = {
    s"${key.replaceAll("[^a-zA-Z0-9]", "_")}"
  }
}
