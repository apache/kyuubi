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

  private var instanceLabel: String = _

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

    instanceLabel = s"""instance="${JavaUtils.findLocalInetAddress.getCanonicalHostName}:$port"""

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

  private def getMetricsSnapshot(): String = {
    import scala.collection.JavaConverters._

    val gaugesLabel = s"""{type="gauges",$instanceLabel}"""
    val countersLabel = s"""{type="counters",$instanceLabel}"""
    val metersLabel = countersLabel
    val histogramslabels = s"""{type="histograms",,$instanceLabel}"""
    val timersLabels = s"""{type="timers",,$instanceLabel}"""

    val sb = new StringBuilder()
    registry.getGauges.asScala.foreach { case (k, v) =>
      if (!v.getValue.isInstanceOf[String]) {
        sb.append(s"${normalizeKey(k)}Number$gaugesLabel ${v.getValue}\n")
        sb.append(s"${normalizeKey(k)}Value$gaugesLabel ${v.getValue}\n")
      }
    }
    registry.getCounters.asScala.foreach { case (k, v) =>
      sb.append(s"${normalizeKey(k)}Count$countersLabel ${v.getCount}\n")
    }
    registry.getHistograms.asScala.foreach { case (k, h) =>
      val snapshot = h.getSnapshot
      val prefix = normalizeKey(k)
      sb.append(s"${prefix}Count$histogramslabels ${h.getCount}\n")
      sb.append(s"${prefix}Max$histogramslabels ${snapshot.getMax}\n")
      sb.append(s"${prefix}Mean$histogramslabels ${snapshot.getMean}\n")
      sb.append(s"${prefix}Min$histogramslabels ${snapshot.getMin}\n")
      sb.append(s"${prefix}50thPercentile$histogramslabels ${snapshot.getMedian}\n")
      sb.append(s"${prefix}75thPercentile$histogramslabels ${snapshot.get75thPercentile}\n")
      sb.append(s"${prefix}95thPercentile$histogramslabels ${snapshot.get95thPercentile}\n")
      sb.append(s"${prefix}98thPercentile$histogramslabels ${snapshot.get98thPercentile}\n")
      sb.append(s"${prefix}99thPercentile$histogramslabels ${snapshot.get99thPercentile}\n")
      sb.append(s"${prefix}999thPercentile$histogramslabels ${snapshot.get999thPercentile}\n")
      sb.append(s"${prefix}StdDev$histogramslabels ${snapshot.getStdDev}\n")
    }
    registry.getMeters.entrySet.iterator.asScala.foreach { kv =>
      val prefix = normalizeKey(kv.getKey)
      val meter = kv.getValue
      sb.append(s"${prefix}Count$metersLabel ${meter.getCount}\n")
      sb.append(s"${prefix}MeanRate$metersLabel ${meter.getMeanRate}\n")
      sb.append(s"${prefix}OneMinuteRate$metersLabel ${meter.getOneMinuteRate}\n")
      sb.append(s"${prefix}FiveMinuteRate$metersLabel ${meter.getFiveMinuteRate}\n")
      sb.append(s"${prefix}FifteenMinuteRate$metersLabel ${meter.getFifteenMinuteRate}\n")
    }
    registry.getTimers.entrySet.iterator.asScala.foreach { kv =>
      val prefix = normalizeKey(kv.getKey)
      val timer = kv.getValue
      val snapshot = timer.getSnapshot
      sb.append(s"${prefix}Count$timersLabels ${timer.getCount}\n")
      sb.append(s"${prefix}Max$timersLabels ${snapshot.getMax}\n")
      sb.append(s"${prefix}Mean$timersLabels ${snapshot.getMean}\n")
      sb.append(s"${prefix}Min$timersLabels ${snapshot.getMin}\n")
      sb.append(s"${prefix}50thPercentile$timersLabels ${snapshot.getMedian}\n")
      sb.append(s"${prefix}75thPercentile$timersLabels ${snapshot.get75thPercentile}\n")
      sb.append(s"${prefix}95thPercentile$timersLabels ${snapshot.get95thPercentile}\n")
      sb.append(s"${prefix}98thPercentile$timersLabels ${snapshot.get98thPercentile}\n")
      sb.append(s"${prefix}99thPercentile$timersLabels ${snapshot.get99thPercentile}\n")
      sb.append(s"${prefix}999thPercentile$timersLabels ${snapshot.get999thPercentile}\n")
      sb.append(s"${prefix}StdDev$timersLabels ${snapshot.getStdDev}\n")
      sb.append(s"${prefix}FifteenMinuteRate$timersLabels ${timer.getFifteenMinuteRate}\n")
      sb.append(s"${prefix}FiveMinuteRate$timersLabels ${timer.getFiveMinuteRate}\n")
      sb.append(s"${prefix}OneMinuteRate$timersLabels ${timer.getOneMinuteRate}\n")
      sb.append(s"${prefix}MeanRate$timersLabels ${timer.getMeanRate}\n")
    }
    sb.toString()
  }

  private def normalizeKey(key: String): String = {
    s"${key.replaceAll("[^a-zA-Z0-9]", "_")}_"
  }
}
