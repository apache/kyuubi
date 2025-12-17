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

import java.util
import javax.servlet.DispatcherType
import javax.servlet.http.{HttpServlet, HttpServletRequest, HttpServletResponse}

import com.codahale.metrics.MetricRegistry
import io.prometheus.client.CollectorRegistry
import io.prometheus.client.dropwizard.DropwizardExports
import io.prometheus.client.exporter.MetricsServlet
import io.prometheus.client.exporter.common.TextFormat
import org.eclipse.jetty.server.{HttpConfiguration, HttpConnectionFactory, Server, ServerConnector}
import org.eclipse.jetty.servlet.{FilterHolder, ServletContextHandler, ServletHolder}

import org.apache.kyuubi.KyuubiException
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.FRONTEND_JETTY_SEND_VERSION_ENABLED
import org.apache.kyuubi.service.AbstractService
import org.apache.kyuubi.util.JavaUtils

class PrometheusReporterService(registry: MetricRegistry)
  extends AbstractService("PrometheusReporterService") {

  private val bridgeRegistry = new CollectorRegistry

  // VisibleForTesting
  private[metrics] var httpServer: Server = _
  private[metrics] var httpServerConnector: ServerConnector = _
  @volatile protected var isStarted = false

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

    // Add BasicAuth filter if enabled
    addBasicAuthFilterIfEnabled(conf, context)

    new DropwizardExports(registry).register(bridgeRegistry)
    if (conf.get(MetricsConf.METRICS_PROMETHEUS_LABELS_INSTANCE_ENABLED)) {
      val instanceLabel =
        Map("instance" -> s"${JavaUtils.findLocalInetAddress.getCanonicalHostName}:$port")
      context.addServlet(
        new ServletHolder(createPrometheusServletWithLabels(instanceLabel)),
        contextPath)
    } else {
      val metricsServlet = new MetricsServlet(bridgeRegistry)
      context.addServlet(new ServletHolder(metricsServlet), contextPath)
    }

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

  /**
   * Add BasicAuth filter to the servlet context if authentication is enabled
   */
  private def addBasicAuthFilterIfEnabled(
      conf: KyuubiConf,
      context: ServletContextHandler): Unit = {
    val authEnabled = conf.get(MetricsConf.METRICS_PROMETHEUS_AUTH_ENABLED)

    if (authEnabled) {
      val username = conf.get(MetricsConf.METRICS_PROMETHEUS_AUTH_USERNAME)
      val password = conf.get(MetricsConf.METRICS_PROMETHEUS_AUTH_PASSWORD)

      (username, password) match {
        case (Some(user), Some(pass)) =>
          if (user.trim.isEmpty || pass.trim.isEmpty) {
            throw new KyuubiException(
              "Username and password cannot be empty when authentication is enabled")
          }

          val authFilter = new BasicAuthFilter(user, pass)
          val filterHolder = new FilterHolder(authFilter)
          context.addFilter(
            filterHolder,
            "/*",
            util.EnumSet.of(DispatcherType.REQUEST))

          info(s"BasicAuth enabled for Prometheus metrics endpoint with username: $user")

        case _ =>
          throw new KyuubiException(
            "Both username and password must be configured when " +
              "kyuubi.metrics.prometheus.auth.enabled is true")
      }
    } else {
      info("BasicAuth disabled for Prometheus metrics endpoint")
    }
  }

  private def createPrometheusServletWithLabels(labels: Map[String, String]): HttpServlet = {
    new HttpServlet {
      override def doGet(request: HttpServletRequest, response: HttpServletResponse): Unit = {
        try {
          response.setContentType("text/plain;charset=utf-8")
          response.setStatus(HttpServletResponse.SC_OK)
          response.getWriter.print(getMetricsSnapshot(labels))
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

  private def getMetricsSnapshot(labels: Map[String, String]): String = {
    val metricsSnapshotWriter = new java.io.StringWriter
    val contentType = TextFormat.chooseContentType(null)
    TextFormat.writeFormat(contentType, metricsSnapshotWriter, bridgeRegistry.metricFamilySamples())
    val labelStr = labels.map { case (k, v) => s"""$k="$v"""" }.toArray.sorted.mkString(",")
    metricsSnapshotWriter.toString.split("\n").map { line =>
      if (line.startsWith("#")) {
        line
      } else {
        line.split("\\s+", 2) match {
          case Array(metrics, rest) =>
            val metricsWithNewLabels = PrometheusReporterService.applyExtraLabels(metrics, labelStr)
            s"""$metricsWithNewLabels $rest"""
          case _ => line
        }
      }
    }.mkString("\n")
  }
}

object PrometheusReporterService {
  def applyExtraLabels(metrics: String, labelStr: String): String = {
    val labelStartIdx = metrics.indexOf("{")
    val labelEndIdx = metrics.indexOf("}")
    if (labelStartIdx > 0 && labelEndIdx > 0) {
      metrics.substring(0, labelEndIdx).stripSuffix(",") +
        "," +
        labelStr +
        metrics.substring(labelEndIdx)
    } else {
      s"$metrics{${labelStr}}"
    }
  }
}
