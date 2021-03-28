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

import java.io.OutputStream
import java.net.{InetAddress, InetSocketAddress}
import java.nio.charset.StandardCharsets
import java.time.Duration
import java.util.Properties
import java.util.concurrent.{ScheduledExecutorService, TimeUnit}

import com.sun.net.httpserver.{HttpExchange, HttpServer}
import io.micrometer.core.instrument.composite.CompositeMeterRegistry
import io.micrometer.prometheus.PrometheusMeterRegistry

import org.apache.kyuubi.metrics.MetricsConf._
import org.apache.kyuubi.service.AbstractService
import org.apache.kyuubi.util.ThreadUtils

class PrometheusMetricsExporterService(registry: CompositeMeterRegistry)
  extends AbstractService("PrometheusMetricsExporterService") {

  lazy val executor: ScheduledExecutorService =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("prometheus_http_server")

  var server: HttpServer = _

  override def start(): Unit = synchronized {
    val props = new Properties
    props.setProperty("prometheus.step",
      Duration.ofMillis(conf.get(METRICS_PROMETHEUS_INTERVAL)).toString)
    props.setProperty("prometheus.descriptions", conf.get(METRICS_PROMETHEUS_DESCRIPTION).toString)
    val prometheusMeterRegistry = new PrometheusMeterRegistry(key => props.getProperty(key))

    server = HttpServer.create(new InetSocketAddress(conf.get(METRICS_PROMETHEUS_HOST)
      .map(InetAddress.getByName).getOrElse(InetAddress.getLocalHost),
      conf.get(METRICS_PROMETHEUS_PORT)), 0)
    server.setExecutor(executor)

    val ctxPath = conf.get(METRICS_PROMETHEUS_CONTEXT_PATH)
    server.createContext(ctxPath, (httpExchange: HttpExchange) => {
      val responseBytes = prometheusMeterRegistry.scrape().getBytes(StandardCharsets.UTF_8)
      httpExchange.sendResponseHeaders(200, responseBytes.length)
      var os: OutputStream = null
      try {
        os = httpExchange.getResponseBody
        os.write(responseBytes)
        os.flush()
      } finally {
        os.close()
      }
    })
    server.start()

    info(s"Expose Prometheus metrics at " +
      s"http://${server.getAddress.getAddress.getHostAddress}:${server.getAddress.getPort}$ctxPath")

    registry.add(prometheusMeterRegistry)
    super.start()
  }

  override def stop(): Unit = synchronized {
    server.stop(Duration.ofSeconds(3).getSeconds.toInt)
    executor.shutdown()
    try {
      executor.awaitTermination(3, TimeUnit.SECONDS)
    } catch {
      case i: InterruptedException =>
        warn(s"Exceeded to shutdown Prometheus HTTP Server ", i)
    }
    super.stop()
  }
}

