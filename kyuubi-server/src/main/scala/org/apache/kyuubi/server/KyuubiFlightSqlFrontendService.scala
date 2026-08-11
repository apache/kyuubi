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

package org.apache.kyuubi.server

import java.util.concurrent.atomic.AtomicBoolean

import scala.util.control.NonFatal

import org.apache.arrow.flight.{FlightServer, Location}
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}

import org.apache.kyuubi.{KyuubiException, Logging}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.ha.client.{FlightSqlServiceDiscovery, ServiceDiscovery}
import org.apache.kyuubi.metrics.{MetricsConstants, MetricsSystem}
import org.apache.kyuubi.server.flight.{KyuubiFlightAuthHandler, KyuubiFlightSqlProducer, KyuubiFlightTlsUtils}
import org.apache.kyuubi.service.{AbstractFrontendService, Serverable, Service}
import org.apache.kyuubi.util.JavaUtils

class KyuubiFlightSqlFrontendService(override val serverable: Serverable)
  extends AbstractFrontendService("KyuubiFlightSqlFrontendService") with Logging {

  private var allocator: BufferAllocator = _
  private var producer: KyuubiFlightSqlProducer = _
  private var flightServer: FlightServer = _
  private var configuredPort: Int = _
  private var tlsMaterial: Option[KyuubiFlightTlsUtils.TlsMaterial] = None

  private val started = new AtomicBoolean(false)

  private lazy val host: String = conf.get(FRONTEND_FLIGHT_SQL_BIND_HOST).getOrElse {
    if (conf.get(FRONTEND_CONNECTION_URL_USE_HOSTNAME)) {
      JavaUtils.findLocalInetAddress.getCanonicalHostName
    } else {
      JavaUtils.findLocalInetAddress.getHostAddress
    }
  }

  private def sslEnabled: Boolean = conf.get(FRONTEND_FLIGHT_SQL_SSL_ENABLED)

  private def locationFor(hostName: String, port: Int): Location =
    if (sslEnabled) Location.forGrpcTls(hostName, port)
    else Location.forGrpcInsecure(hostName, port)

  private def configuredLocation: Location = locationFor(host, configuredPort)

  private def currentLocation: Location = {
    val advertisedHost = conf.get(FRONTEND_ADVERTISED_HOST).getOrElse(host)
    if (flightServer != null && started.get()) {
      locationFor(advertisedHost, flightServer.getPort)
    } else {
      locationFor(advertisedHost, configuredPort)
    }
  }

  override def initialize(conf: KyuubiConf): Unit = synchronized {
    this.conf = conf
    configuredPort = this.conf.get(FRONTEND_FLIGHT_SQL_BIND_PORT)
    allocator = new RootAllocator()
    producer = new KyuubiFlightSqlProducer(
      serverable.backendService,
      allocator,
      () => currentLocation,
      this.conf)
    super.initialize(this.conf)
  }

  override def start(): Unit = synchronized {
    if (!started.get()) {
      try {
        val builder = FlightServer
          .builder(allocator, configuredLocation, producer)
          .headerAuthenticator(KyuubiFlightAuthHandler.create(conf))
          .backpressureThreshold(10 * 1024 * 1024)

        if (sslEnabled) {
          val material = KyuubiFlightTlsUtils.resolve(conf)
          KyuubiFlightTlsUtils.validateCertPresent(material)
          tlsMaterial = Some(material)
          builder.useTls(material.certFile, material.keyFile)
        }

        flightServer = builder.build().start()
        started.set(true)
        info(s"Flight SQL frontend service started at $connectionUrl" +
          s" (tls=$sslEnabled)")
      } catch {
        case NonFatal(e) =>
          MetricsSystem.tracing(_.incCount(MetricsConstants.FLIGHT_SQL_CONN_FAIL))
          if (flightServer != null) {
            try flightServer.close()
            catch {
              case NonFatal(closeError) =>
                warn("Failed to close Flight SQL server after startup failure", closeError)
            }
            flightServer = null
          }
          tlsMaterial.foreach(_.cleanup())
          tlsMaterial = None
          throw new KyuubiException("Cannot start Flight SQL frontend service", e)
      }
    }
    super.start()
  }

  override def stop(): Unit = synchronized {
    if (started.getAndSet(false)) {
      if (producer != null) {
        try producer.close()
        catch {
          case NonFatal(e) => warn("Failed to close Flight SQL producer", e)
        }
      }
      if (flightServer != null) {
        try flightServer.close()
        catch {
          case NonFatal(e) => warn("Failed to close Flight SQL server", e)
        }
        flightServer = null
      }
      tlsMaterial.foreach(_.cleanup())
      tlsMaterial = None
      if (allocator != null) {
        try allocator.close()
        catch {
          case NonFatal(e) => warn("Failed to close Flight SQL allocator", e)
        }
        allocator = null
      }
    }
    super.stop()
  }

  override def connectionUrl: String = {
    checkInitialized()
    val advertisedHost = conf.get(FRONTEND_ADVERTISED_HOST).getOrElse(host)
    s"$advertisedHost:${if (flightServer != null) flightServer.getPort else configuredPort}"
  }

  override lazy val discoveryService: Option[Service] = {
    if (ServiceDiscovery.supportServiceDiscovery(conf)) {
      Some(new FlightSqlServiceDiscovery(this))
    } else {
      None
    }
  }
}
