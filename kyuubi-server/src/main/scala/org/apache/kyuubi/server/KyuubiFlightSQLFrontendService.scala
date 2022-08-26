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

import java.util.concurrent.{SynchronousQueue, ThreadPoolExecutor, TimeUnit}

import org.apache.arrow.flight.{FlightServer, Location}

import org.apache.kyuubi._
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.server.filght.{ArrowUtils, KyuubiFlightSQLProducer}
import org.apache.kyuubi.service.{AbstractFrontendService, Serverable, Service}
import org.apache.kyuubi.util.NamedThreadFactory

/**
 * A frontend service implement Arrow Flight SQL protocol.
 */
class KyuubiFlightSQLFrontendService(override val serverable: Serverable)
  extends AbstractFrontendService("KyuubiFlightSQLFrontendService") with Logging {

  private val allocator = ArrowUtils.rootAllocator
    .newChildAllocator("kyuubi-flight-sql", 0, Long.MaxValue)
  private var execPool: ThreadPoolExecutor = _
  private var flightServer: FlightServer = _

  private var host: String = _
  private var port: Int = _

  @volatile protected var isStarted = false

  override def initialize(conf: KyuubiConf): Unit = synchronized {
    val host = conf.get(FRONTEND_FLIGHT_SQL_BIND_HOST)
      .getOrElse(Utils.findLocalInetAddress.getCanonicalHostName)
    val port = conf.get(FRONTEND_FLIGHT_SQL_BIND_PORT)

    execPool = new ThreadPoolExecutor(
      conf.get(FRONTEND_FLIGHT_SQL_MIN_WORKER_THREADS),
      conf.get(FRONTEND_FLIGHT_SQL_MAX_WORKER_THREADS),
      conf.get(FRONTEND_FLIGHT_SQL_WORKER_KEEPALIVE_TIME),
      TimeUnit.MILLISECONDS,
      new SynchronousQueue[Runnable](),
      new NamedThreadFactory("flight-sql-exec-pool", false))

    flightServer = FlightServer.builder()
      .location(Location.forGrpcInsecure(host, port))
      .allocator(allocator)
      .producer(new KyuubiFlightSQLProducer)
      .executor(execPool)
      .build()
    super.initialize(conf)
  }

  override def connectionUrl: String = {
    checkInitialized()
    flightServer.getLocation.getUri.toString
  }

  override def start(): Unit = synchronized {
    if (!isStarted) {
      try {
        flightServer.start()
        host = flightServer.getLocation.getUri.getHost
        port = flightServer.getLocation.getUri.getPort
        isStarted = true
        info(s"Flight SQL frontend service has started at $connectionUrl.")
      } catch {
        case rethrow: Exception =>
          throw new KyuubiException("Cannot start Flight SQL frontend service", rethrow)
      }
    }
    super.start()
  }

  override def stop(): Unit = synchronized {
    if (isStarted) {
      flightServer.shutdown()
      isStarted = false
    }
    super.stop()
  }

  override val discoveryService: Option[Service] = None
}
