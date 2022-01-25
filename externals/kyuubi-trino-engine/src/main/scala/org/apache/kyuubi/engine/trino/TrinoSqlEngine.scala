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

package org.apache.kyuubi.engine.trino

import java.net.URI
import java.time.ZoneId
import java.util.Locale
import java.util.Optional
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._

import io.airlift.units.Duration
import io.trino.client.ClientSelectedRole
import io.trino.client.ClientSession
import okhttp3.OkHttpClient

import org.apache.kyuubi.KyuubiSQLException
import org.apache.kyuubi.Logging
import org.apache.kyuubi.Utils.TRINO_ENGINE_SHUTDOWN_PRIORITY
import org.apache.kyuubi.Utils.addShutdownHook
import org.apache.kyuubi.Utils.currentUser
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.engine.trino.TrinoSqlEngine.countDownLatch
import org.apache.kyuubi.engine.trino.TrinoSqlEngine.currentEngine
import org.apache.kyuubi.ha.HighAvailabilityConf.HA_ZK_CONN_RETRY_POLICY
import org.apache.kyuubi.ha.client.RetryPolicies
import org.apache.kyuubi.service.Serverable
import org.apache.kyuubi.util.SignalRegister

case class TrinoSqlEngine(trino: TrinoContext)
  extends Serverable("TrinoSQLEngine") {

  override val backendService = new TrinoBackendService(trino)

  override val frontendServices = Seq(new TrinoTBinaryFrontendService(this))

  override def start(): Unit = {
    super.start()
    // Start engine self-terminating checker after all services are ready and it can be reached by
    // all servers in engine spaces.
    backendService.sessionManager.startTerminatingChecker(() => {
      assert(currentEngine.isDefined)
      currentEngine.get.stop()
    })
  }

  override protected def stopServer(): Unit = {
    countDownLatch.countDown()
  }
}

object TrinoSqlEngine extends Logging {
  private val countDownLatch = new CountDownLatch(1)

  val kyuubiConf: KyuubiConf = KyuubiConf()

  var currentEngine: Option[TrinoSqlEngine] = None

  def createTrinoContext(): TrinoContext = {
    val session = createClientSession()
    val httpClient = new OkHttpClient.Builder().build()

    kyuubiConf.setIfMissing(KyuubiConf.FRONTEND_THRIFT_BINARY_BIND_PORT, 0)
    kyuubiConf.setIfMissing(HA_ZK_CONN_RETRY_POLICY, RetryPolicies.N_TIME.toString)

    if (logger.isDebugEnabled) {
      kyuubiConf.getAll.foreach { case (k, v) =>
        debug(s"KyuubiConf: $k = $v")
      }
    }

    TrinoContext(httpClient, session)
  }

  def startEngine(trino: TrinoContext): Unit = {
    currentEngine = Some(new TrinoSqlEngine(trino))
    currentEngine.foreach { engine =>
      engine.initialize(kyuubiConf)
      engine.start()
      addShutdownHook(() => engine.stop(), TRINO_ENGINE_SHUTDOWN_PRIORITY + 1)
    }
  }

  def main(args: Array[String]): Unit = {
    SignalRegister.registerLogger(logger)

    try {
      val trino = createTrinoContext()
      startEngine(trino)
      // blocking main thread
      countDownLatch.await()
    } catch {
      case t: Throwable => error(s"Failed to instantiate Trino: ${t.getMessage}", t)
    }
  }

  private def createClientSession(): ClientSession = {
    val connectionUrl = kyuubiConf.get(KyuubiConf.ENGINE_TRINO_CONNECTION_URL).getOrElse(
      throw KyuubiSQLException("Trino server url can not be null!"))
    val catalog = kyuubiConf.get(KyuubiConf.ENGINE_TRINO_CONNECTION_CATALOG).getOrElse(
      throw KyuubiSQLException("Trino default catalog can not be null!"))
    val user = kyuubiConf.getOption("kyuubi.trino.user").getOrElse(currentUser)
    val clientRequestTimeout = kyuubiConf.get(TrinoConf.CLIENT_REQUEST_TIMEOUT)

    new ClientSession(
      URI.create(connectionUrl),
      user,
      Optional.empty(),
      "kyuubi",
      Optional.empty(),
      Set[String]().asJava,
      null,
      catalog,
      null,
      null,
      ZoneId.systemDefault(),
      Locale.getDefault,
      Map[String, String]().asJava,
      Map[String, String]().asJava,
      Map[String, String]().asJava,
      Map[String, ClientSelectedRole]().asJava,
      Map[String, String]().asJava,
      null,
      new Duration(clientRequestTimeout, TimeUnit.MILLISECONDS),
      true)
  }
}
