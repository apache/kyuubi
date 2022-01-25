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

package org.apache.kyuubi.engine.trino.session

import java.net.URI
import java.time.ZoneId
import java.util.Locale
import java.util.Optional
import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._

import io.airlift.units.Duration
import io.trino.client.ClientSelectedRole
import io.trino.client.ClientSession
import okhttp3.OkHttpClient
import org.apache.hive.service.rpc.thrift.TProtocolVersion

import org.apache.kyuubi.KyuubiSQLException
import org.apache.kyuubi.Utils
import org.apache.kyuubi.Utils.currentUser
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.ENGINE_OPERATION_LOG_DIR_ROOT
import org.apache.kyuubi.config.KyuubiConf.ENGINE_SHARE_LEVEL
import org.apache.kyuubi.engine.ShareLevel
import org.apache.kyuubi.engine.trino.TrinoConf
import org.apache.kyuubi.engine.trino.TrinoContext
import org.apache.kyuubi.engine.trino.TrinoSqlEngine
import org.apache.kyuubi.engine.trino.operation.TrinoOperationManager
import org.apache.kyuubi.session.SessionHandle
import org.apache.kyuubi.session.SessionManager

class TrinoSessionManager
  extends SessionManager("TrinoSessionManager") {

  val operationManager = new TrinoOperationManager()

  override def initialize(conf: KyuubiConf): Unit = {
    val absPath = Utils.getAbsolutePathFromWork(conf.get(ENGINE_OPERATION_LOG_DIR_ROOT))
    _operationLogRoot = Some(absPath.toAbsolutePath.toString)
    super.initialize(conf)
  }

  override def openSession(
      protocol: TProtocolVersion,
      user: String,
      password: String,
      ipAddress: String,
      conf: Map[String, String]): SessionHandle = {
    info(s"Opening session for $user@$ipAddress")
    val trinoContext = createTrinoContext()
    val sessionImpl =
      new TrinoSessionImpl(protocol, user, password, ipAddress, conf, this, trinoContext)

    try {
      val handle = sessionImpl.handle
      sessionImpl.open()
      setSession(handle, sessionImpl)
      info(s"$user's trino session with $handle is opened, current opening sessions" +
        s" $getOpenSessionCount")
      handle
    } catch {
      case e: Exception =>
        sessionImpl.close()
        throw KyuubiSQLException(e)
    }
  }

  def createTrinoContext(): TrinoContext = {
    val connectionUrl = conf.get(KyuubiConf.ENGINE_TRINO_CONNECTION_URL).getOrElse(
      throw KyuubiSQLException("Trino server url can not be null!"))
    val catalog = conf.get(KyuubiConf.ENGINE_TRINO_CONNECTION_CATALOG).getOrElse(
      throw KyuubiSQLException("Trino default catalog can not be null!"))
    val user = conf.getOption("kyuubi.trino.user").getOrElse(currentUser)
    val clientRequestTimeout = conf.get(TrinoConf.CLIENT_REQUEST_TIMEOUT)

    val session = new ClientSession(
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

    val httpClient = new OkHttpClient.Builder().build()

    TrinoContext(httpClient, session)
  }

  override def closeSession(sessionHandle: SessionHandle): Unit = {
    super.closeSession(sessionHandle)
    if (conf.get(ENGINE_SHARE_LEVEL) == ShareLevel.CONNECTION.toString) {
      info("Session stopped due to shared level is Connection.")
      stopSession()
    }
  }

  private def stopSession(): Unit = {
    TrinoSqlEngine.currentEngine.foreach(_.stop())
  }

  override protected def isServer: Boolean = false
}
