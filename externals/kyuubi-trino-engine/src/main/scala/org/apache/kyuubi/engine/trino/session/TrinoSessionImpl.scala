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
import java.util.Collections
import java.util.Locale
import java.util.Optional
import java.util.concurrent.TimeUnit

import io.airlift.units.Duration
import io.trino.client.ClientSession
import okhttp3.OkHttpClient
import org.apache.hive.service.rpc.thrift.TProtocolVersion

import org.apache.kyuubi.KyuubiSQLException
import org.apache.kyuubi.Utils.currentUser
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.engine.trino.TrinoConf
import org.apache.kyuubi.engine.trino.TrinoContext
import org.apache.kyuubi.session.AbstractSession
import org.apache.kyuubi.session.SessionHandle
import org.apache.kyuubi.session.SessionManager

class TrinoSessionImpl(
    protocol: TProtocolVersion,
    user: String,
    password: String,
    ipAddress: String,
    conf: Map[String, String],
    sessionManager: SessionManager)
  extends AbstractSession(protocol, user, password, ipAddress, conf, sessionManager) {

  var trinoContext: TrinoContext = _
  private var clientSession: ClientSession = _

  override val handle: SessionHandle = SessionHandle(protocol)

  override def open(): Unit = {
    normalizedConf.foreach {
      case ("use:database", database) => clientSession = createClientSession(database)
      case _ => // do nothing
    }

    val httpClient = new OkHttpClient.Builder().build()

    if (clientSession == null) {
      clientSession = createClientSession()
    }
    trinoContext = TrinoContext(httpClient, clientSession)

    super.open()
  }

  private def createClientSession(schema: String = null): ClientSession = {
    val sessionConf = sessionManager.getConf
    val connectionUrl = sessionConf.get(KyuubiConf.ENGINE_TRINO_CONNECTION_URL).getOrElse(
      throw KyuubiSQLException("Trino server url can not be null!"))
    val catalog = sessionConf.get(KyuubiConf.ENGINE_TRINO_CONNECTION_CATALOG).getOrElse(
      throw KyuubiSQLException("Trino default catalog can not be null!"))
    val user = sessionConf.getOption("kyuubi.trino.user").getOrElse(currentUser)
    val clientRequestTimeout = sessionConf.get(TrinoConf.CLIENT_REQUEST_TIMEOUT)

    new ClientSession(
      URI.create(connectionUrl),
      user,
      Optional.empty(),
      "kyuubi",
      Optional.empty(),
      Collections.emptySet(),
      null,
      catalog,
      schema,
      null,
      ZoneId.systemDefault(),
      Locale.getDefault,
      Collections.emptyMap(),
      Collections.emptyMap(),
      Collections.emptyMap(),
      Collections.emptyMap(),
      Collections.emptyMap(),
      null,
      new Duration(clientRequestTimeout, TimeUnit.MILLISECONDS),
      true)
  }
}
