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
import java.util.{Locale, Optional}
import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._

import io.airlift.units.Duration
import io.trino.client.ClientSession
import io.trino.client.OkHttpUtil
import okhttp3.OkHttpClient

import org.apache.kyuubi.KyuubiSQLException
import org.apache.kyuubi.Utils.currentUser
import org.apache.kyuubi.config.{KyuubiConf, KyuubiReservedKeys}
import org.apache.kyuubi.config.KyuubiReservedKeys.KYUUBI_SESSION_HANDLE_KEY
import org.apache.kyuubi.engine.trino.{TrinoConf, TrinoContext, TrinoStatement}
import org.apache.kyuubi.engine.trino.event.TrinoSessionEvent
import org.apache.kyuubi.events.EventBus
import org.apache.kyuubi.operation.{Operation, OperationHandle}
import org.apache.kyuubi.session.{AbstractSession, SessionHandle, SessionManager, USE_CATALOG, USE_DATABASE}
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.{TGetInfoType, TGetInfoValue, TProtocolVersion}

class TrinoSessionImpl(
    protocol: TProtocolVersion,
    user: String,
    password: String,
    ipAddress: String,
    conf: Map[String, String],
    sessionManager: SessionManager)
  extends AbstractSession(protocol, user, password, ipAddress, conf, sessionManager) {

  val sessionConf: KyuubiConf = {
    val engineConf = sessionManager.getConf.clone
    conf.foreach { case (k, v) => engineConf.set(k, v) }
    engineConf
  }

  override val handle: SessionHandle =
    conf.get(KYUUBI_SESSION_HANDLE_KEY).map(SessionHandle.fromUUID).getOrElse(SessionHandle())

  private val sessionUser: String = sessionConf
    .getOption(KyuubiReservedKeys.KYUUBI_SESSION_USER_KEY).getOrElse(currentUser)

  var trinoContext: TrinoContext = _
  private var clientSession: ClientSession = _
  private var catalogName: String = _
  private var databaseName: String = _
  private val sessionEvent = TrinoSessionEvent(this)

  override def open(): Unit = {

    val (useCatalogAndDatabaseConf, _) = normalizedConf.partition { case (k, _) =>
      Array(USE_CATALOG, USE_DATABASE).contains(k)
    }

    useCatalogAndDatabaseConf.foreach {
      case (USE_CATALOG, catalog) => catalogName = catalog
      case (USE_DATABASE, database) => databaseName = database
    }
    if (catalogName == null) {
      catalogName = sessionConf.get(KyuubiConf.ENGINE_TRINO_CONNECTION_CATALOG)
        .getOrElse(throw KyuubiSQLException("Trino default catalog can not be null!"))
    }

    clientSession = createClientSession()
    trinoContext = TrinoContext(createHttpClient(), clientSession)

    super.open()
    EventBus.post(sessionEvent)
  }

  private def createClientSession(): ClientSession = {
    val connectionUrl = sessionConf.get(KyuubiConf.ENGINE_TRINO_CONNECTION_URL).getOrElse(
      throw KyuubiSQLException("Trino server url can not be null!"))

    val clientRequestTimeout = sessionConf.get(TrinoConf.CLIENT_REQUEST_TIMEOUT)

    val properties = getTrinoSessionConf(sessionConf).asJava

    ClientSession.builder()
      .server(URI.create(connectionUrl))
      .principal(Optional.of(sessionUser))
      .source("kyuubi")
      .catalog(catalogName)
      .schema(databaseName)
      .timeZone(ZoneId.systemDefault())
      .locale(Locale.getDefault)
      .properties(properties)
      .clientRequestTimeout(new Duration(clientRequestTimeout, TimeUnit.MILLISECONDS))
      .compressionDisabled(true)
      .build()
  }

  private def createHttpClient(): OkHttpClient = {
    val keystorePath = sessionConf.get(KyuubiConf.ENGINE_TRINO_CONNECTION_KEYSTORE_PATH)
    val keystorePassword = sessionConf.get(KyuubiConf.ENGINE_TRINO_CONNECTION_KEYSTORE_PASSWORD)
    val keystoreType = sessionConf.get(KyuubiConf.ENGINE_TRINO_CONNECTION_KEYSTORE_TYPE)
    val truststorePath = sessionConf.get(KyuubiConf.ENGINE_TRINO_CONNECTION_TRUSTSTORE_PATH)
    val truststorePassword = sessionConf.get(KyuubiConf.ENGINE_TRINO_CONNECTION_TRUSTSTORE_PASSWORD)
    val truststoreType = sessionConf.get(KyuubiConf.ENGINE_TRINO_CONNECTION_TRUSTSTORE_TYPE)

    val serverScheme = clientSession.getServer.getScheme

    val builder = new OkHttpClient.Builder()

    OkHttpUtil.setupSsl(
      builder,
      Optional.ofNullable(keystorePath.orNull),
      Optional.ofNullable(keystorePassword.orNull),
      Optional.ofNullable(keystoreType.orNull),
      Optional.ofNullable(truststorePath.orNull),
      Optional.ofNullable(truststorePassword.orNull),
      Optional.ofNullable(truststoreType.orNull),
      true)

    sessionConf.get(KyuubiConf.ENGINE_TRINO_CONNECTION_PASSWORD).foreach { password =>
      require(
        serverScheme.equalsIgnoreCase("https"),
        "Trino engine using username/password requires HTTPS to be enabled")
      val user: String = sessionConf
        .get(KyuubiConf.ENGINE_TRINO_CONNECTION_USER).getOrElse(sessionUser)
      builder.addInterceptor(OkHttpUtil.basicAuth(user, password))
    }

    builder.build()
  }

  override protected def runOperation(operation: Operation): OperationHandle = {
    sessionEvent.totalOperations += 1
    super.runOperation(operation)
  }

  override def getInfo(infoType: TGetInfoType): TGetInfoValue = withAcquireRelease() {
    infoType match {
      case TGetInfoType.CLI_SERVER_NAME | TGetInfoType.CLI_DBMS_NAME =>
        TGetInfoValue.stringValue("Trino")
      case TGetInfoType.CLI_DBMS_VER => TGetInfoValue.stringValue(getTrinoServerVersion)
      case TGetInfoType.CLI_ODBC_KEYWORDS => TGetInfoValue.stringValue("Unimplemented")
      case TGetInfoType.CLI_MAX_COLUMN_NAME_LEN |
          TGetInfoType.CLI_MAX_SCHEMA_NAME_LEN |
          TGetInfoType.CLI_MAX_TABLE_NAME_LEN => TGetInfoValue.lenValue(0)
      case _ => throw KyuubiSQLException(s"Unrecognized GetInfoType value: $infoType")
    }
  }

  private def getTrinoServerVersion: String = {
    val trinoStatement =
      TrinoStatement(trinoContext, sessionManager.getConf, "SELECT version()")
    val resultSet = trinoStatement.execute()

    assert(resultSet.hasNext)
    resultSet.next().head.toString
  }

  private def getTrinoSessionConf(sessionConf: KyuubiConf): Map[String, String] = {
    val trinoSessionConf = sessionConf.getAll.filterKeys(_.startsWith("trino."))
      .map { case (k, v) => (k.stripPrefix("trino."), v) }
    trinoSessionConf.toMap
  }

  override def close(): Unit = {
    sessionEvent.endTime = System.currentTimeMillis()
    EventBus.post(sessionEvent)
    super.close()
  }
}
