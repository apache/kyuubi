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
import java.util.{Collections, Locale, Optional}
import java.util.concurrent.TimeUnit

import io.airlift.units.Duration
import io.trino.client.ClientSession
import okhttp3.OkHttpClient
import org.apache.hive.service.rpc.thrift.{TGetInfoType, TGetInfoValue, TProtocolVersion}

import org.apache.kyuubi.KyuubiSQLException
import org.apache.kyuubi.Utils.currentUser
import org.apache.kyuubi.config.{KyuubiConf, KyuubiReservedKeys}
import org.apache.kyuubi.engine.trino.{TrinoConf, TrinoContext, TrinoStatement}
import org.apache.kyuubi.engine.trino.event.TrinoSessionEvent
import org.apache.kyuubi.events.EventBus
import org.apache.kyuubi.operation.{Operation, OperationHandle}
import org.apache.kyuubi.session.{AbstractSession, SessionManager}

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
  private var catalogName: String = null
  private var databaseName: String = null

  private val sessionEvent = TrinoSessionEvent(this)

  override def open(): Unit = {
    normalizedConf.foreach {
      case ("use:catalog", catalog) => catalogName = catalog
      case ("use:database", database) => databaseName = database
      case _ => // do nothing
    }

    val httpClient = new OkHttpClient.Builder().build()

    clientSession = createClientSession()
    trinoContext = TrinoContext(httpClient, clientSession)

    super.open()
    EventBus.post(sessionEvent)
  }

  private def createClientSession(): ClientSession = {
    val sessionConf = sessionManager.getConf
    val connectionUrl = sessionConf.get(KyuubiConf.ENGINE_TRINO_CONNECTION_URL).getOrElse(
      throw KyuubiSQLException("Trino server url can not be null!"))

    if (catalogName == null) {
      catalogName = sessionConf.get(
        KyuubiConf.ENGINE_TRINO_CONNECTION_CATALOG).getOrElse(
        throw KyuubiSQLException("Trino default catalog can not be null!"))
    }

    val user = sessionConf
      .getOption(KyuubiReservedKeys.KYUUBI_SESSION_USER_KEY).getOrElse(currentUser)
    val clientRequestTimeout = sessionConf.get(TrinoConf.CLIENT_REQUEST_TIMEOUT)

    new ClientSession(
      URI.create(connectionUrl),
      user,
      Optional.empty(),
      "kyuubi",
      Optional.empty(),
      Collections.emptySet(),
      null,
      catalogName,
      databaseName,
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

  override def close(): Unit = {
    sessionEvent.endTime = System.currentTimeMillis()
    EventBus.post(sessionEvent)
    super.close()
  }
}
