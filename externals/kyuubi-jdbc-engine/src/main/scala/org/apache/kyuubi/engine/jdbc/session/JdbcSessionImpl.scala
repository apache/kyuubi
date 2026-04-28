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
package org.apache.kyuubi.engine.jdbc.session

import java.sql.{Connection, DatabaseMetaData}

import scala.util.{Failure, Success, Try}

import org.apache.kyuubi.KyuubiSQLException
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.config.KyuubiReservedKeys.KYUUBI_SESSION_HANDLE_KEY
import org.apache.kyuubi.engine.jdbc.connection.ConnectionProvider
import org.apache.kyuubi.engine.jdbc.dialect.JdbcDialects
import org.apache.kyuubi.engine.jdbc.util.KyuubiJdbcUtils
import org.apache.kyuubi.session.{AbstractSession, SessionHandle, SessionManager, USE_DATABASE}
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.{TGetInfoType, TGetInfoValue, TProtocolVersion}

class JdbcSessionImpl(
    protocol: TProtocolVersion,
    user: String,
    password: String,
    ipAddress: String,
    conf: Map[String, String],
    sessionManager: SessionManager)
  extends AbstractSession(protocol, user, password, ipAddress, conf, sessionManager) {

  override val handle: SessionHandle =
    conf.get(KYUUBI_SESSION_HANDLE_KEY).map(SessionHandle.fromUUID).getOrElse(SessionHandle())

  private[jdbc] var sessionConnection: Connection = _

  private var databaseMetaData: DatabaseMetaData = _

  /**
   * The database the client requested via `USE_DATABASE` at session open. Used by
   * `JdbcOperationManager` as the metadata schema filter. `None` when no `USE_DATABASE`
   * was requested, or when the request was the literal `"default"`. The Hive JDBC
   * driver sends `"default"` as a protocol stub when the user did not specify a
   * database in the URL, so it cannot be distinguished from a genuine request and
   * is treated as "no scope".
   */
  private[jdbc] var effectiveDatabase: Option[String] = None

  val sessionConf: KyuubiConf = normalizeConf

  private def normalizeConf: KyuubiConf = {
    val kyuubiConf = sessionManager.getConf.clone
    if (kyuubiConf.get(ENGINE_JDBC_CONNECTION_PROPAGATECREDENTIAL)) {
      kyuubiConf.set(ENGINE_JDBC_CONNECTION_USER, user)
      kyuubiConf.set(ENGINE_JDBC_CONNECTION_PASSWORD, password)
    }
    conf.foreach { case (k, v) => kyuubiConf.set(k, v) }
    kyuubiConf
  }

  override def open(): Unit = {
    info(s"Starting to open jdbc session.")
    if (sessionConnection == null) {
      sessionConnection = ConnectionProvider.create(sessionConf)
      databaseMetaData = sessionConnection.getMetaData
    }
    KyuubiJdbcUtils.initializeJdbcSession(
      sessionConf,
      sessionConnection,
      sessionConf.get(ENGINE_JDBC_SESSION_INITIALIZE_SQL))
    // The Hive JDBC driver sends `"default"` as USE_DATABASE when the user did not
    // specify a database in the URL: a protocol stub indistinguishable from a real
    // request. Filter it here so we don't push a non-existent / unintended schema
    // filter into metadata operations on backends without a `default` database.
    conf.get(USE_DATABASE).filter(_ != "default").foreach { database =>
      JdbcDialects.get(sessionConf).setSchema(sessionConnection, database)
      effectiveDatabase = Some(database)
    }
    super.open()
    info(s"The jdbc session is started.")
  }

  override def getInfo(infoType: TGetInfoType): TGetInfoValue = withAcquireRelease() {
    assert(databaseMetaData != null, "JDBC session has not been initialized")
    infoType match {
      case TGetInfoType.CLI_SERVER_NAME | TGetInfoType.CLI_DBMS_NAME =>
        TGetInfoValue.stringValue(databaseMetaData.getDatabaseProductName)
      case TGetInfoType.CLI_DBMS_VER =>
        TGetInfoValue.stringValue(databaseMetaData.getDatabaseProductVersion)
      case TGetInfoType.CLI_ODBC_KEYWORDS => TGetInfoValue.stringValue("Unimplemented")
      case TGetInfoType.CLI_MAX_COLUMN_NAME_LEN =>
        TGetInfoValue.lenValue(databaseMetaData.getMaxColumnNameLength)
      case TGetInfoType.CLI_MAX_SCHEMA_NAME_LEN =>
        TGetInfoValue.lenValue(databaseMetaData.getMaxSchemaNameLength)
      case TGetInfoType.CLI_MAX_TABLE_NAME_LEN =>
        TGetInfoValue.lenValue(databaseMetaData.getMaxTableNameLength)
      case _ => throw KyuubiSQLException(s"Unrecognized GetInfoType value: $infoType")
    }
  }

  override def close(): Unit = {
    Try {
      if (sessionConnection != null) {
        sessionConnection.close()
      }
    } match {
      case Success(_) =>
        info(s"Closed session connection.")
      case Failure(exception) =>
        warn("Failed to close session connection, ignored it.", exception)
    }
    super.close()
  }

}
