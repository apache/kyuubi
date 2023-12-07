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

package org.apache.kyuubi.engine.flink.session

import scala.util.control.NonFatal

import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.util.EnvironmentInformation
import org.apache.flink.table.client.gateway.SqlExecutionException
import org.apache.flink.table.gateway.api.operation.OperationHandle
import org.apache.flink.table.gateway.service.context.SessionContext
import org.apache.flink.table.gateway.service.session.{Session => FSession}

import org.apache.kyuubi.KyuubiSQLException
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.config.KyuubiReservedKeys.KYUUBI_SESSION_HANDLE_KEY
import org.apache.kyuubi.engine.flink.FlinkEngineUtils
import org.apache.kyuubi.engine.flink.udf.KDFRegistry
import org.apache.kyuubi.session.{AbstractSession, SessionHandle, SessionManager, USE_CATALOG, USE_DATABASE}
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.{TGetInfoType, TGetInfoValue, TProtocolVersion}

class FlinkSessionImpl(
    protocol: TProtocolVersion,
    user: String,
    password: String,
    ipAddress: String,
    conf: Map[String, String],
    sessionManager: SessionManager,
    val fSession: FSession)
  extends AbstractSession(protocol, user, password, ipAddress, conf, sessionManager) {

  override val handle: SessionHandle =
    conf.get(KYUUBI_SESSION_HANDLE_KEY).map(SessionHandle.fromUUID)
      .getOrElse(SessionHandle.fromUUID(fSession.getSessionHandle.getIdentifier.toString))

  val sessionContext: SessionContext = {
    FlinkEngineUtils.getSessionContext(fSession)
  }

  KDFRegistry.registerAll(sessionContext)

  private def setModifiableConfig(key: String, value: String): Unit = {
    try {
      sessionContext.set(key, value)
    } catch {
      case e: SqlExecutionException => warn(e.getMessage)
    }
  }

  override def open(): Unit = {
    val executor = fSession.createExecutor(Configuration.fromMap(fSession.getSessionConfig))

    sessionManager.getConf.get(ENGINE_SESSION_FLINK_INITIALIZE_SQL).foreach { sql =>
      try {
        executor.executeStatement(OperationHandle.create, sql)
      } catch {
        case NonFatal(e) =>
          throw KyuubiSQLException(s"execute ${ENGINE_SESSION_FLINK_INITIALIZE_SQL.key}  $sql ", e)
      }
    }

    val (useCatalogAndDatabaseConf, otherConf) = normalizedConf.partition { case (k, _) =>
      Array(USE_CATALOG, USE_DATABASE).contains(k)
    }

    useCatalogAndDatabaseConf.get(USE_CATALOG).foreach { catalog =>
      try {
        executor.executeStatement(OperationHandle.create, s"USE CATALOG $catalog")
      } catch {
        case NonFatal(e) =>
          throw e
      }
    }

    useCatalogAndDatabaseConf.get("use:database").foreach { database =>
      try {
        executor.executeStatement(OperationHandle.create, s"USE $database")
      } catch {
        case NonFatal(e) =>
          if (database != "default") {
            throw e
          }
      }
    }

    otherConf.foreach {
      case (key, value) => setModifiableConfig(key, value)
    }
    super.open()
  }

  override def getInfo(infoType: TGetInfoType): TGetInfoValue = withAcquireRelease() {
    infoType match {
      case TGetInfoType.CLI_SERVER_NAME | TGetInfoType.CLI_DBMS_NAME =>
        TGetInfoValue.stringValue("Apache Flink")
      case TGetInfoType.CLI_DBMS_VER => TGetInfoValue.stringValue(EnvironmentInformation.getVersion)
      case TGetInfoType.CLI_ODBC_KEYWORDS => TGetInfoValue.stringValue("Unimplemented")
      case _ => throw KyuubiSQLException(s"Unrecognized GetInfoType value: $infoType")
    }
  }
}
