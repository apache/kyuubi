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

import org.apache.flink.runtime.util.EnvironmentInformation
import org.apache.flink.table.client.gateway.SqlExecutionException
import org.apache.flink.table.client.gateway.context.SessionContext
import org.apache.flink.table.client.gateway.local.LocalExecutor
import org.apache.hive.service.rpc.thrift.{TGetInfoType, TGetInfoValue, TProtocolVersion}

import org.apache.kyuubi.KyuubiSQLException
import org.apache.kyuubi.engine.flink.FlinkEngineUtils
import org.apache.kyuubi.session.{AbstractSession, SessionManager}

class FlinkSessionImpl(
    protocol: TProtocolVersion,
    user: String,
    password: String,
    ipAddress: String,
    conf: Map[String, String],
    sessionManager: SessionManager,
    val executor: LocalExecutor)
  extends AbstractSession(protocol, user, password, ipAddress, conf, sessionManager) {

  lazy val sessionContext: SessionContext = {
    FlinkEngineUtils.getSessionContext(executor, handle.identifier.toString)
  }

  private def setModifiableConfig(key: String, value: String): Unit = {
    try {
      sessionContext.set(key, value)
    } catch {
      case e: SqlExecutionException => warn(e.getMessage)
    }
  }

  override def open(): Unit = {
    executor.openSession(handle.identifier.toString)
    normalizedConf.foreach {
      case ("use:catalog", catalog) =>
        val tableEnv = sessionContext.getExecutionContext.getTableEnvironment
        try {
          tableEnv.useCatalog(catalog)
        } catch {
          case NonFatal(e) =>
            throw e
        }
      case ("use:database", database) =>
        val tableEnv = sessionContext.getExecutionContext.getTableEnvironment
        try {
          tableEnv.useDatabase(database)
        } catch {
          case NonFatal(e) =>
            if (database != "default") {
              throw e
            }
        }
      case (key, value) => setModifiableConfig(key, value)
    }
    super.open()
  }

  override def getInfo(infoType: TGetInfoType): TGetInfoValue = withAcquireRelease() {
    infoType match {
      case TGetInfoType.CLI_SERVER_NAME | TGetInfoType.CLI_DBMS_NAME =>
        TGetInfoValue.stringValue("Apache Flink")
      case TGetInfoType.CLI_DBMS_VER => TGetInfoValue.stringValue(EnvironmentInformation.getVersion)
      case _ => throw KyuubiSQLException(s"Unrecognized GetInfoType value: $infoType")
    }
  }
}
