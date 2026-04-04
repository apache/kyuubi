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
package org.apache.kyuubi.engine.dataagent.session

import org.apache.kyuubi.{KYUUBI_VERSION, KyuubiSQLException}
import org.apache.kyuubi.config.KyuubiReservedKeys.KYUUBI_SESSION_HANDLE_KEY
import org.apache.kyuubi.session.{AbstractSession, SessionHandle, SessionManager}
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.{TGetInfoType, TGetInfoValue, TProtocolVersion}

class DataAgentSessionImpl(
    protocol: TProtocolVersion,
    user: String,
    password: String,
    ipAddress: String,
    conf: Map[String, String],
    sessionManager: SessionManager)
  extends AbstractSession(protocol, user, password, ipAddress, conf, sessionManager) {

  override val handle: SessionHandle =
    conf.get(KYUUBI_SESSION_HANDLE_KEY).map(SessionHandle.fromUUID).getOrElse(SessionHandle())

  private val dataAgentProvider =
    sessionManager.asInstanceOf[DataAgentSessionManager].dataAgentProvider

  override def open(): Unit = {
    info(s"Starting to open data agent session.")
    dataAgentProvider.open(handle.identifier.toString, user)
    try {
      super.open()
    } catch {
      case e: Throwable =>
        try {
          dataAgentProvider.close(handle.identifier.toString)
        } catch {
          case ex: Throwable =>
            error("Failed to cleanup data agent provider on session open failure", ex)
        }
        throw e
    }
    info(s"The data agent session is started.")
  }

  override def getInfo(infoType: TGetInfoType): TGetInfoValue = withAcquireRelease() {
    infoType match {
      case TGetInfoType.CLI_SERVER_NAME | TGetInfoType.CLI_DBMS_NAME =>
        TGetInfoValue.stringValue("Kyuubi Data Agent Engine")
      case TGetInfoType.CLI_DBMS_VER =>
        TGetInfoValue.stringValue(KYUUBI_VERSION)
      case TGetInfoType.CLI_ODBC_KEYWORDS => TGetInfoValue.stringValue("Unimplemented")
      case TGetInfoType.CLI_MAX_COLUMN_NAME_LEN =>
        TGetInfoValue.lenValue(128)
      case TGetInfoType.CLI_MAX_SCHEMA_NAME_LEN =>
        TGetInfoValue.lenValue(128)
      case TGetInfoType.CLI_MAX_TABLE_NAME_LEN =>
        TGetInfoValue.lenValue(128)
      case _ => throw KyuubiSQLException(s"Unrecognized GetInfoType value: $infoType")
    }
  }

  override def close(): Unit = {
    try {
      dataAgentProvider.close(handle.identifier.toString)
    } finally {
      super.close()
    }
  }

}
