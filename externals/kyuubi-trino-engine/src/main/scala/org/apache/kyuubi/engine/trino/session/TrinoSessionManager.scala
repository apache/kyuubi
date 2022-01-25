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

import org.apache.hive.service.rpc.thrift.TProtocolVersion

import org.apache.kyuubi.KyuubiSQLException
import org.apache.kyuubi.Utils
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.ENGINE_OPERATION_LOG_DIR_ROOT
import org.apache.kyuubi.config.KyuubiConf.ENGINE_SHARE_LEVEL
import org.apache.kyuubi.engine.ShareLevel
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
    val sessionImpl =
      new TrinoSessionImpl(protocol, user, password, ipAddress, conf, this)

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
