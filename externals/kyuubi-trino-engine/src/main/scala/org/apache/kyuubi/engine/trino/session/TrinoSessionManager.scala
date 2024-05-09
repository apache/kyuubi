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

import org.apache.kyuubi.config.KyuubiConf.ENGINE_SHARE_LEVEL
import org.apache.kyuubi.config.KyuubiReservedKeys.KYUUBI_SESSION_HANDLE_KEY
import org.apache.kyuubi.engine.ShareLevel
import org.apache.kyuubi.engine.trino.TrinoSqlEngine
import org.apache.kyuubi.engine.trino.operation.TrinoOperationManager
import org.apache.kyuubi.session.{Session, SessionHandle, SessionManager}
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.TProtocolVersion

class TrinoSessionManager
  extends SessionManager("TrinoSessionManager") {

  val operationManager = new TrinoOperationManager()

  override protected def createSession(
      protocol: TProtocolVersion,
      user: String,
      password: String,
      ipAddress: String,
      conf: Map[String, String]): Session = {
    conf.get(KYUUBI_SESSION_HANDLE_KEY).map(SessionHandle.fromUUID).flatMap(
      getSessionOption).getOrElse {
      new TrinoSessionImpl(protocol, user, password, ipAddress, conf, this)
    }
  }

  override def closeSession(sessionHandle: SessionHandle): Unit = {
    super.closeSession(sessionHandle)
    if (conf.get(ENGINE_SHARE_LEVEL) == ShareLevel.CONNECTION.toString) {
      info("Trino engine stopped due to session stopped and shared level is CONNECTION.")
      stopEngine()
    }
  }

  private def stopEngine(): Unit = {
    TrinoSqlEngine.currentEngine.foreach(_.stop())
  }

  override protected def isServer: Boolean = false
}
