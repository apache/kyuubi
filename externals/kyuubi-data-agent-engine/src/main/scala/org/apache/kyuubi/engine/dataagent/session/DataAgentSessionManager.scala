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

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.ENGINE_SHARE_LEVEL
import org.apache.kyuubi.config.KyuubiReservedKeys.KYUUBI_SESSION_HANDLE_KEY
import org.apache.kyuubi.engine.ShareLevel
import org.apache.kyuubi.engine.dataagent.DataAgentEngine
import org.apache.kyuubi.engine.dataagent.operation.DataAgentOperationManager
import org.apache.kyuubi.engine.dataagent.provider.DataAgentProvider
import org.apache.kyuubi.operation.OperationManager
import org.apache.kyuubi.session.{Session, SessionHandle, SessionManager}
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.TProtocolVersion

class DataAgentSessionManager(name: String)
  extends SessionManager(name) {

  def this() = this(classOf[DataAgentSessionManager].getSimpleName)

  override protected def isServer: Boolean = false

  lazy val dataAgentProvider: DataAgentProvider = DataAgentProvider.load(conf)

  override lazy val operationManager: OperationManager =
    new DataAgentOperationManager(conf, dataAgentProvider)

  override def initialize(conf: KyuubiConf): Unit = {
    this.conf = conf
    super.initialize(conf)
  }

  override protected def createSession(
      protocol: TProtocolVersion,
      user: String,
      password: String,
      ipAddress: String,
      conf: Map[String, String]): Session = {
    conf.get(KYUUBI_SESSION_HANDLE_KEY).map(SessionHandle.fromUUID)
      .flatMap(getSessionOption).getOrElse {
        new DataAgentSessionImpl(protocol, user, password, ipAddress, conf, this)
      }
  }

  override def stop(): Unit = {
    try {
      dataAgentProvider.stop()
    } finally {
      super.stop()
    }
  }

  override def closeSession(sessionHandle: SessionHandle): Unit = {
    super.closeSession(sessionHandle)
    if (conf.get(ENGINE_SHARE_LEVEL) == ShareLevel.CONNECTION.toString) {
      info("Data Agent engine stopped due to session stopped and shared level is CONNECTION.")
      stopEngine()
    }
  }

  private def stopEngine(): Unit = {
    DataAgentEngine.currentEngine.foreach(_.stop())
  }
}
