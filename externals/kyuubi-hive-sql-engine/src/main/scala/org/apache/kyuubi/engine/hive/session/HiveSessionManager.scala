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

package org.apache.kyuubi.engine.hive.session

import java.io.File
import java.util.concurrent.Future

import scala.collection.JavaConverters._

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hive.service.cli.{SessionHandle => ImportedSessionHandle}
import org.apache.hive.service.cli.session.{HiveSessionImplwithUGI => ImportedHiveSessionImpl, HiveSessionProxy, SessionManager => ImportedHiveSessionManager}
import org.apache.hive.service.rpc.thrift.TProtocolVersion

import org.apache.kyuubi.config.KyuubiConf.ENGINE_SHARE_LEVEL
import org.apache.kyuubi.engine.ShareLevel
import org.apache.kyuubi.engine.hive.HiveSQLEngine
import org.apache.kyuubi.engine.hive.operation.HiveOperationManager
import org.apache.kyuubi.operation.OperationManager
import org.apache.kyuubi.session.{Session, SessionHandle, SessionManager}

class HiveSessionManager(engine: HiveSQLEngine) extends SessionManager("HiveSessionManager") {
  override protected def isServer: Boolean = false

  override val operationManager: OperationManager = new HiveOperationManager()

  private val internalSessionManager = new ImportedHiveSessionManager(null) {

    /**
     * Avoid unnecessary hive initialization
     */
    override def init(hiveConf: HiveConf): Unit = {
      // this.hiveConf = hiveConf
    }

    /**
     * Avoid unnecessary hive initialization
     */
    override def start(): Unit = {}

    /**
     * Avoid unnecessary hive initialization
     */
    override def stop(): Unit = {}

    /**
     * Submit background operations with exec pool of Kyuubi impl
     */
    override def submitBackgroundOperation(r: Runnable): Future[_] = {
      HiveSessionManager.this.submitBackgroundOperation(r)
    }
  }

  override protected def createSession(
      protocol: TProtocolVersion,
      user: String,
      password: String,
      ipAddress: String,
      conf: Map[String, String]): Session = {
    val sessionHandle = SessionHandle()
    val hive = {
      val sessionWithUGI = new ImportedHiveSessionImpl(
        new ImportedSessionHandle(sessionHandle.toTSessionHandle, protocol),
        protocol,
        user,
        password,
        HiveSQLEngine.hiveConf,
        ipAddress,
        null,
        Seq(ipAddress).asJava)
      val proxy = HiveSessionProxy.getProxy(sessionWithUGI, sessionWithUGI.getSessionUgi)
      sessionWithUGI.setProxySession(proxy)
      proxy
    }
    hive.setSessionManager(internalSessionManager)
    hive.setOperationManager(internalSessionManager.getOperationManager)
    operationLogRoot.foreach(dir => hive.setOperationLogSessionDir(new File(dir)))
    new HiveSessionImpl(
      protocol,
      user,
      password,
      ipAddress,
      conf,
      this,
      sessionHandle,
      hive)
  }

  override def closeSession(sessionHandle: SessionHandle): Unit = {
    super.closeSession(sessionHandle)
    if (conf.get(ENGINE_SHARE_LEVEL) == ShareLevel.CONNECTION.toString) {
      info("Session stopped due to shared level is Connection.")
      engine.stop()
    }
  }
}
