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
import java.util.{List => JList}
import java.util.concurrent.Future

import scala.collection.JavaConverters._
import scala.language.reflectiveCalls

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.hive.service.cli.{SessionHandle => ImportedSessionHandle}
import org.apache.hive.service.cli.session.{HiveSessionImpl => ImportedHiveSessionImpl}
import org.apache.hive.service.cli.session.{HiveSessionImplwithUGI => ImportedHiveSessionImplwithUGI}
import org.apache.hive.service.cli.session.{SessionManager => ImportedHiveSessionManager}
import org.apache.hive.service.cli.session.HiveSessionProxy
import org.apache.hive.service.rpc.thrift.{TProtocolVersion => HiveTProtocolVersion}

import org.apache.kyuubi.config.KyuubiConf.ENGINE_SHARE_LEVEL
import org.apache.kyuubi.config.KyuubiReservedKeys.KYUUBI_SESSION_HANDLE_KEY
import org.apache.kyuubi.engine.ShareLevel
import org.apache.kyuubi.engine.hive.HiveSQLEngine
import org.apache.kyuubi.engine.hive.operation.HiveOperationManager
import org.apache.kyuubi.engine.hive.util.HiveRpcUtils
import org.apache.kyuubi.operation.OperationManager
import org.apache.kyuubi.session.{Session, SessionHandle, SessionManager}
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.TProtocolVersion
import org.apache.kyuubi.util.reflect.DynConstructors

class HiveSessionManager(engine: HiveSQLEngine) extends SessionManager("HiveSessionManager") {
  override protected def isServer: Boolean = false

  override val operationManager: OperationManager = new HiveOperationManager()

  private val internalSessionManager = new ImportedHiveSessionManager(null) {

    var doAsEnabled: Boolean = _

    /**
     * Avoid unnecessary hive initialization
     */
    override def init(hiveConf: HiveConf): Unit = {
      // this.hiveConf = hiveConf
      this.doAsEnabled = hiveConf.getBoolVar(ConfVars.HIVE_SERVER2_ENABLE_DOAS)
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
    conf.get(KYUUBI_SESSION_HANDLE_KEY).map(SessionHandle.fromUUID).flatMap(
      getSessionOption).getOrElse {
      val hiveProtocol = HiveRpcUtils.asHive(protocol)
      val sessionHandle =
        conf.get(KYUUBI_SESSION_HANDLE_KEY).map(SessionHandle.fromUUID).getOrElse(SessionHandle())
      val hiveTSessionHandle = HiveRpcUtils.asHive(sessionHandle.toTSessionHandle)
      val hive = if (internalSessionManager.doAsEnabled) {
        val sessionWithUGI = DynConstructors.builder()
          .impl( // for Hive 3.1
            classOf[ImportedHiveSessionImplwithUGI],
            classOf[ImportedSessionHandle],
            classOf[HiveTProtocolVersion],
            classOf[String],
            classOf[String],
            classOf[HiveConf],
            classOf[String],
            classOf[String],
            classOf[JList[String]])
          .impl( // for Hive 2.3
            classOf[ImportedHiveSessionImplwithUGI],
            classOf[ImportedSessionHandle],
            classOf[HiveTProtocolVersion],
            classOf[String],
            classOf[String],
            classOf[HiveConf],
            classOf[String],
            classOf[String])
          .build[ImportedHiveSessionImplwithUGI]()
          .newInstance(
            new ImportedSessionHandle(hiveTSessionHandle, hiveProtocol),
            hiveProtocol,
            user,
            password,
            HiveSQLEngine.hiveConf,
            ipAddress,
            null,
            Seq(ipAddress).asJava)
        val proxy = HiveSessionProxy.getProxy(sessionWithUGI, sessionWithUGI.getSessionUgi)
        sessionWithUGI.setProxySession(proxy)
        proxy
      } else {
        DynConstructors.builder()
          .impl( // for Hive 3.1
            classOf[ImportedHiveSessionImpl],
            classOf[ImportedSessionHandle],
            classOf[HiveTProtocolVersion],
            classOf[String],
            classOf[String],
            classOf[HiveConf],
            classOf[String],
            classOf[JList[String]])
          .impl( // for Hive 2.3
            classOf[ImportedHiveSessionImpl],
            classOf[ImportedSessionHandle],
            classOf[HiveTProtocolVersion],
            classOf[String],
            classOf[String],
            classOf[HiveConf],
            classOf[String])
          .build[ImportedHiveSessionImpl]()
          .newInstance(
            new ImportedSessionHandle(hiveTSessionHandle, hiveProtocol),
            hiveProtocol,
            user,
            password,
            HiveSQLEngine.hiveConf,
            ipAddress,
            Seq(ipAddress).asJava)
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
  }

  override def closeSession(sessionHandle: SessionHandle): Unit = {
    super.closeSession(sessionHandle)
    if (conf.get(ENGINE_SHARE_LEVEL) == ShareLevel.CONNECTION.toString) {
      info("Hive engine stopped due to session stopped and shared level is CONNECTION.")
      engine.stop()
    }
  }
}
