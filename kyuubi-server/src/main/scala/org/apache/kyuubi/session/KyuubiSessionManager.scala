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

package org.apache.kyuubi.session

import com.codahale.metrics.MetricRegistry
import org.apache.hive.service.rpc.thrift.TProtocolVersion

import org.apache.kyuubi.{KyuubiSQLException, Utils}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.SERVER_OPERATION_LOG_DIR_ROOT
import org.apache.kyuubi.credentials.HadoopCredentialsManager
import org.apache.kyuubi.metrics.MetricsConstants._
import org.apache.kyuubi.metrics.MetricsSystem
import org.apache.kyuubi.operation.KyuubiOperationManager

class KyuubiSessionManager private (name: String) extends SessionManager(name) {

  def this() = this(classOf[KyuubiSessionManager].getSimpleName)

  override def LOG_ROOT: String = if (Utils.isTesting) {
    "target/server_operation_logs"
  } else {
    conf.get(SERVER_OPERATION_LOG_DIR_ROOT).getOrElse("server_operation_logs")
  }

  val operationManager = new KyuubiOperationManager()
  val credentialsManager = new HadoopCredentialsManager()

  override def initialize(conf: KyuubiConf): Unit = {
    addService(credentialsManager)
    super.initialize(conf)
  }

  override def openSession(
      protocol: TProtocolVersion,
      user: String,
      password: String,
      ipAddress: String,
      conf: Map[String, String]): SessionHandle = {

    val username = Option(user).filter(_.nonEmpty).getOrElse("anonymous")

    val sessionImpl = new KyuubiSessionImpl(
      protocol,
      username,
      password,
      ipAddress,
      conf,
      this,
      this.getConf.getUserDefaults(user))
    try {
      sessionImpl.open()
      val handle = sessionImpl.handle
      setSession(handle, sessionImpl)
      info(s"$username's session with $handle is opened, current opening sessions" +
      s" $getOpenSessionCount")
      handle
    } catch {
      case e: Throwable =>
        MetricsSystem.tracing { ms =>
          ms.incCount(CONN_FAIL)
          ms.incCount(MetricRegistry.name(CONN_FAIL, user))
        }
        try {
          sessionImpl.close()
        } catch {
          case t: Throwable =>
            warn(s"Error closing session for $username client ip: $ipAddress", t)
        }
        throw KyuubiSQLException(
          s"Error opening session for $username client ip $ipAddress, due to ${e.getMessage}", e)
    }
  }

  override def start(): Unit = synchronized {
    MetricsSystem.tracing { ms =>
      ms.registerGauge(CONN_OPEN, getOpenSessionCount, 0)
      ms.registerGauge(EXEC_POOL_ALIVE, getExecPoolSize, 0)
      ms.registerGauge(EXEC_POOL_ACTIVE, getActiveCount, 0)
    }
    super.start()
  }

  override protected def isServer: Boolean = true
}
