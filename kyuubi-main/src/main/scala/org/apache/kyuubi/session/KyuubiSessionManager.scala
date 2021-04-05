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

import org.apache.hive.service.rpc.thrift.TProtocolVersion

import org.apache.kyuubi.KyuubiSQLException
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.ha.client.ServiceDiscovery
import org.apache.kyuubi.metrics.Metrics
import org.apache.kyuubi.metrics.MetricsConstants._
import org.apache.kyuubi.operation.KyuubiOperationManager

class KyuubiSessionManager private (name: String) extends SessionManager(name) {

  def this() = this(classOf[KyuubiSessionManager].getSimpleName)

  val operationManager = new KyuubiOperationManager()

  override def initialize(conf: KyuubiConf): Unit = {
    ServiceDiscovery.setUpZooKeeperAuth(conf)
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
    val handle = sessionImpl.handle
    try {
      sessionImpl.open()
      // TODO session long_task_timer
      setSession(handle, sessionImpl)
      info(s"$username's session with $handle is opened, current opening sessions" +
      s" $getOpenSessionCount")
      handle
    } catch {
      case e: Throwable =>
        Metrics.count(SESSION, T_EVT, EVT_FAIL, T_USER, user)(1)
        try {
          sessionImpl.close()
        } catch {
          case t: Throwable => warn(s"Error closing session $handle for $username", t)
        }
        throw KyuubiSQLException(
          s"Error opening session $handle for $username due to ${e.getMessage}", e)
    }
  }

  override def start(): Unit = synchronized {
    Metrics.registerGauge(SERVICE_THD_POOL_CORE_SIZE,
      T_NAME, s"$name-exec-pool")() { () => execPool.getCorePoolSize }
    Metrics.registerGauge(SERVICE_THD_POOL_TASK,
      T_NAME, s"$name-exec-pool", T_STAT, STAT_ACTIVE)() { () => execPool.getActiveCount }
    super.start()
  }

  override protected def isServer: Boolean = true
}
