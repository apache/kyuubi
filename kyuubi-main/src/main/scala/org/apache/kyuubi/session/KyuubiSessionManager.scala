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

import scala.util.control.NonFatal

import org.apache.hive.service.rpc.thrift.TProtocolVersion

import org.apache.kyuubi.KyuubiSQLException
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.ha.HighAvailabilityConf._
import org.apache.kyuubi.ha.client.ServiceDiscovery
import org.apache.kyuubi.operation.KyuubiOperationManager

class KyuubiSessionManager private (name: String) extends SessionManager(name) {

  def this() = this(classOf[KyuubiSessionManager].getSimpleName)

  val operationManager = new KyuubiOperationManager()

  private var zkNamespacePrefix: String = _

  override def initialize(conf: KyuubiConf): Unit = {
    zkNamespacePrefix = conf.get(HA_ZK_NAMESPACE)
    ServiceDiscovery.setUpZooKeeperAuth(conf)
    super.initialize(conf)
  }

  override def openSession(
      protocol: TProtocolVersion,
      user: String,
      password: String,
      ipAddress: String,
      conf: Map[String, String]): SessionHandle = {

    val sessionImpl = new KyuubiSessionImpl(
      protocol, user, password, ipAddress, conf, this, this.getConf.clone, zkNamespacePrefix)
    val handle = sessionImpl.handle
    try {
      sessionImpl.open()
      info(s"$user's session with $handle is opened, current opening sessions" +
        s" $getOpenSessionCount")
      setSession(handle, sessionImpl)
      handle
    } catch {
      case e: Throwable =>
        try {
          sessionImpl.close()
        } catch {
          case t: Throwable => warn(s"Error closing session $handle for $user", t)
        }
        throw KyuubiSQLException(s"Error opening session $handle for $user due to ${e.getMessage}",
          e)
    }
  }
}
