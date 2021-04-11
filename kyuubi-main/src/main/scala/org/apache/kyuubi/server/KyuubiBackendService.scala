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

package org.apache.kyuubi.server

import org.apache.hive.service.rpc.thrift.TProtocolVersion

import org.apache.kyuubi.KyuubiSQLException
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.service.AbstractBackendService
import org.apache.kyuubi.session.{KyuubiSessionManager, SessionHandle, SessionManager}

class KyuubiBackendService(name: String) extends AbstractBackendService(name) {

  def this() = this(classOf[KyuubiBackendService].getSimpleName)

  override val sessionManager: SessionManager = new KyuubiSessionManager()

  override def initialize(conf: KyuubiConf): Unit = {
    super.initialize(conf)
  }

  @throws[KyuubiSQLException]
  override def openSession(
      protocol: TProtocolVersion,
      user: String,
      password: String,
      ipAddr: String,
      configs: Map[String, String]): SessionHandle = {
    intercept(configs)
    super.openSession(protocol, user, password, ipAddr, optimizeConf(configs))
  }

  @throws[KyuubiSQLException]
  def intercept(conf: Map[String, String]): Unit = {
    val restrictList = getConf.get(KyuubiConf.SESSION_CONF_RESTRICT_LIST).toList
        .flatMap(_.split(","))
    restrictList.foreach(key => {
      if (conf.contains(key)) {
        throw KyuubiSQLException(s"Your session was restricted. Illegal key " +
            s"is $key")
      }
    })
  }

  def optimizeConf(conf: Map[String, String]): Map[String, String] = {
    val ignoreList = getConf.get(KyuubiConf.SESSION_CONF_IGNORE_LIST).toList
        .flatMap(_.split(","))
    var newConf = conf
    ignoreList.foreach(key => newConf = conf - key)
    newConf
  }
}
