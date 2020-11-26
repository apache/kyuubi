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

package org.apache.kyuubi.engine

import java.net.InetAddress

import org.apache.curator.utils.ZKPaths

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.{ENGINE_SCOPE, FRONTEND_BIND_HOST, FRONTEND_BIND_PORT}
import org.apache.kyuubi.engine.EngineScope.{GROUP, SERVER, SESSION, USER}

class EngineAppName(user: String, sessionId: String, conf: KyuubiConf) {

  import EngineAppName._

  private val engineScope = EngineScope.withName(conf.get(ENGINE_SCOPE))
  private val serverHost = conf.get(FRONTEND_BIND_HOST)
    .getOrElse(InetAddress.getLocalHost.getHostName)
  private val serverPort = conf.get(FRONTEND_BIND_PORT)
  // TODO: config user group
  private val userGroup = "default"

  /**
   * kyuubi_host_port_[KGUS]user_sessionid
   *
   * @return
   */
  def generateAppName(): String = {
    StringBuilder.newBuilder.append(APP_NAME_PREFIX)
      .append(DELIMITER).append(serverHost)
      .append(DELIMITER).append("[").append(engineScope).append("]").append(user)
      .append(DELIMITER).append(sessionId).mkString
  }

  def makeZkPath(zkNamespace: String): String = {
    engineScope match {
      case SESSION =>
        ZKPaths.makePath(zkNamespace, "sessions", sessionId)
      case USER =>
        ZKPaths.makePath(zkNamespace, "users", user)
      case GROUP =>
        ZKPaths.makePath(zkNamespace, "groups", userGroup)
      case SERVER =>
        ZKPaths.makePath(zkNamespace, "servers", serverHost + ":" + serverPort)
    }
  }

}

object EngineAppName {

  private val APP_NAME_PREFIX = "kyuubi"

  private val DELIMITER = "_"

  val SPARK_APP_NAME_KEY = "spark.app.name"

  def apply(user: String, sessionId: String, conf: KyuubiConf): EngineAppName =
    new EngineAppName(user, sessionId, conf)

  def parseAppName(appName: String, conf: KyuubiConf): EngineAppName = {
    val params = appName.split(DELIMITER)
    val user = params(2).substring(3)
    EngineAppName(user, params(3), conf)
  }
}

