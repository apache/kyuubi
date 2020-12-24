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

import org.apache.curator.utils.ZKPaths

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.ENGINE_SCOPE
import org.apache.kyuubi.engine.EngineScope.{EngineScope, SESSION}

class EngineAppName(user: String, sessionId: String, conf: KyuubiConf) {

  import EngineAppName._

  private val engineScope = EngineScope.withName(conf.get(ENGINE_SCOPE))

  def getEngineScope: EngineScope = engineScope

  /**
   * kyuubi_[KGUS]_user_sessionid
   *
   * @return engine app name
   */
  def generateAppName(): String = {
    StringBuilder.newBuilder.append(APP_NAME_PREFIX)
      .append(DELIMITER).append(engineScope)
      .append(DELIMITER).append(user)
      .append(DELIMITER).append(sessionId).mkString
  }

  /**
   * if engine scope is [S] return
   * /[zkNamespace]-engine/S/[user]/[sessionId]
   * else return
   * /[zkNamespace]-engine/[engineScope]/[user]
   *
   * @param zkNamespace zk root path
   * @return engine zk path
   */
  def makeZkPath(zkNamespace: String): String = {
    val namespace = zkNamespace + "-" + ZK_NAMESPACE_SUFFIX
    engineScope match {
      case SESSION =>
        ZKPaths.makePath(namespace, engineScope.toString, user, sessionId)
      case _ =>
        ZKPaths.makePath(namespace, engineScope.toString, user)
    }
  }

}

object EngineAppName {

  private val APP_NAME_PREFIX = "kyuubi"

  private val ZK_NAMESPACE_SUFFIX = "engine"

  private val DELIMITER = "_"

  val SPARK_APP_NAME_KEY = "spark.app.name"

  def apply(user: String, sessionId: String, conf: KyuubiConf): EngineAppName =
    new EngineAppName(user, sessionId, conf)

  def parseAppName(appName: String, conf: KyuubiConf): EngineAppName = {
    val params = appName.split(DELIMITER)
    val clone = conf.clone
    clone.set(ENGINE_SCOPE, params(1))
    EngineAppName(params(2), params(3), conf)
  }

}

