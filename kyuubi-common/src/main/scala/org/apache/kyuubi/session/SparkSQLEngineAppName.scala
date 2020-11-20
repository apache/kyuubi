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

import java.time.Instant
import java.util.Locale

import org.apache.curator.utils.ZKPaths

import org.apache.kyuubi.session.EngineScope.{EngineScope, GROUP, SERVER, SESSION, USER}

class SparkSQLEngineAppName(
   engineScope: EngineScope,
   serverHost: String,
   serverPort: Int,
   userGroup: String,
   user: String,
   handle: String) {

  import SparkSQLEngineAppName._

  def generateAppName(): String = {
    val appNameBuilder = StringBuilder.newBuilder
      .append(APP_NAME_PREFIX)
      .append(DELIMITER).append(engineScope.toString.toLowerCase(Locale.ROOT))
    engineScope match {
      case SESSION =>
        appNameBuilder.append(DELIMITER).append(serverHost)
          .append(DELIMITER).append(serverPort)
          .append(DELIMITER).append(userGroup)
          .append(DELIMITER).append(user)
          .append(DELIMITER).append(handle)
      case USER =>
        appNameBuilder.append(DELIMITER).append(user)
      case GROUP =>
        appNameBuilder.append(DELIMITER).append(userGroup)
      case SERVER =>
        appNameBuilder.append(DELIMITER).append(serverHost)
          .append(DELIMITER).append(serverPort)
    }
    appNameBuilder.append(DELIMITER).append(Instant.now).mkString
  }

  def makeZkPath(zkNamespace: String): String = {
    engineScope match {
      case SESSION =>
        ZKPaths.makePath(zkNamespace, "sessions", handle)
      case USER =>
        ZKPaths.makePath(zkNamespace, "users", user)
      case GROUP =>
        ZKPaths.makePath(zkNamespace, "groups", userGroup)
      case SERVER =>
        ZKPaths.makePath(zkNamespace, "servers", serverHost + ":" + serverPort)
    }
  }

}

object SparkSQLEngineAppName {

  private val APP_NAME_PREFIX = "kyuubi"

  private val DELIMITER = "|"

  def apply(engineScope: EngineScope, serverHost: String, serverPort: Int,
        userGroup: String, user: String, handle: String): SparkSQLEngineAppName =
    new SparkSQLEngineAppName(engineScope, serverHost, serverPort, userGroup, user, handle)

  def parseAppName(appName: String): SparkSQLEngineAppName = {
    val params = appName.split("\\|")
    val engineScope = EngineScope.withName(params(1).toUpperCase(Locale.ROOT))
    engineScope match {
      case SESSION =>
        SparkSQLEngineAppName(engineScope, serverHost = params(2), serverPort = params(3).toInt,
          userGroup = params(4), user = params(5), handle = params(6))
      case USER =>
        SparkSQLEngineAppName(engineScope, serverHost = null, serverPort = 0,
          userGroup = null, user = params(2), handle = null)
      case GROUP =>
        SparkSQLEngineAppName(engineScope, serverHost = null, serverPort = 0,
          userGroup = params(2), user = null, handle = null)
      case SERVER =>
        SparkSQLEngineAppName(engineScope, serverHost = params(2), serverPort = params(3).toInt,
          userGroup = null, user = null, handle = null)
    }
  }
}

