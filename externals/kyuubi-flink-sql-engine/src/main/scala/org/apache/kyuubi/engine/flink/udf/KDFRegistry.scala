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

package org.apache.kyuubi.engine.flink.udf

import java.util

import scala.collection.mutable.ArrayBuffer

import org.apache.flink.table.functions.{ScalarFunction, UserDefinedFunction}
import org.apache.flink.table.gateway.service.context.SessionContext

import org.apache.kyuubi.{KYUUBI_VERSION, Utils}
import org.apache.kyuubi.config.KyuubiReservedKeys.{KYUUBI_ENGINE_NAME, KYUUBI_SESSION_USER_KEY}

object KDFRegistry {

  def createKyuubiDefinedFunctions(sessionContext: SessionContext): Array[KyuubiDefinedFunction] = {

    val kyuubiDefinedFunctions = new ArrayBuffer[KyuubiDefinedFunction]

    val flinkConfigMap: util.Map[String, String] = sessionContext.getSessionConf.toMap

    val kyuubi_version: KyuubiDefinedFunction = create(
      "kyuubi_version",
      new KyuubiVersionFunction(flinkConfigMap),
      "Return the version of Kyuubi Server",
      "string",
      "1.8.0")
    kyuubiDefinedFunctions += kyuubi_version

    val engineName: KyuubiDefinedFunction = create(
      "kyuubi_engine_name",
      new EngineNameFunction(flinkConfigMap),
      "Return the application name for the associated query engine",
      "string",
      "1.8.0")
    kyuubiDefinedFunctions += engineName

    val engineId: KyuubiDefinedFunction = create(
      "kyuubi_engine_id",
      new EngineIdFunction(flinkConfigMap),
      "Return the application id for the associated query engine",
      "string",
      "1.8.0")
    kyuubiDefinedFunctions += engineId

    val systemUser: KyuubiDefinedFunction = create(
      "kyuubi_system_user",
      new SystemUserFunction(flinkConfigMap),
      "Return the system user name for the associated query engine",
      "string",
      "1.8.0")
    kyuubiDefinedFunctions += systemUser

    val sessionUser: KyuubiDefinedFunction = create(
      "kyuubi_session_user",
      new SessionUserFunction(flinkConfigMap),
      "Return the session username for the associated query engine",
      "string",
      "1.8.0")
    kyuubiDefinedFunctions += sessionUser

    kyuubiDefinedFunctions.toArray
  }

  def create(
      name: String,
      udf: UserDefinedFunction,
      description: String,
      returnType: String,
      since: String): KyuubiDefinedFunction = {
    val kdf = KyuubiDefinedFunction(name, udf, description, returnType, since)
    kdf
  }

  def registerAll(sessionContext: SessionContext): Unit = {
    val functions = createKyuubiDefinedFunctions(sessionContext)
    for (func <- functions) {
      sessionContext.getSessionState.functionCatalog
        .registerTemporarySystemFunction(func.name, func.udf, true)
    }
  }
}

class KyuubiVersionFunction(confMap: util.Map[String, String]) extends ScalarFunction {
  def eval(): String = KYUUBI_VERSION
}

class EngineNameFunction(confMap: util.Map[String, String]) extends ScalarFunction {
  def eval(): String = {
    confMap match {
      case m if m.containsKey("yarn.application.name") => m.get("yarn.application.name")
      case m if m.containsKey("kubernetes.cluster-id") => m.get("kubernetes.cluster-id")
      case m => m.getOrDefault(KYUUBI_ENGINE_NAME, "unknown-engine-name")
    }
  }
}

class EngineIdFunction(confMap: util.Map[String, String]) extends ScalarFunction {
  def eval(): String = {
    confMap match {
      case m if m.containsKey("yarn.application.id") => m.get("yarn.application.id")
      case m if m.containsKey("kubernetes.cluster-id") => m.get("kubernetes.cluster-id")
      case m => m.getOrDefault("high-availability.cluster-id", "unknown-engine-id")
    }
  }
}

class SystemUserFunction(confMap: util.Map[String, String]) extends ScalarFunction {
  def eval(): String = Utils.currentUser
}

class SessionUserFunction(confMap: util.Map[String, String]) extends ScalarFunction {
  def eval(): String = confMap.getOrDefault(KYUUBI_SESSION_USER_KEY, "unknown-user")
}
