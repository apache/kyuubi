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

package org.apache.kyuubi.engine.flink.shim

import org.apache.flink.table.gateway.api.session.{SessionEnvironment, SessionHandle}
import org.apache.flink.table.gateway.service.context.DefaultContext
import org.apache.flink.table.gateway.service.session.Session

import org.apache.kyuubi.engine.flink.FlinkEngineUtils.FLINK_RUNTIME_VERSION
import org.apache.kyuubi.util.reflect._
import org.apache.kyuubi.util.reflect.ReflectUtils._

class FlinkSessionManager(engineContext: DefaultContext) {

  val sessionManager: AnyRef = {
    if (FLINK_RUNTIME_VERSION === "1.16") {
      DynConstructors.builder().impl(
        "org.apache.flink.table.gateway.service.session.SessionManager",
        classOf[DefaultContext])
        .build()
        .newInstance(engineContext)
    } else {
      DynConstructors.builder().impl(
        "org.apache.flink.table.gateway.service.session.SessionManagerImpl",
        classOf[DefaultContext])
        .build()
        .newInstance(engineContext)
    }
  }

  def start(): Unit = invokeAs(sessionManager, "start")

  def stop(): Unit = invokeAs(sessionManager, "stop")

  def getSession(sessionHandle: SessionHandle): Session =
    invokeAs(sessionManager, "getSession", (classOf[SessionHandle], sessionHandle))

  def openSession(environment: SessionEnvironment): Session =
    invokeAs(sessionManager, "openSession", (classOf[SessionEnvironment], environment))

  def closeSession(sessionHandle: SessionHandle): Unit =
    invokeAs(sessionManager, "closeSession", (classOf[SessionHandle], sessionHandle))
}
