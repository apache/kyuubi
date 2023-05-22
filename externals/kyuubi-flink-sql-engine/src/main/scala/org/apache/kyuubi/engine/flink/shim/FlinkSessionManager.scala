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

import org.apache.kyuubi.engine.flink.FlinkEngineUtils
import org.apache.kyuubi.util.reflect.{DynConstructors, DynMethods}

class FlinkSessionManager(engineContext: DefaultContext) {

  val sessionManager: AnyRef = {
    if (FlinkEngineUtils.isFlinkVersionEqualTo("1.16")) {
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

  def start(): Unit = {
    if (FlinkEngineUtils.isFlinkVersionEqualTo("1.16")) {
      DynMethods.builder("start")
        .impl("org.apache.flink.table.gateway.service.session.SessionManager")
        .build()
        .invoke(sessionManager)
    } else {
      DynMethods.builder("start")
        .impl("org.apache.flink.table.gateway.service.session.SessionManagerImpl")
        .build()
        .invoke(sessionManager)
    }
  }

  def stop(): Unit = {
    if (FlinkEngineUtils.isFlinkVersionEqualTo("1.16")) {
      DynMethods.builder("stop")
        .impl("org.apache.flink.table.gateway.service.session.SessionManager")
        .build()
        .invoke(sessionManager)
    } else {
      DynMethods.builder("stop")
        .impl("org.apache.flink.table.gateway.service.session.SessionManagerImpl")
        .build()
        .invoke(sessionManager)
    }
  }

  def getSession(sessionHandle: SessionHandle): Session = {
    if (FlinkEngineUtils.isFlinkVersionEqualTo("1.16")) {
      DynMethods.builder("getSession")
        .impl(
          "org.apache.flink.table.gateway.service.session.SessionManager",
          classOf[SessionHandle])
        .build()
        .invoke(sessionManager, sessionHandle)
        .asInstanceOf[Session]
    } else {
      DynMethods.builder("getSession")
        .impl(
          "org.apache.flink.table.gateway.service.session.SessionManagerImpl",
          classOf[SessionHandle])
        .build()
        .invoke(sessionManager, sessionHandle)
        .asInstanceOf[Session]
    }
  }

  def openSession(environment: SessionEnvironment): Session = {
    if (FlinkEngineUtils.isFlinkVersionEqualTo("1.16")) {
      DynMethods.builder("openSession")
        .impl(
          "org.apache.flink.table.gateway.service.session.SessionManager",
          classOf[SessionEnvironment])
        .build()
        .invoke(sessionManager, environment)
        .asInstanceOf[Session]
    } else {
      DynMethods.builder("openSession")
        .impl(
          "org.apache.flink.table.gateway.service.session.SessionManagerImpl",
          classOf[SessionEnvironment])
        .build()
        .invoke(sessionManager, environment)
        .asInstanceOf[Session]
    }
  }

  def closeSession(sessionHandle: SessionHandle): Unit = {
    if (FlinkEngineUtils.isFlinkVersionEqualTo("1.16")) {
      DynMethods.builder("closeSession")
        .impl(
          "org.apache.flink.table.gateway.service.session.SessionManager",
          classOf[SessionHandle])
        .build()
        .invoke(sessionManager, sessionHandle)
    } else {
      DynMethods.builder("closeSession")
        .impl(
          "org.apache.flink.table.gateway.service.session.SessionManagerImpl",
          classOf[SessionHandle])
        .build()
        .invoke(sessionManager, sessionHandle)
    }
  }
}
