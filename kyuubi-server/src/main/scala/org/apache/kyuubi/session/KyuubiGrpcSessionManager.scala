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

import java.util.concurrent.ConcurrentHashMap

import org.apache.kyuubi.KyuubiSQLException
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.engine.KyuubiApplicationManager
import org.apache.kyuubi.operation.KyuubiGrpcOperationManager

class KyuubiGrpcSessionManager extends GrpcSessionManager("KyuubiGrpcSessionManager") {

  val applicationManager = new KyuubiApplicationManager()

  override protected def isServer: Boolean = true

  override def operationManager: KyuubiGrpcOperationManager = new KyuubiGrpcOperationManager

  private val handleToSession = new ConcurrentHashMap[GrpcSessionHandle, KyuubiGrpcSession]

  override def initialize(conf: KyuubiConf): Unit = {
    this.conf = conf
    addService(applicationManager)
    super.initialize(conf)
  }

  override def getOrCreateSession(
      sessionKey: GrpcSessionHandle,
      previouslyObservedSesssionId: Option[String]): KyuubiGrpcSession = {

    if (handleToSession.containsKey(sessionKey)) {
      handleToSession.get(sessionKey)
    } else {
      info(s"Opening session $sessionKey")
      val session = new KyuubiGrpcSession(sessionKey, Map.empty, this.conf.clone, this)
      val handle = session.handle
      try {
        setSession(handle, session)
        session.open()
        logSessionCountInfo(session, "opened")
      } catch {
        case e: Exception =>
          try {
            closeSession(handle)
          } catch {
            case t: Throwable =>
              warn(s"Error closing session for $sessionKey", t)
          }
          throw KyuubiSQLException(e)
      }
      handleToSession.put(sessionKey, session)
      session
    }
  }
}
