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
package org.apache.kyuubi.grpc.session

import org.apache.kyuubi.grpc.operation.GrpcOperationManager
import org.apache.kyuubi.service.CompositeService
import org.apache.kyuubi.util.ThreadUtils

import java.util.concurrent._


abstract class GrpcSessionManager(name: String) extends CompositeService(name) {

  @volatile private var shutdown = false

  private val sessionKeyToSession = new ConcurrentHashMap[SessionKey, GrpcSession]

  @volatile private var _latestLogoutTime: Long = System.currentTimeMillis()
  def latestLogoutTime: Long = _latestLogoutTime

  private val timeoutChecker =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor(s"$name-timeout-checker")

  protected def isServer: Boolean

  private var execPool: ThreadPoolExecutor = _

  def grpcOperationManager: GrpcOperationManager

  protected def getOrCreateSession(
      key: SessionKey,
      previouslyObservedSessionId: Option[String]): GrpcSession

  def openSession(
      key: SessionKey,
      previouslyObservedSessionId: Option[String]): SessionKey = {
    info(s"Opening grpc session for ${key.userId}")
    val session = getOrCreateSession(key, previouslyObservedSessionId)
    try {
      val key = session.sessionKey
      session.open()
      setSession(key, session)
      logSessionCountInfo(session, "opened")
    }
  }

  protected def removeSession(key: SessionKey): Option[GrpcSession]

  protected def shutdownSession(session: GrpcSession): Unit

  protected def closeSession(key: SessionKey): Unit

  protected def close(): Unit


  final protected def setSession(key: SessionKey, session: GrpcSession): Unit = {
    sessionKeyToSession.put(key, session)
  }

  protected def logSessionCountInfo(session: GrpcSession, action: String): Unit = {
    info(s"${session.sessionKey.userId}'s ${session.getClass.getSimpleName} with" +
      s" ${session.sessionKey.sessionId}${session.name.map("/" + _).getOrElse("")} is $action," +
      s" current opening sessions $getOpenSessionCount")
  }

  def getOpenSessionCount: Int = sessionKeyToSession.size()
}
