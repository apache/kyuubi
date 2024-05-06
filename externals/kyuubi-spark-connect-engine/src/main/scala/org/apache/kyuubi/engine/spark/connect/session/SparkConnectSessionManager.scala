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
package org.apache.kyuubi.engine.spark.connect.session

import java.util.UUID

import scala.collection.mutable

import org.apache.spark.sql.SparkSession

import org.apache.kyuubi.KyuubiSQLException
import org.apache.kyuubi.engine.spark.connect.holder.SessionKey
import org.apache.kyuubi.engine.spark.connect.operation.SparkConnectOperationManager
import org.apache.kyuubi.operation.OperationManager
import org.apache.kyuubi.session.{Session, SessionManager}
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.TProtocolVersion

class SparkConnectSessionManager private (name: String, spark: SparkSession)
  extends SessionManager(name) {

  def this(spark: SparkSession) = this(classOf[SparkConnectSessionManager].getSimpleName, spark)

  private lazy val sessionStore = mutable.HashMap[SessionKey, Session]()

  private val sessionsLock = new Object

  override protected def isServer: Boolean = false

  override def operationManager: OperationManager = new SparkConnectOperationManager(spark)

  override protected def createSession(
      protocol: TProtocolVersion,
      user: String,
      password: String,
      ipAddress: String,
      conf: Map[String, String]): Session = {
    throw KyuubiSQLException.featureNotSupported()
  }

  def getOrCreateSessionHolder(
      userId: String,
      sessionId: String,
      previouslyObservedSessionId: Option[String]): SparkConnectSessionImpl = {
    val sessionKey = SessionKey(userId, sessionId)
    val sparkConnectSession = getSparkConnectSession(
      sessionKey,
      Some(() => {
        validateSessionCreate(sessionKey)
        val sparkConnectSessionImpl = new SparkConnectSessionImpl()
        sparkConnectSessionImpl
      }))
    previouslyObservedSessionId.foreach(sessionId =>
      validateSessionId(sessionKey, sparkConnectSession.spark.sessionUUID, sessionId))
    sparkConnectSession
  }

  private def validateSessionCreate(key: SessionKey): Unit = {
    try {
      UUID.fromString(key.sessionId).toString
    } catch {
      case _: IllegalArgumentException =>
        throw KyuubiSQLException(msg = "INVALID_HANDLE.FORMAT")
    }
  }

  private def getSparkConnectSession(
      key: SessionKey,
      default: Option[() => SparkConnectSessionImpl]): SparkConnectSessionImpl = {
    sessionsLock.synchronized{
      val sessionOpt = sessionStore.get(key)
      val session = sessionOpt match {
        case Some(e) => e
        case None =>
          default match {
            case Some(callable) =>
              val session = callable()
              sessionStore.put(key, session)
              session
            case None =>
              null
          }
      }
      session match {
        case null => null
        case s: SparkConnectSessionImpl =>
          s
      }
    }
  }

  private def validateSessionId(key: SessionKey,
                                sessionUUID: String,
                                previouslyObservedSessionId: String): Unit = {
    if (sessionUUID != previouslyObservedSessionId) {
      throw KyuubiSQLException("INVALID_HANDLE.SESSION_CHANGED")
    }
  }
}
