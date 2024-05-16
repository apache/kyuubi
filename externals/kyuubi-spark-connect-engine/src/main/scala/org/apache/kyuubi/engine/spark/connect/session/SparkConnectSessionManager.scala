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

import org.apache.spark.sql.SparkSession
import org.apache.kyuubi.KyuubiSQLException
import org.apache.kyuubi.grpc.operation.GrpcOperationManager
import org.apache.kyuubi.grpc.session.{GrpcSession, GrpcSessionManager, SessionKey}

class SparkConnectSessionManager(name: String, spark: SparkSession)
  extends GrpcSessionManager(name) {

  private val sessionsLock = new Object

  private def validateSessionId(
      key: SessionKey,
      sessionUUID: String,
      previouslyObservedSessionId: String) = {
    if (sessionUUID != previouslyObservedSessionId) {
      throw KyuubiSQLException(s"Invalid sessionUUID $sessionUUID")
    }
  }

  override def getOrCreateSession(
      key: SessionKey): GrpcSession = {
    var session = getSession(key)
    if (session == null) {
      session = new SparkConnectSessionImpl(key, this, newIsolatedSession())
    }
    session
  }

  private def newIsolatedSession(): SparkSession = {
    spark
  }

  override protected def isServer: Boolean = ???

  override def grpcOperationManager: GrpcOperationManager = ???

  override protected def removeSession(key: SessionKey): Option[GrpcSession] = ???

  override protected def shutdownSession(session: GrpcSession): Unit = ???
}
