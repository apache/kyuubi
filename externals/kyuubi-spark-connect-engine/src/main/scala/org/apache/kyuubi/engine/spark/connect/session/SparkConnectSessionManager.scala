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
import org.apache.spark.sql.{SparkSQLUtils, SparkSession}
import org.apache.kyuubi.{KyuubiSQLException, Logging}
import org.apache.kyuubi.engine.spark.connect.grpc.GrpcOperationManager
import org.apache.kyuubi.engine.spark.connect.operation.SparkConnectOperationManager
import org.apache.kyuubi.grpc.operation.GrpcOperationManager
import org.apache.kyuubi.grpc.session
import org.apache.kyuubi.grpc.session.{GrpcSession, GrpcSessionManager}
import org.apache.kyuubi.operation.OperationManager
import org.apache.kyuubi.session.{Session, SessionManager}
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.TProtocolVersion

case class SessionKey(userId: String, sessionId: String)
class SparkConnectSessionManager private (name: String, spark: SparkSession)
  extends GrpcSessionManager(name) with Logging{
  override protected def isServer: Boolean = ???

  override def grpcOperationManager: GrpcOperationManager = ???

  override def getOrCreateSession(key: session.SessionKey, previouslyObservedSessionId: Option[String]): GrpcSession = ???

  override protected def removeSession(key: session.SessionKey): Option[GrpcSession] = ???

  override protected def shutdownSession(session: GrpcSession): Unit = ???
}
