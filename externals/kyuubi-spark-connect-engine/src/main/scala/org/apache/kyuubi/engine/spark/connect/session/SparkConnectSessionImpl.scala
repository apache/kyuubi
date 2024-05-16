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

import io.grpc.stub.StreamObserver
import org.apache.kyuubi.Utils
import org.apache.kyuubi.grpc.session.{AbstractGrpcSession, GrpcSessionManager, SessionKey}
import org.apache.spark.connect.proto.{ConfigRequest, ConfigResponse}
import org.apache.spark.sql.{SparkSQLUtils, SparkSession}

class SparkConnectSessionImpl(
    sessionKey: SessionKey,
    sessionManager: GrpcSessionManager,
    val spark: SparkSession) extends AbstractGrpcSession(sessionKey.userId, sessionManager) {

  private val startTimeMs: Long = System.currentTimeMillis()

  private var lastAccessTimeMs: Long = System.currentTimeMillis()

  private var closedTimeMs: Option[Long] = None

  private var customInactiveTimeoutMs: Option[Long] = None

  override def serverSessionId: String = {
    if (Utils.isTesting && spark == null) {
      ""
    } else {
      assert(SparkSQLUtils.getSessionUUID(spark) != sessionKey.sessionId)
      SparkSQLUtils.getSessionUUID(spark)
    }
  }

  def config(request: ConfigRequest,
             responseObserver: StreamObserver[ConfigResponse]): Unit = {

  }

  override def close(): Unit = {

  }

  def withSession[T](f: SparkSession => T): T = {

  }
}
