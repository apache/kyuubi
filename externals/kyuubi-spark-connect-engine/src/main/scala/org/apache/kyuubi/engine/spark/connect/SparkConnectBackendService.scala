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
package org.apache.kyuubi.engine.spark.connect

import io.grpc.stub.StreamObserver
import org.apache.spark.sql.SparkSession
import org.apache.kyuubi.engine.spark.connect.session.{SparkConnectSessionImpl, SparkConnectSessionManager}
import org.apache.kyuubi.grpc.service.AbstractGrpcBackendService
import org.apache.kyuubi.grpc.session.SessionKey
import org.apache.spark.connect.proto.{ConfigRequest, ConfigResponse}

class SparkConnectBackendService(name: String, spark: SparkSession)
  extends AbstractGrpcBackendService(name) {
  def this(spark: SparkSession) = this(classOf[SparkConnectBackendService].getSimpleName, spark)
  override def grpcSessionManager: SparkConnectSessionManager =
    new SparkConnectSessionManager(name, spark)

  def sparkSession: SparkSession = spark

  def config(
      key: SessionKey,
      request: ConfigRequest,
      responseObserver: StreamObserver[ConfigResponse]): Unit = {
    grpcSessionManager.getSession(key).config(request, responseObserver)
  }

  def getSession(key: SessionKey): SparkConnectSessionImpl = {
    grpcSessionManager.getOrCreateSession(key)
  }
}
