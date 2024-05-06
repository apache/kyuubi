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

import scala.collection.mutable

import io.grpc.stub.StreamObserver
import org.apache.spark.sql.SparkSession

import org.apache.kyuubi.engine.spark.connect.grpc.GrpcSessionManager
import org.apache.kyuubi.engine.spark.connect.grpc.proto._
import org.apache.kyuubi.engine.spark.connect.holder.{SessionHolder, SessionKey}
import org.apache.kyuubi.operation.OperationManager
import org.apache.kyuubi.session.{Session, SessionManager}
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.TProtocolVersion

class SparkConnectSessionManager private (name: String, spark: SparkSession)
  extends SessionManager(name) {

  def this(spark: SparkSession) = this(classOf[SparkConnectSessionManager].getSimpleName, spark)

  private lazy val sessionStore = mutable.HashMap[SessionKey, SessionHolder]()

  override protected def isServer: Boolean = false

  override def operationManager: OperationManager = ???

  override protected def createSession(protocol: TProtocolVersion, user: String, password: String, ipAddress: String, conf: Map[String, String]): Session = ???
}
