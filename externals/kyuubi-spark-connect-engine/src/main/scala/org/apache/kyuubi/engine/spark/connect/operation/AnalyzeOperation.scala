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
package org.apache.kyuubi.engine.spark.connect.operation

import io.grpc.stub.StreamObserver
import org.apache.kyuubi.Logging
import org.apache.kyuubi.engine.spark.connect.session.SparkConnectSessionImpl
import org.apache.kyuubi.grpc.operation.AbstractGrpcOperation
import org.apache.kyuubi.grpc.session.GrpcSession
import org.apache.kyuubi.operation.log.OperationLog
import org.apache.spark.connect.proto.{AnalyzePlanRequest, AnalyzePlanResponse}
import org.apache.spark.sql.SparkConnectPlanner

class AnalyzeOperation(session: GrpcSession,
                       request: AnalyzePlanRequest,
                       responseObserver: StreamObserver[AnalyzePlanResponse])
 extends SparkConnectOperation(session) with Logging {
  override protected def runInternal(): Unit = {
    lazy val planner = new SparkConnectPlanner(session)
    val spark = session.asInstanceOf[SparkConnectSessionImpl].spark
    val builder = AnalyzePlanResponse.newBuilder()
    request.getAnalyzeCase match {

    }
  }

  override def getOperationLog: Option[OperationLog] = ???

  override def isTimedOut: Boolean = ???
}
