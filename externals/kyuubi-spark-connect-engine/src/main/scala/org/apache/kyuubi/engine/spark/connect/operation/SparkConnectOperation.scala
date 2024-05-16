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

import org.apache.spark.sql.SparkSession
import org.apache.kyuubi.engine.spark.connect.session.SparkConnectSessionImpl
import org.apache.kyuubi.grpc.operation.{AbstractGrpcOperation, GrpcOperation, OperationKey}
import org.apache.kyuubi.grpc.session.GrpcSession
import org.apache.kyuubi.operation.{AbstractOperation, OperationStatus}
import org.apache.kyuubi.operation.FetchOrientation.FetchOrientation
import org.apache.kyuubi.operation.OperationState.OperationState
import org.apache.kyuubi.session.Session
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.{TFetchResultsResp, TGetResultSetMetadataResp, TStatus, TStatusCode}

abstract class SparkConnectOperation(session: SparkConnectSessionImpl)
  extends AbstractGrpcOperation(session) {

  protected def spark: SparkSession = session.spark

  val operationTag = OperationTag(key)

  // default empty, executePlanOperation will override it
  protected def sparkSessionTags: Set[String] = Set.empty[String]



  override def beforeRun(): Unit = {
    Thread.currentThread().setContextClassLoader(spark.sharedState.jarClassLoader)
  }



}

object OperationTag {
  private val prefix = "SparkConnect_OperationTag"

  def apply(key: OperationKey): String = {
    s"${prefix}_" +
      s"User_${key.key.userId}_" +
      s"Session_${key.key.sessionId}_" +
      s"Operation_${key.identifier}"
  }

  def unapply(tag: String): Option[String] = {
    if (tag.startsWith(prefix)) Some(tag) else None
  }
}
