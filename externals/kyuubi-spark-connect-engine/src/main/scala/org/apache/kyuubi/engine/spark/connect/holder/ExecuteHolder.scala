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
package org.apache.kyuubi.engine.spark.connect.holder

import org.apache.kafka.common.Uuid
import org.apache.kyuubi.{KyuubiSQLException, Logging}
import org.apache.kyuubi.engine.spark.connect.grpc.proto.ExecutePlanRequest

class ExecuteHolder(
    val request: ExecutePlanRequest,
    val sessionHolder: SessionHolder) extends Logging {

  val session = sessionHolder.session

  val operationId = if (request.hasOperationId) {
    try {
      Uuid.fromString(request.getOperationId).toString
    } catch {
      case _: IllegalArgumentException =>
        throw KyuubiSQLException
    }
  } else {
    Uuid.randomUuid().toString
  }

  val jobTag
}

object ExecuteJobTag {
  private val prefix = "KyuubiSparkConnect_OperationTag"

  def apply(sessionId: String, userId: String, operationId: String): String = {
    s"${prefix}_" +
      s"User_${userId}_" +
      s"Session_${sessionId}_" +
      s"Operation_${operationId}"
  }

  def unapply(jobTag: String): Option[String] = {
    if (jobTag.startsWith(prefix)) Some(jobTag) else None
  }
}

object ExecuteSessionTag {
  private val prefix = "KyuubiSparkConnect_SessionTag"

  def apply(sessionId: String, userId: String, tag: String): String = {
    ProtoUtils
  }
}
