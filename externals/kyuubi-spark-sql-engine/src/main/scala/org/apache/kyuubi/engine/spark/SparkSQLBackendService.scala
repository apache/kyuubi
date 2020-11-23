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

package org.apache.kyuubi.engine.spark

import java.util.concurrent.{ExecutionException, TimeoutException, TimeUnit}

import scala.concurrent.CancellationException

import org.apache.spark.sql.SparkSession

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.engine.spark.operation.ExecuteStatement
import org.apache.kyuubi.engine.spark.session.SparkSQLSessionManager
import org.apache.kyuubi.operation.{Operation, OperationHandle, OperationStatus}
import org.apache.kyuubi.service.AbstractBackendService
import org.apache.kyuubi.service.BackendService
import org.apache.kyuubi.session.Session
import org.apache.kyuubi.session.SessionManager

/**
 * A [[BackendService]] constructed with [[SparkSession]] which give it the ability to talk with
 * Spark and let Spark do all the rest heavy work :)
 *
 *  @param name  Service Name
 * @param spark A [[SparkSession]] instance that this backend service holds to run [[Operation]]s.
 */
class SparkSQLBackendService(name: String, spark: SparkSession)
  extends AbstractBackendService(name) {

  private lazy val timeout = conf.get(KyuubiConf.ENGINE_LONG_POLLING_TIMEOUT)

  def this(spark: SparkSession) = this(classOf[SparkSQLBackendService].getSimpleName, spark)

  override val sessionManager: SessionManager = new SparkSQLSessionManager(spark)

  override def getOperationStatus(operationHandle: OperationHandle): OperationStatus = {
    val operation = sessionManager.operationManager.getOperation(operationHandle)
    operation match {
      case es: ExecuteStatement if es.shouldRunAsync =>
        try {
          es.backgroundHandle.get(timeout, TimeUnit.MILLISECONDS)
        } catch {
          case e: TimeoutException =>
            debug(s"$operationHandle: Long polling timed out, ${e.getMessage}")
          case e: CancellationException =>
            debug(s"$operationHandle: The background operation was cancelled, ${e.getMessage}")
          case e: ExecutionException =>
            debug(s"$operationHandle: The background operation was aborted, ${e.getMessage}")
          case _: InterruptedException =>
        }
      case _ =>
    }
    super.getOperationStatus(operationHandle)
  }
}
