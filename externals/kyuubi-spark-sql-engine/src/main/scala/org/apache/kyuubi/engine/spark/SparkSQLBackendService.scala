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

import org.apache.spark.sql.SparkSession

import org.apache.kyuubi.engine.spark.session.SparkSQLSessionManager
import org.apache.kyuubi.service.AbstractBackendService
import org.apache.kyuubi.session.SessionManager

/**
 * A [[org.apache.kyuubi.service.BackendService]] constructed
 * with [[SparkSession]] which give it the ability to talk with
 * Spark and let Spark do all the rest heavy work :)
 *
 *  @param name  Service Name
 *  @param spark A [[SparkSession]] instance
 *               that this backend service holds to run [[org.apache.kyuubi.operation.Operation]]s.
 */
class SparkSQLBackendService(name: String, spark: SparkSession)
  extends AbstractBackendService(name) {

  def this(spark: SparkSession) = this(classOf[SparkSQLBackendService].getSimpleName, spark)

  override val sessionManager: SessionManager = new SparkSQLSessionManager(spark)
}
