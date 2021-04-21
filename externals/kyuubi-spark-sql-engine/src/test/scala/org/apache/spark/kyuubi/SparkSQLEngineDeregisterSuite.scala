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

package org.apache.spark.kyuubi

import java.util.UUID

import org.apache.spark.SparkException
import org.apache.spark.sql.internal.SQLConf.{ANSI_ENABLED, DECIMAL_OPERATIONS_ALLOW_PREC_LOSS}

import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.engine.spark.WithDiscoverySparkSQLEngine
import org.apache.kyuubi.service.ServiceState

abstract class SparkSQLEngineDeregisterSuite extends WithDiscoverySparkSQLEngine{
  override def withKyuubiConf: Map[String, String] = {
    super.withKyuubiConf ++ Map(
      ANSI_ENABLED.key -> "true",
      DECIMAL_OPERATIONS_ALLOW_PREC_LOSS.key -> "false"
    )
  }

  override val namespace: String = s"/kyuubi/deregister_test/${UUID.randomUUID().toString}"

  test("deregister when meeting specified exception") {
    spark.sql("CREATE TABLE t AS SELECT * FROM VALUES(-499.198741, 1, 1)")
    val query = "SELECT CAST(col1 AS DECIMAL(18,6)) / CAST(col2 AS DECIMAL(18,14)) * " +
      "CAST(col3 AS DECIMAL(22,18)) from t"
    assert(engine.discoveryService.getServiceState === ServiceState.STARTED)
    intercept[SparkException](spark.sql(query).collect())
    assert(engine.discoveryService.getServiceState === ServiceState.STOPPED)
  }
}

class SparkSQLEngineDeregisterExceptionSuite extends SparkSQLEngineDeregisterSuite {
  override def withKyuubiConf: Map[String, String] = {
    super.withKyuubiConf ++ Map(ENGINE_DEREGISTER_EXCEPTION_CLASSES.key ->
      classOf[SparkException].getCanonicalName)
  }
}

class SparkSQLEngineDeregisterMsgSuite extends SparkSQLEngineDeregisterSuite {
  override def withKyuubiConf: Map[String, String] = {
    super.withKyuubiConf ++ Map(ENGINE_DEREGISTER_EXCEPTION_MESSAGES.key ->
      classOf[ArithmeticException].getCanonicalName)
  }
}

class SparkSQLEngineDeregisterStacktraceSuite extends SparkSQLEngineDeregisterSuite {
  override def withKyuubiConf: Map[String, String] = {
    super.withKyuubiConf ++ Map(ENGINE_DEREGISTER_EXCEPTION_STACKTRACES.key ->
      "org.apache.spark.scheduler.DAGScheduler.abortStage")
  }
}
