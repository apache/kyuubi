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

import org.apache.spark.{SparkArithmeticException, SparkException}
import org.apache.spark.sql.internal.SQLConf.ANSI_ENABLED
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime

import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.engine.spark.{WithDiscoverySparkSQLEngine, WithEmbeddedZookeeper}
import org.apache.kyuubi.service.ServiceState

abstract class SparkSQLEngineDeregisterSuite
  extends WithDiscoverySparkSQLEngine with WithEmbeddedZookeeper {
  protected val maxJobFailures: Int = 2

  override def withKyuubiConf: Map[String, String] = {
    super.withKyuubiConf ++
      zookeeperConf ++ Map(
        ANSI_ENABLED.key -> "true",
        ENGINE_DEREGISTER_JOB_MAX_FAILURES.key -> maxJobFailures.toString)
  }

  override val namespace: String = s"/kyuubi/deregister_test/${UUID.randomUUID().toString}"

  test("deregister when meeting specified exception") {
    spark.sql("CREATE TABLE t AS SELECT * FROM VALUES(CAST(2147483648 as DOUBLE))")
    val query = "SELECT CAST(col1 AS Integer) from t"
    assert(engine.frontendServices.head.discoveryService.get.getServiceState ===
      ServiceState.STARTED)
    (0 until maxJobFailures).foreach { _ =>
      intercept[Exception](spark.sql(query).collect()) match {
        case se: SparkException => assert(se.getCause.isInstanceOf[ArithmeticException])
        case e => assert(e.isInstanceOf[SparkArithmeticException])
      }
    }
    eventually(timeout(5.seconds), interval(1.second)) {
      assert(engine.frontendServices.head.discoveryService.get.getServiceState ===
        ServiceState.STOPPED)
    }
  }
}

class SparkSQLEngineDeregisterExceptionSuite extends SparkSQLEngineDeregisterSuite {
  override def withKyuubiConf: Map[String, String] = {
    super.withKyuubiConf ++
      Map(ENGINE_DEREGISTER_EXCEPTION_CLASSES.key ->
        classOf[SparkArithmeticException].getCanonicalName)
  }
}

class SparkSQLEngineDeregisterMsgSuite extends SparkSQLEngineDeregisterSuite {
  override def withKyuubiConf: Map[String, String] = {
    super.withKyuubiConf ++ Map(ENGINE_DEREGISTER_EXCEPTION_MESSAGES.key ->
      // see https://issues.apache.org/jira/browse/SPARK-38926
      // Upper case SQL types in error messages
      // see https://issues.apache.org/jira/browse/SPARK-39007
      // Use double quotes for SQL configs in error messages
      // see https://issues.apache.org/jira/browse/SPARK-39214
      // Improve errors related to CAST
      "overflow")
  }
}

class SparkSQLEngineDeregisterExceptionTTLSuite
  extends WithDiscoverySparkSQLEngine with WithEmbeddedZookeeper {
  protected val maxJobFailures: Int = 2
  protected val deregisterExceptionTTL = 2000

  override def withKyuubiConf: Map[String, String] = {
    super.withKyuubiConf ++
      zookeeperConf ++ Map(
        ANSI_ENABLED.key -> "true",
        ENGINE_DEREGISTER_EXCEPTION_CLASSES.key ->
          classOf[SparkArithmeticException].getCanonicalName,
        ENGINE_DEREGISTER_JOB_MAX_FAILURES.key -> maxJobFailures.toString,
        ENGINE_DEREGISTER_EXCEPTION_TTL.key -> deregisterExceptionTTL.toString)
  }

  override val namespace: String = s"/kyuubi/deregister_test/${UUID.randomUUID().toString}"

  test("deregister exception ttl test") {
    spark.sql("CREATE TABLE t AS SELECT * FROM VALUES(CAST(2147483648 as DOUBLE))")
    val query = "SELECT CAST(col1 AS Integer) from t"
    assert(engine.frontendServices.head.discoveryService.get.getServiceState ===
      ServiceState.STARTED)

    intercept[Exception](spark.sql(query).collect()) match {
      case se: SparkException => assert(se.getCause.isInstanceOf[ArithmeticException])
      case e => assert(e.isInstanceOf[SparkArithmeticException])
    }

    Thread.sleep(deregisterExceptionTTL + 1000)

    (0 until maxJobFailures).foreach { _ =>
      intercept[Exception](spark.sql(query).collect()) match {
        case se: SparkException => assert(se.getCause.isInstanceOf[ArithmeticException])
        case e => assert(e.isInstanceOf[SparkArithmeticException])
      }
    }
    eventually(timeout(5.seconds), interval(1.second)) {
      assert(engine.frontendServices.head.discoveryService.get.getServiceState ===
        ServiceState.STOPPED)
    }
  }
}
