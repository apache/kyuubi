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

import java.sql.{SQLTimeoutException, Statement}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import org.apache.spark.TaskKilled
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.sql.SparkSession
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.time.SpanSugar._

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.operation.JDBCTestUtils

class SparkEngineSuites extends KyuubiFunSuite {

  test("Add config to control if cancel invoke interrupt task on engine") {
    Seq(true, false).foreach { force =>
      withSparkJdbcStatement(Map(KyuubiConf.OPERATION_FORCE_CANCEL.key -> force.toString)) {
        case (statement, spark) =>
          val index = new AtomicInteger(0)
          val forceCancel = new AtomicBoolean(false)
          val listener = new SparkListener {
            override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
              assert(taskEnd.reason.isInstanceOf[TaskKilled])
              if (forceCancel.get()) {
                assert(System.currentTimeMillis() - taskEnd.taskInfo.launchTime < 3000)
                index.incrementAndGet()
              } else {
                assert(System.currentTimeMillis() - taskEnd.taskInfo.launchTime >= 4000)
                index.incrementAndGet()
              }
            }
          }

          spark.sparkContext.addSparkListener(listener)
          try {
            statement.setQueryTimeout(3)
            forceCancel.set(force)
            val e1 = intercept[SQLTimeoutException] {
              statement.execute("select java_method('java.lang.Thread', 'sleep', 5000L)")
            }.getMessage
            assert(e1.contains("Query timed out"))
            eventually(Timeout(30.seconds)) {
              assert(index.get() == 1)
            }
          } finally {
            spark.sparkContext.removeSparkListener(listener)
          }
      }
    }
  }

  private def withSparkJdbcStatement(
      conf: Map[String, String] = Map.empty)(
      statement: (Statement, SparkSession) => Unit): Unit = {
    val spark = new WithSparkSuite {
      override def withKyuubiConf: Map[String, String] = conf
      override protected def jdbcUrl: String = getJdbcUrl
    }
    spark.startSparkEngine()
    val tmp: Statement => Unit = { tmpStatement =>
      statement(tmpStatement, spark.getSpark)
    }
    try {
      spark.withJdbcStatement()(tmp)
    } finally {
      spark.stopSparkEngine()
    }
  }
}

trait WithSparkSuite extends WithSparkSQLEngine with JDBCTestUtils
