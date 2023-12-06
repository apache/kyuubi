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
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobEnd, SparkListenerJobStart, SparkListenerTaskEnd}
import org.apache.spark.sql.SparkSession
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.time.SpanSugar._

import org.apache.kyuubi.{KyuubiException, KyuubiFunSuite}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.config.KyuubiReservedKeys._
import org.apache.kyuubi.operation.HiveJDBCTestHelper

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
              // When OPERATION_FORCE_CANCEL variable is true, and the task execution is cancelled,
              // the following statement will be executed
              index.incrementAndGet()
            }

            override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
              // Means the query statement is executed
              index.incrementAndGet()
            }

            override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
              // Always be executed
              index.incrementAndGet()
            }
          }

          spark.sparkContext.addSparkListener(listener)
          try {
            statement.setQueryTimeout(5)
            forceCancel.set(force)
            val e1 = intercept[SQLTimeoutException] {
              statement.execute("select java_method('java.lang.Thread', 'sleep', 500000L)")
            }.getMessage
            assert(e1.contains("Query timed out"))
            assert(index.get() != 0, "The query statement was not executed.")
            eventually(Timeout(30.seconds)) {
              if (forceCancel.get()) {
                assert(index.get() == 3)
              } else {
                assert(index.get() == 2)
              }
            }
          } finally {
            spark.sparkContext.removeSparkListener(listener)
          }
      }
    }
  }

  test("test engine submit timeout") {
    val timeout = 180000
    val submitTime = System.currentTimeMillis() - timeout
    withSystemProperty(Map(
      s"spark.$KYUUBI_ENGINE_SUBMIT_TIME_KEY" -> String.valueOf(submitTime),
      s"spark.${ENGINE_INIT_TIMEOUT.key}" -> String.valueOf(timeout))) {
      SparkSQLEngine.setupConf()
      SparkSQLEngine.currentEngine = None
      val e1 = intercept[KyuubiException] {
        SparkSQLEngine.main(Array.empty)
      }.getMessage
      assert(SparkSQLEngine.currentEngine.isEmpty)
      assert(e1.startsWith("The total engine initialization time"))
    }
  }

  test("test engine `createSpark` timeout") {
    val timeout = 3000
    val submitTime = System.currentTimeMillis()
    withSystemProperty(Map(
      s"spark.$KYUUBI_ENGINE_SUBMIT_TIME_KEY" -> String.valueOf(submitTime),
      s"spark.${ENGINE_INIT_TIMEOUT.key}" -> String.valueOf(timeout),
      s"spark.${ENGINE_SPARK_INITIALIZE_SQL.key}" ->
        "select 1 where java_method('java.lang.Thread', 'sleep', 60000L) is null")) {
      SparkSQLEngine.setupConf()
      SparkSQLEngine.currentEngine = None
      val logAppender = new LogAppender("test createSpark timeout")
      withLogAppender(logAppender) {
        try {
          SparkSQLEngine.main(Array.empty)
        } catch {
          case e: Exception => error("", e)
        }
      }
      assert(SparkSQLEngine.currentEngine.isEmpty)
      val errorMsg = s"The Engine main thread was interrupted, possibly due to `createSpark`" +
        s" timeout. The `${ENGINE_INIT_TIMEOUT.key}` is ($timeout ms) " +
        s" and submitted at $submitTime."
      assert(logAppender.loggingEvents.exists(
        _.getMessage.getFormattedMessage.equals(errorMsg)))
      SparkSession.getActiveSession.foreach(_.close())
      SparkSession.getDefaultSession.foreach(_.close())
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

trait WithSparkSuite extends WithSparkSQLEngine with HiveJDBCTestHelper
