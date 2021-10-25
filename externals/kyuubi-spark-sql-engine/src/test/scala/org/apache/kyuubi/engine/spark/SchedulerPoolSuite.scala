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

import java.util.concurrent.Executors

import org.apache.spark.scheduler.{SparkListener, SparkListenerJobEnd, SparkListenerJobStart}
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime

import org.apache.kyuubi.operation.JDBCTestHelper

class SchedulerPoolSuite extends WithSparkSQLEngine with JDBCTestHelper {
  override protected def jdbcUrl: String = getJdbcUrl
  override def withKyuubiConf: Map[String, String] = {
    val poolFile =
      Thread.currentThread().getContextClassLoader.getResource("test-scheduler-pool.xml")
    Map("spark.scheduler.mode" -> "FAIR",
      "spark.scheduler.allocation.file" -> poolFile.getFile,
      "spark.master" -> "local[2]")
  }

  test("Scheduler pool") {
    @volatile var job0Started = false
    @volatile var job1StartTime = 0L
    @volatile var job2StartTime = 0L
    @volatile var job1FinishTime = 0L
    @volatile var job2FinishTime = 0L
    val listener = new SparkListener {
      override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
        info(jobStart)
        jobStart.jobId - initJobId match {
          case 1 => job1StartTime = jobStart.time
          case 2 => job2StartTime = jobStart.time
          case 0 => job0Started = true
        }
      }

      override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
        info(jobEnd)
        jobEnd.jobId - initJobId match {
          case 1 => job1FinishTime = jobEnd.time
          case 2 => job2FinishTime = jobEnd.time
          case _ =>
        }
      }
    }
    spark.sparkContext.addSparkListener(listener)

    try {
      val threads = Executors.newFixedThreadPool(3)
      threads.execute(() => {
        withJdbcStatement() { statement =>
          statement.execute("SET kyuubi.operation.scheduler.pool=p0")
          statement.execute("SELECT java_method('java.lang.Thread', 'sleep', 5000l)" +
            "FROM range(1, 3, 1, 2)")
        }
      })
      // make sure job0 started then we have no resource right now
      eventually(Timeout(3.seconds)) {
        assert(job0Started)
      }
      Seq(1, 0).foreach { priority =>
        threads.execute(() => {
          priority match {
            case 0 =>
              withJdbcStatement() { statement =>
                statement.execute("SET kyuubi.operation.scheduler.pool=p0")
                statement.execute("SELECT java_method('java.lang.Thread', 'sleep', 1500l)" +
                  "FROM range(1, 3, 1, 2)")
              }

            case 1 =>
              withJdbcStatement() { statement =>
                statement.execute("SET kyuubi.operation.scheduler.pool=p1")
                statement.execute("SELECT java_method('java.lang.Thread', 'sleep', 1500l)" +
                  " FROM range(1, 3, 1, 2)")
              }
          }
        })
      }
      threads.shutdown()
      eventually(Timeout(10.seconds)) {
        // We can not ensure that job1 is started before job2 so here using abs.
        assert(Math.abs(job1StartTime - job2StartTime) < 1000)
        // Job1 minShare is 2(total resource) so that job2 should be allocated tasks after
        // job1 finished.
        assert(job2FinishTime - job1FinishTime >= 1000)
      }
    } finally {
      spark.sparkContext.removeSparkListener(listener)
    }
  }
}
