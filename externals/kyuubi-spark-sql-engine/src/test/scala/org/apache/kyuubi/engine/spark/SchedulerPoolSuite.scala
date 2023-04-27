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

import scala.concurrent.duration.SECONDS

import org.apache.spark.KyuubiSparkContextHelper
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobEnd, SparkListenerJobStart}
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime

import org.apache.kyuubi.operation.HiveJDBCTestHelper

class SchedulerPoolSuite extends WithSparkSQLEngine with HiveJDBCTestHelper {
  override protected def jdbcUrl: String = getJdbcUrl
  override def withKyuubiConf: Map[String, String] = {
    val poolFile =
      Thread.currentThread().getContextClassLoader.getResource("test-scheduler-pool.xml")
    Map(
      "spark.scheduler.mode" -> "FAIR",
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
      threads.execute(() => {
        // job name job1
        withJdbcStatement() { statement =>
          statement.execute("SET kyuubi.operation.scheduler.pool=p1")
          statement.execute("SELECT java_method('java.lang.Thread', 'sleep', 1500l)" +
            " FROM range(1, 3, 1, 2)")
        }
      })
      // make sure job1 started before job2
      eventually(Timeout(2.seconds)) {
        assert(job1StartTime > 0)
      }

      threads.execute(() => {
        // job name job2
        withJdbcStatement() { statement =>
          statement.execute("SET kyuubi.operation.scheduler.pool=p0")
          statement.execute("SELECT java_method('java.lang.Thread', 'sleep', 1500l)" +
            "FROM range(1, 3, 1, 2)")
        }
      })
      threads.shutdown()
      threads.awaitTermination(20, SECONDS)
      // make sure the SparkListener has received the finished events for job1 and job2.
      KyuubiSparkContextHelper.waitListenerBus(spark)
      // job1 should be started before job2
      assert(job1StartTime < job2StartTime)
      // job2 minShare is 2(total resource) so that job1 should be allocated tasks after
      // job2 finished.
      assert(job2FinishTime < job1FinishTime)
    } finally {
      spark.sparkContext.removeSparkListener(listener)
    }
  }
}
