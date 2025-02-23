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
package org.apache.spark.sql

import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent}
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.time.SpanSugar._

import org.apache.kyuubi.sql.KyuubiSQLConf.COLLECT_METRICS_PRETTY_DISPLAY_ENABLED

class CollectMetricsPrettyDisplaySuite extends KyuubiSparkSQLExtensionTest {

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    setupData()
  }

  test("collect metrics pretty display") {
    withSQLConf(COLLECT_METRICS_PRETTY_DISPLAY_ENABLED.key -> "true") {
      val executionId = new AtomicLong(-1)
      val executionIdListener = new SparkListener {
        override def onOtherEvent(event: SparkListenerEvent): Unit = {
          event match {
            case e: SparkListenerSQLExecutionEnd =>
              executionId.set(e.executionId)
            case _ =>
          }
        }
      }

      spark.sparkContext.addSparkListener(executionIdListener)
      try {
        import org.apache.spark.sql.functions._
        spark.table("t1").observe("observer1", sum(col("c1")), count(lit(1))).collect()

        eventually(Timeout(3.seconds)) {
          assert(executionId.get() >= 0)
          val sparkPlanGraph = spark.sharedState.statusStore.planGraph(executionId.get())
          val collectMetricsDescs =
            sparkPlanGraph.allNodes.filter(_.name == "CollectMetrics").map(_.desc)
          assert(collectMetricsDescs.size == 1)
          assert(collectMetricsDescs.head ==
            "CollectMetrics(observer1) [sum(c1)=5050, count(1)=100]")
        }
      } finally {
        spark.sparkContext.removeSparkListener(executionIdListener)
      }
    }
  }
}
