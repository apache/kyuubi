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

import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent}
import org.apache.spark.sql.execution.ui.SparkListenerSQLAdaptiveExecutionUpdate

import org.apache.kyuubi.sql.KyuubiSQLConf

class InjectResourceProfileSuite extends KyuubiSparkSQLExtensionTest {
  private def checkCustomResourceProfile(sqlString: String, exists: Boolean): Unit = {
    @volatile var lastEvent: SparkListenerSQLAdaptiveExecutionUpdate = null
    val listener = new SparkListener {
      override def onOtherEvent(event: SparkListenerEvent): Unit = {
        event match {
          case e: SparkListenerSQLAdaptiveExecutionUpdate => lastEvent = e
          case _ =>
        }
      }
    }

    spark.sparkContext.addSparkListener(listener)
    try {
      sql(sqlString).collect()
      spark.sparkContext.listenerBus.waitUntilEmpty()
      assert(lastEvent != null)
      var current = lastEvent.sparkPlanInfo
      var shouldStop = false
      while (!shouldStop) {
        if (current.nodeName != "CustomResourceProfile") {
          if (current.children.isEmpty) {
            assert(!exists)
            shouldStop = true
          } else {
            current = current.children.head
          }
        } else {
          assert(exists)
          shouldStop = true
        }
      }
    } finally {
      spark.sparkContext.removeSparkListener(listener)
    }
  }

  test("Inject resource profile") {
    withTable("t") {
      withSQLConf(
        "spark.sql.adaptive.forceApply" -> "true",
        KyuubiSQLConf.FINAL_STAGE_CONFIG_ISOLATION.key -> "true",
        KyuubiSQLConf.FINAL_WRITE_STAGE_RESOURCE_ISOLATION_ENABLED.key -> "true") {

        sql("CREATE TABLE t (c1 int, c2 string) USING PARQUET")

        checkCustomResourceProfile("INSERT INTO TABLE t VALUES(1, 'a')", false)
        checkCustomResourceProfile("SELECT 1", false)
        checkCustomResourceProfile(
          "INSERT INTO TABLE t SELECT /*+ rebalance */ * FROM VALUES(1, 'a')",
          true)
      }
    }
  }
}
