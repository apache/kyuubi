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

package org.apache.kyuubi.plugin.lineage.events

import java.util.concurrent.CountDownLatch

import org.apache.spark.SparkConf
import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent}
import org.apache.spark.sql.SparkListenerExtensionTest

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.plugin.lineage.helper.SparkListenerHelper.isSparkVersionAtMost

class OperationLineageEventSuite extends KyuubiFunSuite with SparkListenerExtensionTest {

  val catalogName =
    if (isSparkVersionAtMost("3.1")) "org.apache.spark.sql.connector.InMemoryTableCatalog"
    else "org.apache.spark.sql.connector.catalog.InMemoryTableCatalog"

  override protected val catalogImpl: String = "hive"

  override def sparkConf(): SparkConf = {
    super.sparkConf()
      .set("spark.sql.catalog.v2_catalog", catalogName)
      .set(
        "spark.sql.queryExecutionListeners",
        "org.apache.kyuubi.plugin.lineage.SparkOperationLineageQueryExecutionListener")
  }

  test("operation lineage event capture: for execute sql") {
    val countDownLatch = new CountDownLatch(1)
    var actual: Lineage = null
    spark.sparkContext.removeSparkListener(new SparkListener {
      override def onOtherEvent(event: SparkListenerEvent): Unit = {
        event match {
          case lineageEvent: OperationLineageEvent =>
            lineageEvent.lineage.foreach {
              case lineage if lineage.inputTables.nonEmpty =>
                actual = lineage
                countDownLatch.countDown()
            }
          case _ =>
        }
      }
    })

    withTable("test_table0") { _ =>
      spark.sql("create table test_table0(a string, b string)")
      spark.sql("select a as col0, b as col1 from test_table0").collect()
      val expected = Lineage(
        List("default.test_table0"),
        List(),
        List(
          ("col0", Set("default.test_table0.a")),
          ("col1", Set("default.test_table0.b"))))
      countDownLatch.await()
      assert(actual == expected)
    }
  }

}
