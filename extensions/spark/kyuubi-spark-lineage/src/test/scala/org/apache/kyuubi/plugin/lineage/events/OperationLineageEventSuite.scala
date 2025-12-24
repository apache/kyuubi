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

import java.util.concurrent.{CountDownLatch, TimeUnit}

import org.apache.spark.SparkConf
import org.apache.spark.kyuubi.lineage.LineageConf._
import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent}
import org.apache.spark.sql.SparkListenerExtensionTest

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.events.EventBus
import org.apache.kyuubi.plugin.lineage.Lineage
import org.apache.kyuubi.plugin.lineage.dispatcher.{OperationLineageKyuubiEvent, OperationLineageSparkEvent}

class OperationLineageEventSuite extends KyuubiFunSuite with SparkListenerExtensionTest {

  val catalogName = "org.apache.spark.sql.connector.catalog.InMemoryTableCatalog"

  override protected val catalogImpl: String = "hive"

  override def sparkConf(): SparkConf = {
    super.sparkConf()
      .set("spark.sql.catalog.v2_catalog", catalogName)
      .set(
        "spark.sql.queryExecutionListeners",
        "org.apache.kyuubi.plugin.lineage.SparkOperationLineageQueryExecutionListener")
      .set(DISPATCHERS.key, "SPARK_EVENT,KYUUBI_EVENT")
      .set(SKIP_PARSING_PERMANENT_VIEW_ENABLED.key, "true")
  }

  test("operation lineage event capture: for execute sql") {
    val countDownLatch = new CountDownLatch(2)
    // get lineage from spark event
    var actualSparkEventLineage: Lineage = null
    spark.sparkContext.addSparkListener(new SparkListener {
      override def onOtherEvent(event: SparkListenerEvent): Unit = {
        event match {
          case lineageEvent: OperationLineageSparkEvent =>
            lineageEvent.lineage.foreach {
              case lineage if lineage.inputTables.nonEmpty =>
                actualSparkEventLineage = lineage
                countDownLatch.countDown()
            }
          case _ =>
        }
      }
    })

    // get lineage from kyuubi event
    var actualKyuubiEventLineage: Lineage = null
    EventBus.register[OperationLineageKyuubiEvent] { lineageEvent: OperationLineageKyuubiEvent =>
      lineageEvent.lineage.foreach {
        case lineage if lineage.inputTables.nonEmpty =>
          actualKyuubiEventLineage = lineage
          countDownLatch.countDown()
      }
    }

    withTable("test_table0") { _ =>
      spark.sql("create table test_table0(a string, b string)")
      spark.sql("select a as col0, b as col1 from test_table0").collect()
      val expected = Lineage(
        List(s"$DEFAULT_CATALOG.default.test_table0"),
        List(),
        List(
          ("col0", Set(s"$DEFAULT_CATALOG.default.test_table0.a")),
          ("col1", Set(s"$DEFAULT_CATALOG.default.test_table0.b"))))
      countDownLatch.await(20, TimeUnit.SECONDS)
      assert(actualSparkEventLineage == expected)
      assert(actualKyuubiEventLineage == expected)
    }
  }

  test("operation lineage event capture: for `cache table` sql ") {
    val countDownLatch = new CountDownLatch(1)
    var executionId: Long = -1
    val expected = Lineage(
      List(s"$DEFAULT_CATALOG.default.table1", s"$DEFAULT_CATALOG.default.table0"),
      List(),
      List(
        ("aa", Set(s"$DEFAULT_CATALOG.default.table1.a")),
        ("bb", Set(s"$DEFAULT_CATALOG.default.table0.b"))))

    spark.sparkContext.addSparkListener(new SparkListener {
      override def onOtherEvent(event: SparkListenerEvent): Unit = {
        event match {
          case lineageEvent: OperationLineageSparkEvent
              if executionId == lineageEvent.executionId =>
            lineageEvent.lineage.foreach { lineage =>
              assert(lineage == expected)
              countDownLatch.countDown()
            }
          case _ =>
        }
      }
    })

    withTable("table0", "table1") { _ =>
      val ddls =
        """
          |create table if not exists table0(a int, b string, c string)
          |create table if not exists table1(a int, b string, c string)
          |""".stripMargin
      ddls.split("\n").filter(_.nonEmpty).foreach(spark.sql)
      spark.sql("cache table t0_cached select a as a0, b as b0 from table0 where a = 1 ")
      val sql =
        """
          |select b.a as aa, t0_cached.b0 as bb from t0_cached join table1 b on b.a = t0_cached.a0
          |""".stripMargin
      val r = spark.sql(sql)
      executionId = r.queryExecution.id
      r.collect()
      countDownLatch.await(20, TimeUnit.SECONDS)
      assert(countDownLatch.getCount == 0)
    }
  }

  test("test for skip parsing permanent view") {
    val countDownLatch = new CountDownLatch(1)
    var actual: Lineage = null
    spark.sparkContext.addSparkListener(new SparkListener {
      override def onOtherEvent(event: SparkListenerEvent): Unit = {
        event match {
          case lineageEvent: OperationLineageSparkEvent =>
            lineageEvent.lineage.foreach {
              case lineage if lineage.inputTables.nonEmpty && lineage.outputTables.isEmpty =>
                actual = lineage
                countDownLatch.countDown()
            }
          case _ =>
        }
      }
    })

    withTable("t1") { _ =>
      spark.sql("CREATE TABLE t1 (a string, b string, c string) USING hive")
      spark.sql("CREATE VIEW t2 as select * from t1")
      spark.sql(
        s"select a as k, b" +
          s" from t2" +
          s" where a in ('HELLO') and c = 'HELLO'").collect()

      val expected = Lineage(
        List(s"$DEFAULT_CATALOG.default.t2"),
        List(),
        List(
          ("k", Set(s"$DEFAULT_CATALOG.default.t2.a")),
          ("b", Set(s"$DEFAULT_CATALOG.default.t2.b"))))
      countDownLatch.await(20, TimeUnit.SECONDS)
      assert(actual == expected)
    }
  }

}
