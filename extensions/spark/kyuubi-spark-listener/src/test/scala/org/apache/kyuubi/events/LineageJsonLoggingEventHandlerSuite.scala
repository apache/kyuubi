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

package org.apache.kyuubi.events

import java.nio.file.Paths

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkListenerExtensionTest

import org.apache.kyuubi.{KyuubiFunSuite, SparkListenerSQLConf, Utils}

class LineageJsonLoggingEventHandlerSuite extends KyuubiFunSuite
  with SparkListenerExtensionTest {

  private val logRoot = "file://" + Utils.createTempDir().toString
  private val currentDate = Utils.getDateFromTimestamp(System.currentTimeMillis())

  override protected val catalogImpl: String = "hive"

  override def sparkConf(): SparkConf = {
    super.sparkConf()
      .set(SparkListenerSQLConf.LINEAGE_EVENT_JSON_LOG_PATH.key, logRoot)
      .set("spark.eventLog.enabled", "true")
      .set("spark.eventLog.dir", logRoot)
      .set(
        "spark.sql.queryExecutionListeners",
        "org.apache.kyuubi.listener.SparkOperationLineageQueryExecutionListener")
  }

  test("lineage json logging for execute sql") {
    val statementEventPath = Paths.get(
      logRoot,
      "operation_lineage",
      s"day=$currentDate",
      spark.sparkContext.applicationId + ".json")
    val table = statementEventPath.getParent

    withTable("test_table0") { _ =>
      spark.sql("create table test_table0(a string, b string)")
      spark.sql("select a as col0, b as col1 from test_table0").collect()

      val result = spark.sql(s"select * from `json`.`$table`")
      val expected =
        "WrappedArray(WrappedArray(col0, [\"default.test_table0.a\"]), " +
          "WrappedArray(col1, [\"default.test_table0.b\"]))"

      assert(result.select("lineage")
        .collect().last
        .getStruct(0)
        .getAs("columnLineage")
        .toString == expected)
    }

  }
}
