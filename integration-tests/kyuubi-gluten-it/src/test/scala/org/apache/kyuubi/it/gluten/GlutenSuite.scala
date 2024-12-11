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

package org.apache.kyuubi.it.gluten

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import org.apache.kyuubi.{GlutenSuiteMixin, KyuubiFunSuite}
import org.apache.kyuubi.spark.connector.common.LocalSparkSession.withSparkSession
import org.apache.kyuubi.tags.GlutenTest

@GlutenTest
class GlutenSuite extends KyuubiFunSuite with GlutenSuiteMixin {

  lazy val sparkConf: SparkConf = {
    val glutenConf = new SparkConf().setMaster("local[*]")
      .set("spark.ui.enabled", "false")
    extraConfigs.foreach { case (k, v) => glutenConf.set(k, v) }
    glutenConf
  }

  test("KYUUBI #5467:test gluten select") {
    withSparkSession(SparkSession.builder.config(sparkConf).getOrCreate()) { spark =>
      val result = spark.sql("SELECT 1").head()
      assert(result.get(0) == 1)
    }
  }

  test("KYUUBI #5467: test gluten plan") {
    withSparkSession(SparkSession.builder.config(sparkConf).getOrCreate()) { spark =>
      val plan = spark.sql("explain SELECT 1").head().getString(0)
      assert(plan.contains("VeloxColumnarToRow") && plan.contains("RowToVeloxColumnar"))
    }
  }
}
