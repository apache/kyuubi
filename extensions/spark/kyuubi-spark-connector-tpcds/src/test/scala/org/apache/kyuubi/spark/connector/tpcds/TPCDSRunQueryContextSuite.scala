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
package org.apache.kyuubi.spark.connector.tpcds

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.spark.connector.common.LocalSparkSession.withSparkSession

class TPCDSRunQueryContextSuite extends KyuubiFunSuite {

  test("test TPCDSRunQueryContext") {
    assert(TPCDSRunQueryContext.querySQLs.size == 103)

    val sparkConf = new SparkConf().setMaster("local[*]")
      .set("spark.ui.enabled", "false")
      .set("spark.sql.catalogImplementation", "in-memory")
      .set("spark.sql.catalog.tpcds", classOf[TPCDSCatalog].getName)
    withSparkSession(SparkSession.builder.config(sparkConf).getOrCreate()) { _ =>
      val queryContext = TPCDSRunQueryContext("tpcds.tiny")
      val query1 = queryContext.runQuery("q1").collect()
      assert(query1.length == 1)
      assert(query1.head.getString(0) == "q1")
      assert(query1.head.getString(1) == "SUCCESS")
      assert(query1.head.getInt(2) > 0)
      assert(query1.head.getString(3) == null)

      val query2 = queryContext.runQueries("q1", "q2").collect()
      assert(query2.length == 2)
      assert(query2.map(_.getString(0)) === Seq("q1", "q2"))
      query2.foreach { row =>
        assert(row.getString(1) == "SUCCESS")
        assert(row.getInt(2) > 0)
        assert(row.getString(3) == null)
      }
    }
  }

}
