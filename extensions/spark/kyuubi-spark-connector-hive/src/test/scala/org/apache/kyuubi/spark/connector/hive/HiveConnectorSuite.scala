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

package org.apache.kyuubi.spark.connector.hive

import org.apache.spark.sql.AnalysisException

class HiveConnectorSuite extends KyuubiHiveTest {

  def withTempTable(table: String)(f: => Unit): Unit = {
    withSparkSession() { spark =>
      spark.sql(
        s"""
           | CREATE TABLE IF NOT EXISTS
           | $table (id String, date String)
           | USING PARQUET
           | PARTITIONED BY (date)
           |""".stripMargin).collect()

      try f
      finally spark.sql(s"DROP TABLE IF EXISTS $table")
    }
  }

  test("A simple query flow.") {
    withSparkSession() { spark =>
      val table = "hive.default.employee"
      withTempTable(table) {
        // can query an existing Hive table in three sections
        val result = spark.sql(
          s"""
             | SELECT * FROM $table
             |""".stripMargin)
        assert(result.collect().isEmpty)

        // error msg should contains catalog info if table is not exist
        val e = intercept[AnalysisException] {
          spark.sql(
            s"""
               | SELECT * FROM hive.ns1.tb1
               |""".stripMargin)
        }
        assert(e.getMessage().contains("Table or view not found: hive.ns1.tb1"))
      }
    }
  }
}
