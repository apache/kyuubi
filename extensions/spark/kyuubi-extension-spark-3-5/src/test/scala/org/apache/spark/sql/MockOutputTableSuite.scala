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

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand
import org.apache.spark.sql.hive.execution.CreateHiveTableAsSelectCommand

import org.apache.kyuubi.sql.KyuubiSQLConf.MOCK_OUTPUT_TABLE

class MockOutputTableSuite extends KyuubiSparkSQLExtensionTest {

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    setupData()
  }

  test("test mock output table") {
    withTable("table1", "table2", "table3") {
      sql("create table table1 (c1 int, c2 string) stored as parquet")
      sql("create table table2 (c1 int, c2 string) stored as parquet")
      sql("create table table3 (c1 int, c2 string) stored as parquet")

      def checkOutputTable(plan: LogicalPlan, table: String): Unit = {
        val mockTablePattern = (table + "_mock_\\d+").r
        val checkTableName = (name: String) => mockTablePattern.findFirstIn(name).isDefined
        plan match {
          case InsertIntoHadoopFsRelationCommand(
                outputPath,
                _,
                _,
                _,
                _,
                _,
                _,
                _,
                _,
                catalogTable,
                _,
                _) =>
            assert(checkTableName(outputPath.toString) &&
              checkTableName(catalogTable.get.identifier.table))
          case CreateHiveTableAsSelectCommand(tableDesc, _, _, _) =>
            assert(checkTableName(tableDesc.identifier.table))
          case _ => fail("should not reach here")
        }
      }

      withSQLConf(MOCK_OUTPUT_TABLE.key -> "true") {
        val df1 = sql("insert overwrite table3 " +
          " select a.c1 as c1, b.c2 as c2 from table1 a join table2 b on a.c1 = b.c1")
        checkOutputTable(df1.queryExecution.analyzed, "table3")
        df1.queryExecution.analyzed
        val df2 = sql("create table table4 stored as parquet as select c1, c2 from t1")
        checkOutputTable(df2.queryExecution.analyzed, "table4")
      }
    }
  }

}
