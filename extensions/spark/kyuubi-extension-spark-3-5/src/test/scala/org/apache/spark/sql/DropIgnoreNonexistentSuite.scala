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

import org.apache.spark.sql.catalyst.plans.logical.{DropNamespace, DropTable, NoopCommand}
import org.apache.spark.sql.execution.command._

import org.apache.kyuubi.sql.KyuubiSQLConf

class DropIgnoreNonexistentSuite extends KyuubiSparkSQLExtensionTest {

  test("drop ignore nonexistent") {
    withSQLConf(KyuubiSQLConf.DROP_IGNORE_NONEXISTENT.key -> "true") {
      // drop nonexistent database
      val df1 = sql("DROP DATABASE nonexistent_database")
      assert(df1.queryExecution.analyzed.asInstanceOf[DropNamespace].ifExists == true)

      // drop nonexistent table
      val df2 = sql("DROP TABLE nonexistent_table")
      assert(df2.queryExecution.analyzed.asInstanceOf[DropTable].ifExists == true)

      // drop nonexistent view
      val df3 = sql("DROP VIEW nonexistent_view")
      assert(df3.queryExecution.analyzed.asInstanceOf[DropTableCommand].isView == true &&
        df3.queryExecution.analyzed.asInstanceOf[DropTableCommand].ifExists == true)

      // drop nonexistent function
      val df4 = sql("DROP FUNCTION nonexistent_function")
      assert(df4.queryExecution.analyzed.isInstanceOf[NoopCommand])

      // drop nonexistent temporary function
      val df5 = sql("DROP TEMPORARY FUNCTION nonexistent_temp_function")
      assert(df5.queryExecution.analyzed.asInstanceOf[DropFunctionCommand].ifExists == true)

      // drop nonexistent PARTITION
      withTable("test") {
        sql("CREATE TABLE IF NOT EXISTS test(i int) PARTITIONED BY (p int)")
        val df5 = sql("ALTER TABLE test DROP PARTITION (p = 1)")
        assert(df5.queryExecution.analyzed
          .asInstanceOf[AlterTableDropPartitionCommand].ifExists == true)
      }
    }
  }
}
