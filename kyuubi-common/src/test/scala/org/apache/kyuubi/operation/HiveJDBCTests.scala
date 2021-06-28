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

package org.apache.kyuubi.operation

import org.apache.kyuubi.Utils
import org.apache.kyuubi.operation.meta.ResultSetSchemaConstant._

trait HiveJDBCTests extends BasicJDBCTests {

  test("get tables - hive catalog") {
    val table_test = "table_1_test"
    val table_external_test = "table_2_test"
    val view_test = "view_1_test"
    val view_global_test = "view_2_test"
    val tables = Seq(table_test, table_external_test, view_test, view_global_test)
    val schemas = Seq("default", "default", "default", "global_temp")
    val tableTypes = Seq("TABLE", "TABLE", "VIEW", "VIEW")
    withJdbcStatement(view_test, view_global_test, table_test, view_test) { statement =>
      statement.execute(
        s"CREATE TABLE IF NOT EXISTS $table_test(key int) USING parquet COMMENT '$table_test'")
      val loc = Utils.createTempDir()
      statement.execute(s"CREATE EXTERNAL TABLE IF NOT EXISTS $table_external_test(key int)" +
        s" COMMENT '$table_external_test' LOCATION '$loc'")
      statement.execute(s"CREATE VIEW IF NOT EXISTS $view_test COMMENT '$view_test'" +
        s" AS SELECT * FROM $table_test")
      statement.execute(s"CREATE GLOBAL TEMP VIEW $view_global_test" +
        s" COMMENT '$view_global_test' AS SELECT * FROM $table_test")

      val metaData = statement.getConnection.getMetaData
      val rs1 = metaData.getTables(null, null, null, null)
      var i = 0
      while(rs1.next()) {
        val catalogName = rs1.getString(TABLE_CAT)
        assert(catalogName === "spark_catalog" || catalogName === null)
        assert(rs1.getString(TABLE_SCHEM) === schemas(i))
        assert(rs1.getString(TABLE_NAME) == tables(i))
        assert(rs1.getString(TABLE_TYPE) == tableTypes(i))
        assert(rs1.getString(REMARKS) === tables(i).replace(view_global_test, ""))
        i += 1
      }
      assert(i === 4)

      val rs2 = metaData.getTables(null, null, null, Array("VIEW"))
      i = 2
      while(rs2.next()) {
        assert(rs2.getString(TABLE_NAME) == tables(i))
        i += 1
      }
      assert(i === 4)

      val rs3 = metaData.getTables(null, "*", "*", Array("VIEW"))
      i = 2
      while(rs3.next()) {
        assert(rs3.getString(TABLE_NAME) == tables(i))
        i += 1
      }
      assert(i === 4)

      val rs4 = metaData.getTables(null, null, "table%", Array("VIEW"))
      assert(!rs4.next())

      val rs5 = metaData.getTables(null, "*", "table%", Array("VIEW"))
      assert(!rs5.next())

      val rs6 = metaData.getTables(null, null, "table%", Array("TABLE"))
      i = 0
      while(rs6.next()) {
        assert(rs6.getString(TABLE_NAME) == tables(i))
        i += 1
      }
      assert(i === 2)

      val rs7 = metaData.getTables(null, "default", "%", Array("VIEW"))
      i = 2
      while(rs7.next()) {
        assert(rs7.getString(TABLE_NAME) == view_test)
      }
    }
  }
}
