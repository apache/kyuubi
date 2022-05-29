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
package org.apache.kyuubi.engine.jdbc.doris

import org.apache.kyuubi.operation.HiveJDBCTestHelper
import org.apache.kyuubi.operation.meta.ResultSetSchemaConstant.{TABLE_NAME, TABLE_SCHEMA}

class DorisOperationSuite extends WithDorisEngine with HiveJDBCTestHelper {

  test("doris - get tables") {
    withJdbcStatement() { statement =>
      statement.execute("create database if not exists db1")
      statement.execute("use db1")
      statement.execute("create table db1.test1(id bigint)" +
        "ENGINE=OLAP\n" +
        "DISTRIBUTED BY HASH(`id`) BUCKETS 32\n" +
        "PROPERTIES (\n\"replication_num\" = \"1\")")

      val meta = statement.getConnection.getMetaData
      val table1 = meta.getTables(null, "db1", "test1", null)
      table1.next()
      val tableSchema1 = table1.getString(TABLE_SCHEMA)
      val tableName1 = table1.getString(TABLE_NAME)
      assert(tableSchema1 == "db1")
      assert(tableName1 == "test1")
      statement.execute("drop table db1.test1")
      statement.execute("drop database db1")
    }
  }

  override protected def jdbcUrl: String = getJdbcUrl
}
