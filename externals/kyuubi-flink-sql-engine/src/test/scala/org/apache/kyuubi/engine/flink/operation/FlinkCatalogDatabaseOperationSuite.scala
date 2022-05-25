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

package org.apache.kyuubi.engine.flink.operation

import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.config.KyuubiConf.OperationModes.NONE
import org.apache.kyuubi.engine.flink.WithFlinkSQLEngine
import org.apache.kyuubi.operation.HiveJDBCTestHelper

class FlinkCatalogDatabaseOperationSuite extends WithFlinkSQLEngine with HiveJDBCTestHelper {
  override def withKyuubiConf: Map[String, String] =
    Map(
      OPERATION_PLAN_ONLY_MODE.key -> NONE.toString,
      ENGINE_OPERATION_CONVERT_CATALOG_DATABASE_ENABLED.key -> "true")

  override protected def jdbcUrl: String =
    s"jdbc:hive2://${engine.frontendServices.head.connectionUrl}/;"

  test("execute statement - set/get catalog") {
    withJdbcStatement()({ statement =>
      statement.executeQuery("create catalog cat_a with ('type'='generic_in_memory')")
      val catalog = statement.getConnection.getCatalog
      assert(catalog == "default_catalog")
      statement.getConnection.setCatalog("cat_a")
      val changedCatalog = statement.getConnection.getCatalog
      assert(changedCatalog == "cat_a")
      assert(statement.execute("drop catalog cat_a"))
    })
  }

  test("execute statement - set/get database") {
    withJdbcStatement()({ statement =>
      statement.executeQuery("create database db_a")
      val schema = statement.getConnection.getSchema
      assert(schema == "default_database")
      statement.getConnection.setSchema("db_a")
      val changedSchema = statement.getConnection.getSchema
      assert(changedSchema == "db_a")
      assert(statement.execute("drop database db_a"))
    })
  }
}
