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

package org.apache.kyuubi.engine.trino.operation

import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.engine.trino.WithTrinoEngine
import org.apache.kyuubi.operation.HiveJDBCTestHelper

class TrinoCatalogDatabaseOperationSuite extends WithTrinoEngine with HiveJDBCTestHelper {
  override def withKyuubiConf: Map[String, String] = Map(
    ENGINE_TRINO_CONNECTION_CATALOG.key -> "memory",
    ENGINE_OPERATION_CONVERT_CATALOG_DATABASE_ENABLED.key -> "true")

  // use default schema, do not set to 'default', since withSessionHandle strip suffix '/;'
  override protected val schema = ""

  override protected def jdbcUrl: String = getJdbcUrl

  test("trino - set/get catalog") {
    withJdbcStatement() { statement =>
      val catalog = statement.getConnection.getCatalog
      assert(catalog == "memory")
      statement.getConnection.setCatalog("system")
      val changedCatalog = statement.getConnection.getCatalog
      assert(changedCatalog == "system")
    }
  }

  test("trino - set/get database") {
    withJdbcStatement() { statement =>
      statement.execute("CREATE SCHEMA IF NOT EXISTS memory.test_trino_db")
      val schema = statement.getConnection.getSchema
      assert(schema == "default")
      statement.getConnection.setSchema("test_trino_db")
      val changedSchema = statement.getConnection.getSchema
      assert(changedSchema == "test_trino_db")
      statement.execute("DROP SCHEMA memory.test_trino_db")
    }
  }
}
