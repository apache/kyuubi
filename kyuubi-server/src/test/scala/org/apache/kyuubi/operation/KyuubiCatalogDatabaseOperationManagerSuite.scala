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

import org.apache.kyuubi.WithKyuubiServer
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._

class KyuubiCatalogDatabaseOperationManagerSuite extends WithKyuubiServer with HiveJDBCTestHelper {
  override protected val conf: KyuubiConf = {
    KyuubiConf().set(OPERATION_QUERY_TIMEOUT.key, "PT1S")
      .set(ENGINE_OPERATION_CONVERT_CATALOG_DATABASE_ENABLED.key, "true")
  }

  test("set/get current catalog") {
    withJdbcStatement() { statement =>
      val catalog = statement.getConnection.getCatalog
      assert(catalog == "spark_catalog")
      // The server starts the spark engine without other catalogs
      statement.getConnection.setCatalog("dummy_catalog")
      val changedCatalog = statement.getConnection.getCatalog
      assert(changedCatalog == "spark_catalog")
    }
  }

  test("set/get current database") {
    withDatabases("test_database") { statement =>
      statement.execute("create database test_database")
      val schema = statement.getConnection.getSchema
      assert(schema == "default")
      statement.getConnection.setSchema("test_database")
      val changedSchema = statement.getConnection.getSchema
      assert(changedSchema == "test_database")
    }
  }

  override protected def jdbcUrl: String = getJdbcUrl
}
