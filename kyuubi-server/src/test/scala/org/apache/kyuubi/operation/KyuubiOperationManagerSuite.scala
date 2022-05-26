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

import java.sql.SQLTimeoutException

import org.apache.kyuubi.WithKyuubiServer
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._

class KyuubiOperationManagerSuite extends WithKyuubiServer with HiveJDBCTestHelper {
  override protected val conf: KyuubiConf = {
    KyuubiConf().set(OPERATION_QUERY_TIMEOUT.key, "PT1S")
  }

  test(OPERATION_QUERY_TIMEOUT.key + " initialize") {
    val kyuubiConf: KyuubiConf = KyuubiConf()
    val mgr1 = new KyuubiOperationManager()
    mgr1.initialize(kyuubiConf)
    assert(mgr1.getConf.get(OPERATION_QUERY_TIMEOUT).isEmpty)

    val mgr2 = new KyuubiOperationManager()
    mgr2.initialize(kyuubiConf.set(OPERATION_QUERY_TIMEOUT, 1000000L))
    assert(mgr2.getConf.get(OPERATION_QUERY_TIMEOUT) === Some(1000000L))

    val mgr3 = new KyuubiOperationManager()
    intercept[IllegalArgumentException] {
      mgr3.initialize(kyuubiConf.set(OPERATION_QUERY_TIMEOUT.key, "10000A"))
    }

    val mgr4 = new KyuubiOperationManager()
    val conf4 = kyuubiConf.set(OPERATION_QUERY_TIMEOUT, -1000000L)
    intercept[IllegalArgumentException] { mgr4.initialize(conf4) }
  }

  test("query time out shall respect server-side first") {
    withJdbcStatement() { statement =>
      Range(-1, 20, 5).foreach { clientTimeout =>
        statement.setQueryTimeout(clientTimeout)
        val e = intercept[SQLTimeoutException] {
          statement.executeQuery("select java_method('java.lang.Thread', 'sleep', 10000L)")
        }.getMessage
        assert(e.contains("Query timed out after"))
      }
    }
  }

  test("set/get current catalog") {
    withSessionConf()(
      Map(ENGINE_OPERATION_CONVERT_CATALOG_DATABASE_ENABLED.key -> "true"))(
      Map.empty) {
      withJdbcStatement() { statement =>
        val catalog = statement.getConnection.getCatalog
        assert(catalog == "spark_catalog")
        // The server starts the spark engine without other catalogs
        statement.getConnection.setCatalog("dummy_catalog")
        val changedCatalog = statement.getConnection.getCatalog
        assert(changedCatalog == "spark_catalog")
      }
    }
  }

  test("set/get current database") {
    withSessionConf()(
      Map(ENGINE_OPERATION_CONVERT_CATALOG_DATABASE_ENABLED.key -> "true"))(
      Map.empty) {
      withDatabases("test_database") { statement =>
        statement.execute("create database test_database")
        val schema = statement.getConnection.getSchema
        assert(schema == "default")
        statement.getConnection.setSchema("test_database")
        val changedSchema = statement.getConnection.getSchema
        assert(changedSchema == "test_database")
      }
    }
  }

  override protected def jdbcUrl: String = getJdbcUrl
}
