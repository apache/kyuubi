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

package org.apache.kyuubi.engine.spark.operation

import org.apache.spark.sql.connector.catalog.CatalogPlugin
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import org.apache.kyuubi.config.KyuubiConf.ENGINE_OPERATION_CONVERT_CATALOG_DATABASE_ENABLED
import org.apache.kyuubi.engine.spark.WithSparkSQLEngine
import org.apache.kyuubi.engine.spark.util.SparkCatalogUtils
import org.apache.kyuubi.operation.HiveJDBCTestHelper

class SparkCatalogDatabaseOperationSuite extends WithSparkSQLEngine with HiveJDBCTestHelper {

  override protected def jdbcUrl: String = getJdbcUrl

  override def withKyuubiConf: Map[String, String] =
    Map(
      "spark.sql.catalog.dummy" -> classOf[DummyCatalog].getName,
      ENGINE_OPERATION_CONVERT_CATALOG_DATABASE_ENABLED.key -> "true")

  test("set/get current catalog") {
    withJdbcStatement() { statement =>
      val catalog = statement.getConnection.getCatalog
      assert(catalog == SparkCatalogUtils.SESSION_CATALOG)
      statement.getConnection.setCatalog("dummy")
      val changedCatalog = statement.getConnection.getCatalog
      assert(changedCatalog == "dummy")
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
}

class DummyCatalog extends CatalogPlugin {
  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    _name = name
  }

  private var _name: String = _

  override def name(): String = _name

  override def defaultNamespace(): Array[String] = Array("a", "b")
}
