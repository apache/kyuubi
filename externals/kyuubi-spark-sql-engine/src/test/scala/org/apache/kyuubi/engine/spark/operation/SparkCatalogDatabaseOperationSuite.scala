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

import java.sql.SQLException
import java.util
import java.util.regex.Pattern

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.catalyst.analysis.{NoSuchNamespaceException, NoSuchTableException}
import org.apache.spark.sql.connector.catalog.{CatalogPlugin, Identifier, NamespaceChange, SupportsNamespaces, Table, TableCapability, TableCatalog, TableChange}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import org.apache.kyuubi.config.KyuubiConf.{ENGINE_OPERATION_CONVERT_CATALOG_DATABASE_ENABLED, OPERATION_GET_TABLES_IGNORE_TABLE_PROPERTIES}
import org.apache.kyuubi.engine.spark.WithSparkSQLEngine
import org.apache.kyuubi.engine.spark.util.{PaimonCatalogUtils, SparkCatalogUtils}
import org.apache.kyuubi.operation.HiveJDBCTestHelper

class SparkCatalogDatabaseOperationSuite extends WithSparkSQLEngine with HiveJDBCTestHelper {

  override protected def jdbcUrl: String = getJdbcUrl

  override def withKyuubiConf: Map[String, String] =
    Map(
      "spark.sql.catalog.dummy" -> classOf[DummyCatalog].getName,
      "spark.sql.catalog.plain" -> classOf[MetadataTestCatalog].getName,
      "spark.sql.catalog.unloadable" -> classOf[MetadataTestCatalog].getName,
      "spark.sql.catalog.unloadable.fail-load-table" -> "true",
      ENGINE_OPERATION_CONVERT_CATALOG_DATABASE_ENABLED.key -> "true",
      OPERATION_GET_TABLES_IGNORE_TABLE_PROPERTIES.key -> "false")

  private val table = Seq("plain", "default", "orders", "TABLE", "orders table")

  private def checkTables(
      types: Array[String],
      expected: Seq[Seq[String]],
      catalog: String = "plain",
      pattern: String = "%"): Unit = {
    withJdbcStatement() { statement =>
      val result = statement.getConnection.getMetaData.getTables(catalog, "default", pattern, types)
      val actual = ArrayBuffer.empty[Seq[String]]
      try {
        while (result.next()) {
          actual += Seq("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE", "REMARKS")
            .map(result.getString)
        }
      } finally {
        result.close()
      }
      assert(actual.toSeq === expected)
    }
  }

  test("ordinary V2 catalogs return table identities and comments for matching types") {
    Seq(Array("TABLE"), Array("TABLE", "VIEW"), Array("table")).foreach { types =>
      checkTables(types, Seq(table))
    }
  }

  test("view-only requests exclude tables in ordinary V2 catalogs") {
    checkTables(Array("VIEW"), Seq.empty)
  }

  test("unsupported table types return no tables in ordinary V2 catalogs") {
    checkTables(Array("SYSTEM TABLE"), Seq.empty)
  }

  test("null and empty table types include tables in ordinary V2 catalogs") {
    checkTables(null, Seq(table))
    checkTables(Array.empty[String], Seq(table))
  }

  test("ordinary V2 catalog loading failures are propagated when properties are requested") {
    val failure = intercept[SQLException] {
      checkTables(Array("TABLE"), Seq.empty, catalog = "unloadable")
    }
    assert(failure.getMessage.contains("Table metadata must not be loaded"))
  }

  test("ordinary V2 identities-only mode ignores types without loading table properties") {
    withSessionConf()(Map(OPERATION_GET_TABLES_IGNORE_TABLE_PROPERTIES.key -> "true"))() {
      Seq(Array("TABLE"), Array("VIEW"), Array("SYSTEM TABLE"), null).foreach { types =>
        checkTables(types, Seq(table.updated(0, "unloadable").updated(4, "")), "unloadable")
      }
    }
  }

  test("ordinary V2 identities-only mode still applies table name patterns") {
    withSessionConf()(Map(OPERATION_GET_TABLES_IGNORE_TABLE_PROPERTIES.key -> "true"))() {
      checkTables(Array("VIEW"), Seq.empty, "unloadable", pattern = "missing%")
    }
  }

  test("catalogs without optional Paimon classes have no Paimon views") {
    assert(PaimonCatalogUtils.getViews(
      new DummyCatalog,
      Array("default"),
      Pattern.compile(".*")).isEmpty)
  }

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

class MetadataTestCatalog extends TableCatalog with SupportsNamespaces {
  private var catalogName: String = _
  private var failLoadTable: Boolean = false

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    catalogName = name
    failLoadTable = java.lang.Boolean.parseBoolean(options.get("fail-load-table"))
  }

  override def name(): String = catalogName

  override def defaultNamespace(): Array[String] = Array("default")

  private def checkNamespace(namespace: Array[String]): Unit = {
    if (!namespace.sameElements(defaultNamespace())) {
      throw new NoSuchNamespaceException(namespace)
    }
  }

  override def listNamespaces(): Array[Array[String]] = Array(defaultNamespace())

  override def listNamespaces(namespace: Array[String]): Array[Array[String]] = {
    if (namespace.isEmpty) listNamespaces()
    else {
      checkNamespace(namespace)
      Array.empty[Array[String]]
    }
  }

  override def loadNamespaceMetadata(namespace: Array[String]): util.Map[String, String] = {
    checkNamespace(namespace)
    util.Collections.emptyMap[String, String]()
  }

  override def listTables(namespace: Array[String]): Array[Identifier] = {
    checkNamespace(namespace)
    Array(Identifier.of(namespace, "orders"))
  }

  override def loadTable(ident: Identifier): Table = {
    if (failLoadTable) throw new IllegalStateException("Table metadata must not be loaded")
    checkNamespace(ident.namespace())
    if (ident.name() != "orders") throw new NoSuchTableException(ident)
    new Table {
      override def name(): String = s"$catalogName.default.orders"
      override def schema(): StructType = new StructType().add("id", "int")
      override def capabilities(): util.Set[TableCapability] = util.Collections.emptySet()
      override def properties(): util.Map[String, String] =
        util.Collections.singletonMap(TableCatalog.PROP_COMMENT, "orders table")
    }
  }

  override def createTable(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: util.Map[String, String]): Table = throw new UnsupportedOperationException

  override def alterTable(ident: Identifier, changes: TableChange*): Table =
    throw new UnsupportedOperationException

  override def dropTable(ident: Identifier): Boolean = throw new UnsupportedOperationException

  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit =
    throw new UnsupportedOperationException

  override def createNamespace(namespace: Array[String], metadata: util.Map[String, String]): Unit =
    throw new UnsupportedOperationException

  override def alterNamespace(namespace: Array[String], changes: NamespaceChange*): Unit =
    throw new UnsupportedOperationException

  override def dropNamespace(namespace: Array[String], cascade: Boolean): Boolean =
    throw new UnsupportedOperationException
}
