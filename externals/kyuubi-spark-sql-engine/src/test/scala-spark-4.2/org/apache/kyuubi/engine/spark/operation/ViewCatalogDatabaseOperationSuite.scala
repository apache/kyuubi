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

import org.apache.spark.sql.catalyst.analysis.{NoSuchNamespaceException, NoSuchViewException}
import org.apache.spark.sql.connector.catalog.{Identifier, Relation, RelationCatalog, View, ViewCatalog}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import org.apache.kyuubi.config.KyuubiConf.{ENGINE_OPERATION_CONVERT_CATALOG_DATABASE_ENABLED, OPERATION_GET_TABLES_IGNORE_TABLE_PROPERTIES}
import org.apache.kyuubi.engine.spark.WithSparkSQLEngine
import org.apache.kyuubi.engine.spark.util.SparkViewCatalogUtils
import org.apache.kyuubi.operation.HiveJDBCTestHelper

class ViewCatalogDatabaseOperationSuite extends WithSparkSQLEngine with HiveJDBCTestHelper {
  override protected def jdbcUrl: String = getJdbcUrl

  override def withKyuubiConf: Map[String, String] = Map(
    "spark.sql.catalog.relations" -> classOf[MetadataRelationCatalog].getName,
    "spark.sql.catalog.views" -> classOf[MetadataViewOnlyCatalog].getName,
    "spark.sql.catalog.unnamespaced" -> classOf[MetadataUnnamespacedViewCatalog].getName,
    "spark.sql.catalog.rootviews" -> classOf[MetadataRootViewCatalog].getName,
    "spark.sql.catalog.rootchildren" -> classOf[MetadataRootViewCatalog].getName,
    "spark.sql.catalog.rootchildren.with-children" -> "true",
    "spark.sql.catalog.noroot" -> classOf[MetadataRootViewCatalog].getName,
    "spark.sql.catalog.noroot.with-children" -> "true",
    "spark.sql.catalog.noroot.root-exists" -> "false",
    "spark.sql.catalog.norootprobe" -> classOf[MetadataRootViewCatalog].getName,
    "spark.sql.catalog.norootprobe.with-children" -> "true",
    "spark.sql.catalog.norootprobe.fail-root-probe" -> "true",
    "spark.sql.catalog.unloadable" -> classOf[MetadataRelationCatalog].getName,
    "spark.sql.catalog.unloadable.fail-load-view" -> "true",
    "spark.sql.catalog.unlistable" -> classOf[MetadataRelationCatalog].getName,
    "spark.sql.catalog.unlistable.fail-list-views" -> "true",
    "spark.sql.catalog.fast" -> classOf[MetadataRelationCatalog].getName,
    "spark.sql.catalog.fast.fail-load-table" -> "true",
    "spark.sql.catalog.fast.fail-load-view" -> "true",
    "spark.sql.catalog.fast.fail-list-views" -> "true",
    ENGINE_OPERATION_CONVERT_CATALOG_DATABASE_ENABLED.key -> "true",
    OPERATION_GET_TABLES_IGNORE_TABLE_PROPERTIES.key -> "false")

  private val table = Seq("orders", "TABLE", "orders table")
  private val views = Seq(
    Seq("`v.dot`", "VIEW", "quoted view"),
    Seq("v_empty", "VIEW", ""),
    Seq("v_orders", "VIEW", "orders view"))

  private def checkTables(
      types: Array[String],
      expected: Seq[Seq[String]],
      catalog: String = "relations",
      pattern: String = "%",
      schema: String = "default",
      expectedSchema: String = "default"): Unit = {
    withJdbcStatement() { statement =>
      val result = statement.getConnection.getMetaData.getTables(catalog, schema, pattern, types)
      val actual = ArrayBuffer.empty[Seq[String]]
      try {
        while (result.next()) {
          actual += Seq("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE", "REMARKS")
            .map(result.getString)
        }
      } finally {
        result.close()
      }
      assert(actual.toSeq.sortBy(_(2)) === expected.map(Seq(
        catalog,
        expectedSchema) ++ _).sortBy(_(2)))
    }
  }

  test("RelationCatalog returns table and persistent view metadata") {
    checkTables(Array("TABLE", "VIEW"), table +: views)
  }

  test("VIEW-only requests return views without ordinary tables") {
    checkTables(Array("VIEW"), views)
  }

  test("TABLE-only requests do not enumerate views") {
    checkTables(Array("TABLE"), Seq(table), catalog = "unlistable")
  }

  test("null and empty types include tables and views") {
    checkTables(null, table +: views)
    checkTables(Array.empty[String], table +: views)
  }

  test("standard view metadata honors case-insensitive type filters") {
    checkTables(Array("view"), views)
    checkTables(Array("table", "view"), table +: views)
  }

  test("unsupported types do not enumerate views") {
    checkTables(Array("SYSTEM TABLE"), Seq.empty, catalog = "unlistable")
  }

  test("persistent views honor JDBC patterns and quoted identifiers") {
    checkTables(Array("VIEW"), Seq(views.last), pattern = "v\\_orders")
    checkTables(Array("VIEW"), Seq(views.head), pattern = "`v.%`")
  }

  test("view-only catalogs do not require TableCatalog") {
    checkTables(Array("VIEW"), views, catalog = "views")
    checkTables(Array("TABLE"), Seq.empty, catalog = "views")
  }

  test("view-only catalogs without namespace support use the default namespace") {
    checkTables(Array("VIEW"), views, catalog = "unnamespaced", schema = "%", expectedSchema = "")
    checkTables(Array("VIEW"), Seq.empty, catalog = "unnamespaced", schema = "missing%")
  }

  Seq("rootviews", "rootchildren").foreach { catalog =>
    test(s"ViewCatalog with SupportsNamespaces includes root views: $catalog") {
      Seq(Array("VIEW"), Array("TABLE", "VIEW"), null, Array.empty[String]).foreach { types =>
        checkTables(types, views, catalog = catalog, schema = "%", expectedSchema = "")
      }
      checkTables(
        Array("VIEW"),
        Seq(views.last),
        catalog = catalog,
        schema = "",
        pattern = "v\\_orders",
        expectedSchema = "")
    }
  }

  test("root namespaces are probed only when the schema pattern matches") {
    checkTables(Array("VIEW"), Seq.empty, catalog = "norootprobe", schema = "default")
    checkTables(Array("VIEW"), Seq.empty, catalog = "norootprobe", schema = "missing%")
  }

  test("connectors without a root namespace are not asked to list root views") {
    checkTables(Array("VIEW"), Seq.empty, catalog = "noroot", schema = "%")
    checkTables(Array("VIEW"), Seq.empty, catalog = "noroot", schema = "")
  }

  test("TABLE-only and unsupported types do not probe the root namespace") {
    Seq(Array("TABLE"), Array("SYSTEM TABLE")).foreach { types =>
      checkTables(types, Seq.empty, catalog = "norootprobe", schema = "%")
    }
  }

  test("identities-only mode does not probe the root namespace") {
    withSessionConf()(Map(OPERATION_GET_TABLES_IGNORE_TABLE_PROPERTIES.key -> "true"))() {
      checkTables(Array("VIEW"), Seq.empty, catalog = "norootprobe", schema = "%")
    }
  }

  test("unmatched namespaces do not enumerate views") {
    checkTables(Array("VIEW"), Seq.empty, catalog = "unlistable", schema = "missing%")
  }

  test("view name filtering happens before loading metadata") {
    checkTables(Array("VIEW"), Seq.empty, catalog = "unloadable", pattern = "missing%")
  }

  test("view listing and loading errors reach JDBC clients") {
    Seq(
      "unlistable" -> "View listing must not be called",
      "unloadable" -> "View metadata must not be loaded").foreach { case (catalog, message) =>
      val error = intercept[SQLException] {
        checkTables(Array("VIEW"), Seq.empty, catalog = catalog)
      }
      assert(error.getMessage.contains(message))
    }
  }

  test("reflection preserves the original connector failure") {
    val expected = new IllegalStateException("view load failed")
    val catalog = new MetadataViewOnlyCatalog {
      override def loadView(ident: Identifier): View = throw expected
    }
    val actual = intercept[IllegalStateException] {
      SparkViewCatalogUtils.getViews(catalog, Array("default"), Pattern.compile(".*"))
    }
    assert(actual eq expected)
  }

  test("identities-only mode ignores types and skips table and view metadata calls") {
    withSessionConf()(Map(OPERATION_GET_TABLES_IGNORE_TABLE_PROPERTIES.key -> "true"))() {
      Seq(Array("TABLE"), Array("VIEW"), Array("SYSTEM TABLE"), null).foreach { types =>
        checkTables(types, Seq(table.updated(2, "")), catalog = "fast")
      }
      checkTables(Array("VIEW"), Seq.empty, catalog = "fast", pattern = "missing%")
      checkTables(Array("VIEW"), Seq.empty, catalog = "views")
    }
  }
}

trait MetadataTestViews extends ViewCatalog {
  protected def failListViews: Boolean
  protected def failLoadView: Boolean

  override def listViews(namespace: Array[String]): Array[Identifier] = {
    if (failListViews) throw new IllegalStateException("View listing must not be called")
    Array("v_orders", "v_empty", "v.dot").map(Identifier.of(namespace, _))
  }

  override def loadView(ident: Identifier): View = {
    if (failLoadView) throw new IllegalStateException("View metadata must not be loaded")
    val properties = new util.HashMap[String, String]()
    ident.name() match {
      case "v_orders" => properties.put("comment", "orders view")
      case "v.dot" => properties.put("comment", "quoted view")
      case "v_empty" =>
      case _ => throw new NoSuchViewException(ident)
    }
    new View.Builder().withQueryText("SELECT 1 AS id")
      .withSchema(new StructType().add("id", "int")).withProperties(properties).build()
  }

  override def createView(ident: Identifier, view: View): View =
    throw new UnsupportedOperationException
  override def replaceView(ident: Identifier, view: View): View =
    throw new UnsupportedOperationException
  override def dropView(ident: Identifier): Boolean = throw new UnsupportedOperationException
  override def renameView(oldIdent: Identifier, newIdent: Identifier): Unit =
    throw new UnsupportedOperationException
}

class MetadataViewOnlyCatalog extends MetadataTestNamespaceCatalog with MetadataTestViews {
  override protected def failListViews: Boolean =
    catalogOptions != null && catalogOptions.getBoolean("fail-list-views", false)
  override protected def failLoadView: Boolean =
    catalogOptions != null && catalogOptions.getBoolean("fail-load-view", false)
}

class MetadataRelationCatalog extends MetadataTestCatalog
  with RelationCatalog with MetadataTestViews {
  override protected def failListViews: Boolean =
    catalogOptions.getBoolean("fail-list-views", false)
  override protected def failLoadView: Boolean = catalogOptions.getBoolean("fail-load-view", false)

  override def loadRelation(ident: Identifier): Relation = {
    if (ident.name() == "orders") super[MetadataTestCatalog].loadTable(ident) else loadView(ident)
  }
}

class MetadataUnnamespacedViewCatalog extends MetadataTestViews {
  private var catalogName: String = _
  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    catalogName = name
  }
  override def name(): String = catalogName
  override protected def failListViews: Boolean = false
  override protected def failLoadView: Boolean = false
}

class MetadataRootViewCatalog extends MetadataViewOnlyCatalog {
  override def defaultNamespace(): Array[String] = Array.empty[String]

  override def listNamespaces(): Array[Array[String]] = {
    if (catalogOptions.getBoolean("with-children", false)) Array(Array("default"))
    else Array.empty[Array[String]]
  }

  override def listNamespaces(namespace: Array[String]): Array[Array[String]] = {
    checkNamespace(namespace)
    if (namespace.isEmpty) listNamespaces() else Array.empty[Array[String]]
  }

  override protected def checkNamespace(namespace: Array[String]): Unit = {
    val exists = if (namespace.isEmpty) catalogOptions.getBoolean("root-exists", true)
    else namespace.sameElements(Array("default")) &&
    catalogOptions.getBoolean("with-children", false)
    if (!exists) throw new NoSuchNamespaceException(namespace)
  }

  override def namespaceExists(namespace: Array[String]): Boolean = {
    if (namespace.isEmpty && catalogOptions.getBoolean("fail-root-probe", false)) {
      throw new IllegalStateException("Root namespace must not be probed")
    }
    super.namespaceExists(namespace)
  }

  override def listViews(namespace: Array[String]): Array[Identifier] = {
    // Fail if GetTables blindly lists an unsupported root namespace.
    checkNamespace(namespace)
    if (namespace.isEmpty) super.listViews(namespace) else Array.empty[Identifier]
  }
}
