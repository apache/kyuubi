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

package org.apache.kyuubi.engine.spark.util

import java.sql.{Connection, DriverManager}
import java.util.{Collections, Locale, Map => JMap, Set => JSet}
import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogDatabase, CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.connector.catalog.{DelegatingCatalogExtension, Identifier, Table, TableCapability, TableCatalog}
import org.apache.spark.sql.execution.datasources.v2.V2SessionCatalog
import org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog
import org.apache.spark.sql.types.{IntegerType, StructType}

import org.apache.kyuubi.engine.spark.KyuubiSparkUtil.SPARK_ENGINE_RUNTIME_VERSION
import org.apache.kyuubi.engine.spark.WithSparkSQLEngine

object DummySessionCatalog {
  val extraNamespace: Array[String] = Array("2026_db")
  val unloadableTable = "dummy_unloadable"
  val unreadableSchemaTable = "dummy_unreadable_schema"
  val loadTableCalls = new AtomicInteger()
}

class DummySessionCatalog extends DelegatingCatalogExtension {
  import DummySessionCatalog._

  override def listNamespaces(namespace: Array[String]): Array[Array[String]] = {
    val base = super.listNamespaces(namespace)
    if (namespace.isEmpty) base :+ extraNamespace else base
  }

  override def listTables(namespace: Array[String]): Array[Identifier] = {
    if (namespace.sameElements(extraNamespace)) {
      Array(unloadableTable, unreadableSchemaTable).map(Identifier.of(namespace, _))
    } else {
      super.listTables(namespace)
    }
  }

  override def loadTable(ident: Identifier): Table = {
    loadTableCalls.incrementAndGet()
    ident.name() match {
      case `unloadableTable` => throw new NoClassDefFoundError("MissingStorageHandler")
      case `unreadableSchemaTable` => new UnreadableSchemaTable
      case _ => super.loadTable(ident)
    }
  }
}

// Resolves the schema lazily, as Delta tables do.
class UnreadableSchemaTable extends Table {
  override def name(): String = DummySessionCatalog.unreadableSchemaTable
  override def schema(): StructType = throw new IllegalStateException("unreadable schema")
  override def capabilities(): JSet[TableCapability] = Collections.emptySet()
}

// JDBCTableCatalog does not report table comments.
class CommentedJdbcCatalog extends JDBCTableCatalog {
  override def loadTable(ident: Identifier): Table = new CommentedTable(super.loadTable(ident))
}

class CommentedTable(delegate: Table) extends Table {
  override def name(): String = delegate.name()
  override def schema(): StructType = delegate.schema()
  override def capabilities(): JSet[TableCapability] = delegate.capabilities()
  override def properties(): JMap[String, String] =
    Collections.singletonMap(TableCatalog.PROP_COMMENT, SummariesJdbcCatalog.tableComment)
}

class UnloadableJdbcCatalog extends CommentedJdbcCatalog {
  import SummariesJdbcCatalog._

  override def listTables(namespace: Array[String]): Array[Identifier] =
    Identifier.of(namespace, unloadableTable) +: super.listTables(namespace)

  override def loadTable(ident: Identifier): Table = ident.name() match {
    case `unloadableTable` => throw new NoClassDefFoundError("MissingStorageHandler")
    case _ => super.loadTable(ident)
  }
}

object SummariesJdbcCatalog {
  val catalogImpl = "org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog"
  val url = "jdbc:derby:memory:kyuubi_summaries_catalog;create=true"
  val driver = "org.apache.derby.jdbc.EmbeddedDriver"
  val catalogName = "summaries_catalog"
  val commentedCatalogImpl = "org.apache.kyuubi.engine.spark.util.CommentedJdbcCatalog"
  val unloadableCatalogImpl = "org.apache.kyuubi.engine.spark.util.UnloadableJdbcCatalog"
  val unloadableCatalogName = "unloadable_catalog"
  val schema = "summaries_db"
  val table = "summaries_table"
  val tableComment = "summaries_comment"
  val unloadableTable = "unloadable_table"
}

class SparkCatalogUtilsSuite extends WithSparkSQLEngine {

  override def withKyuubiConf: Map[String, String] = Map(
    "spark.sql.catalog.spark_catalog" ->
      "org.apache.kyuubi.engine.spark.util.DummySessionCatalog")

  test("getSchemas lists DSv2-only namespace") {
    val rows = SparkCatalogUtils.getSchemas(spark, SparkCatalogUtils.SESSION_CATALOG, ".*")
    val schemaNames = rows.map(_.getString(0)).toSet

    assert(schemaNames.contains(DummySessionCatalog.extraNamespace.head))
    assert(!schemaNames.contains(s"`${DummySessionCatalog.extraNamespace.head}`"))
    assert(rows.forall(_.getString(1) == SparkCatalogUtils.SESSION_CATALOG))
  }

  test("getSchemas matches session catalog patterns case-insensitively") {
    val pattern = DummySessionCatalog.extraNamespace.head.toUpperCase(Locale.ROOT)
    val rows = SparkCatalogUtils.getSchemas(spark, SparkCatalogUtils.SESSION_CATALOG, pattern)
    val schemaNames = rows.map(_.getString(0)).toSet
    assert(schemaNames.contains(DummySessionCatalog.extraNamespace.head))
  }

  test("getSchemas matches the global temp database case-insensitively") {
    val rows = SparkCatalogUtils.getSchemas(spark, SparkCatalogUtils.SESSION_CATALOG, ".*")
    assert(rows.map(_.getString(0)).toSet.contains("global_temp"))

    val upper = SparkCatalogUtils.getSchemas(
      spark,
      SparkCatalogUtils.SESSION_CATALOG,
      "GLOBAL_TEMP")
    assert(upper.map(_.getString(0)).toSet == Set("global_temp"))
  }

  test("getTables and getColumns tolerate tables that fail to load") {
    import DummySessionCatalog._

    val regularTable = "dummy_regular"
    spark.sql(
      s"CREATE TABLE IF NOT EXISTS default.$regularTable(key INT) USING parquet COMMENT 'regular'")
    try {
      val tableRows = SparkCatalogUtils.getCatalogTablesOrViews(
        spark,
        SparkCatalogUtils.SESSION_CATALOG,
        ".*",
        "dummy_.*",
        Set("TABLE"))
      assert(tableRows.map(r => r.getString(2) -> r.getString(4)).toSet ==
        Set(regularTable -> "regular", unloadableTable -> "", unreadableSchemaTable -> ""))

      val columnRows = SparkCatalogUtils.getColumns(
        spark,
        SparkCatalogUtils.SESSION_CATALOG,
        ".*",
        "dummy_.*",
        ".*")
      assert(columnRows.map(r => r.getString(2) -> r.getString(3)) == Seq(regularTable -> "key"))

      // Filtering by tableTypes skips loadTable for entries whose type does not match.
      loadTableCalls.set(0)
      val viewRows = SparkCatalogUtils.getCatalogTablesOrViews(
        spark,
        SparkCatalogUtils.SESSION_CATALOG,
        extraNamespace.head,
        "dummy_.*",
        Set("VIEW"))
      assert(viewRows.isEmpty)
      assert(loadTableCalls.get() == 0)
    } finally {
      spark.sql(s"DROP TABLE IF EXISTS default.$regularTable")
    }
  }

  test("toV1Pattern rewrites converted regexes back to v1 catalog patterns") {
    assert(SparkCatalogUtils.toV1Pattern(".*") === "*")
    assert(SparkCatalogUtils.toV1Pattern("prefix.*") === "prefix*")
    assert(SparkCatalogUtils.toV1Pattern("v") === "v")
  }

  test("implementsListTableSummaries is false without an own implementation") {
    val catalog = SparkCatalogUtils
      .getCatalog(spark, SparkCatalogUtils.SESSION_CATALOG)
      .asInstanceOf[TableCatalog]
    assert(catalog.isInstanceOf[DummySessionCatalog])
    assert(!SparkCatalogUtils.implementsListTableSummaries(catalog))

    // V2SessionCatalog relies on the interface default via a scalac forwarder (SPARK-52109, 4.1+),
    // which the probe must not mistake for an own implementation.
    assert(!SparkCatalogUtils.implementsListTableSummaries(
      new V2SessionCatalog(spark.sessionState.catalog)))
  }
}

class SummariesCatalogUtilsSuite extends WithSparkSQLEngine {
  import SummariesJdbcCatalog._

  override def withKyuubiConf: Map[String, String] = Map(
    "spark.sql.catalog.spark_catalog" -> commentedCatalogImpl,
    "spark.sql.catalog.spark_catalog.url" -> url,
    "spark.sql.catalog.spark_catalog.driver" -> driver,
    s"spark.sql.catalog.$catalogName" -> catalogImpl,
    s"spark.sql.catalog.$catalogName.url" -> url,
    s"spark.sql.catalog.$catalogName.driver" -> driver,
    s"spark.sql.catalog.$unloadableCatalogName" -> unloadableCatalogImpl,
    s"spark.sql.catalog.$unloadableCatalogName.url" -> url,
    s"spark.sql.catalog.$unloadableCatalogName.driver" -> driver)

  private var derbyConnection: Connection = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    Class.forName(driver)
    derbyConnection = DriverManager.getConnection(url)
    val statement = derbyConnection.createStatement()
    try {
      statement.execute(s"""CREATE SCHEMA "$schema"""")
      statement.execute(s"""CREATE TABLE "$schema"."$table" ("id" INT)""")
    } finally {
      statement.close()
    }
  }

  override def afterAll(): Unit = {
    if (derbyConnection != null) {
      derbyConnection.close()
      derbyConnection = null
    }
    super.afterAll()
  }

  test("implementsListTableSummaries is true with an own implementation") {
    val catalog = SparkCatalogUtils.getCatalog(spark, catalogName).asInstanceOf[TableCatalog]
    assert(catalog.isInstanceOf[JDBCTableCatalog])
    if (SPARK_ENGINE_RUNTIME_VERSION >= "4.1") {
      // SPARK-52109 (4.1.0): JDBCTableCatalog implements listTableSummaries itself.
      assert(SparkCatalogUtils.implementsListTableSummaries(catalog))
    } else {
      assert(!SparkCatalogUtils.implementsListTableSummaries(catalog))
    }
  }

  test("getTables honors tableTypes with ignoreTableProperties") {
    Seq(SparkCatalogUtils.SESSION_CATALOG, catalogName).foreach { catalog =>
      val tableRows = SparkCatalogUtils.getCatalogTablesOrViews(
        spark,
        catalog,
        schema,
        ".*",
        Set("TABLE"),
        ignoreTableProperties = true)
      assert(tableRows.forall(_.getString(0) == catalog))
      assert(tableRows.forall(_.getString(1) == schema))
      assert(tableRows.map(_.getString(2)).toSet == Set(table))
      assert(tableRows.forall(_.getString(3) == "TABLE"))
      assert(tableRows.forall(_.getString(4) == ""))

      val viewRows = SparkCatalogUtils.getCatalogTablesOrViews(
        spark,
        catalog,
        schema,
        ".*",
        Set("VIEW"),
        ignoreTableProperties = true)
      if (SPARK_ENGINE_RUNTIME_VERSION >= "4.1") {
        assert(viewRows.isEmpty)
      } else {
        assert(viewRows.map(_.getString(2)).toSet == Set(table))
        assert(viewRows.forall(_.getString(3) == "TABLE"))
      }
    }
  }

  test("getTables skips v1 lookup for standalone non-CatalogExtension session catalog") {
    val sessionCatalog = spark.sessionState.catalog
    val v1Database =
      CatalogDatabase(schema, "", sessionCatalog.getDefaultDBPath(schema), Map.empty)
    val v1View = CatalogTable(
      identifier = TableIdentifier(table, Some(schema)),
      tableType = CatalogTableType.VIEW,
      storage = CatalogStorageFormat.empty,
      schema = new StructType().add("id", IntegerType),
      viewText = Some("SELECT 1 AS id"),
      comment = Some("v1_view_comment"))
    try {
      sessionCatalog.createDatabase(v1Database, ignoreIfExists = true)
      sessionCatalog.createTable(v1View, ignoreIfExists = true, validateLocation = false)
      // A same-named v1 view in the metastore must not shadow the standalone catalog's table.
      val rows = SparkCatalogUtils.getCatalogTablesOrViews(
        spark,
        SparkCatalogUtils.SESSION_CATALOG,
        schema,
        ".*",
        Set("TABLE", "VIEW"),
        ignoreTableProperties = false)
      assert(rows.map(_.getString(1)) == Seq(schema))
      assert(rows.map(_.getString(2)) == Seq(table))
      assert(rows.map(_.getString(3)) == Seq("TABLE"))
      assert(rows.map(_.getString(4)) == Seq(tableComment))

      val viewRows = SparkCatalogUtils.getCatalogTablesOrViews(
        spark,
        SparkCatalogUtils.SESSION_CATALOG,
        schema,
        ".*",
        Set("VIEW"),
        ignoreTableProperties = false)
      assert(viewRows.isEmpty)
    } finally {
      sessionCatalog.dropDatabase(schema, ignoreIfNotExists = true, cascade = true)
    }
  }

  test("getTempViews and getColumns omit temp views when another catalog is requested") {
    val localView = "other_catalog_local_view"
    val globalView = "other_catalog_global_view"
    try {
      spark.sql(s"CREATE OR REPLACE TEMPORARY VIEW $localView AS SELECT 1 AS a")
      spark.sql(s"CREATE OR REPLACE GLOBAL TEMP VIEW $globalView AS SELECT 1 AS b")
      Seq(null, "", SparkCatalogUtils.SESSION_CATALOG, "SPARK_CATALOG").foreach { requested =>
        val globalRows =
          SparkCatalogUtils.getTempViews(spark, requested, "global_temp", globalView)
        assert(globalRows.map(r => (r.getString(0), r.getString(1), r.getString(2))) ==
          Seq((SparkCatalogUtils.SESSION_CATALOG, "global_temp", globalView)))

        val localRows = SparkCatalogUtils.getTempViews(spark, requested, "default", localView)
        assert(localRows.map(r => (r.getString(0), r.getString(1), r.getString(2))) ==
          Seq((SparkCatalogUtils.SESSION_CATALOG, null, localView)))
      }

      assert(SparkCatalogUtils.getTempViews(spark, catalogName, "global_temp", globalView).isEmpty)
      assert(SparkCatalogUtils.getTempViews(spark, catalogName, "default", localView).isEmpty)

      assert(SparkCatalogUtils
        .getColumns(spark, catalogName, "global_temp", globalView, ".*").isEmpty)
      assert(SparkCatalogUtils
        .getColumns(spark, catalogName, "default", localView, ".*").isEmpty)

      val globalColumns = SparkCatalogUtils
        .getColumns(spark, SparkCatalogUtils.SESSION_CATALOG, "global_temp", globalView, ".*")
      assert(globalColumns.map(r => (r.getString(1), r.getString(2), r.getString(3))) ==
        Seq(("global_temp", globalView, "b")))

      val localColumns = SparkCatalogUtils
        .getColumns(spark, SparkCatalogUtils.SESSION_CATALOG, "default", localView, ".*")
      assert(localColumns.map(r => (r.getString(1), r.getString(2), r.getString(3))) ==
        Seq((null, localView, "a")))
    } finally {
      spark.sql(s"DROP VIEW IF EXISTS $localView")
      spark.sql(s"DROP VIEW IF EXISTS global_temp.$globalView")
    }
  }

  test("getTables and getColumns tolerate tables that fail to load in a non-session catalog") {
    val tableRows = SparkCatalogUtils.getCatalogTablesOrViews(
      spark,
      unloadableCatalogName,
      schema,
      ".*",
      Set("TABLE"))
    assert(tableRows.map(r => r.getString(2) -> r.getString(4)).toSet ==
      Set(unloadableTable -> "", table -> tableComment))

    val columnRows =
      SparkCatalogUtils.getColumns(spark, unloadableCatalogName, schema, ".*", ".*")
    assert(columnRows.map(r => r.getString(2) -> r.getString(3)) == Seq(table -> "id"))
  }
}
