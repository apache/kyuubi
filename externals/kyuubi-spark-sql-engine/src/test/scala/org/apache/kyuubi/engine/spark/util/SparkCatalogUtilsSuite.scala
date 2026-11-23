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
import java.util.Locale

import org.apache.spark.sql.connector.catalog.{DelegatingCatalogExtension, TableCatalog}
import org.apache.spark.sql.execution.datasources.v2.V2SessionCatalog
import org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog

import org.apache.kyuubi.engine.spark.KyuubiSparkUtil.SPARK_ENGINE_RUNTIME_VERSION
import org.apache.kyuubi.engine.spark.WithSparkSQLEngine

object DummySessionCatalog {
  val extraNamespace: Array[String] = Array("2026_db")
}

class DummySessionCatalog extends DelegatingCatalogExtension {

  override def listNamespaces(namespace: Array[String]): Array[Array[String]] = {
    val base = super.listNamespaces(namespace)
    if (namespace.isEmpty) base :+ DummySessionCatalog.extraNamespace else base
  }
}

object SummariesJdbcCatalog {
  val catalogImpl = "org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog"
  val url = "jdbc:derby:memory:kyuubi_summaries_catalog;create=true"
  val driver = "org.apache.derby.jdbc.EmbeddedDriver"
  val catalogName = "summaries_catalog"
  val schema = "summaries_db"
  val table = "summaries_table"
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
    "spark.sql.catalog.spark_catalog" -> catalogImpl,
    "spark.sql.catalog.spark_catalog.url" -> url,
    "spark.sql.catalog.spark_catalog.driver" -> driver,
    s"spark.sql.catalog.$catalogName" -> catalogImpl,
    s"spark.sql.catalog.$catalogName.url" -> url,
    s"spark.sql.catalog.$catalogName.driver" -> driver)

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

  test("getTables falls back to per-table loading when the batched v1 lookup fails") {
    val rows = SparkCatalogUtils.getCatalogTablesOrViews(
      spark,
      SparkCatalogUtils.SESSION_CATALOG,
      schema,
      ".*",
      Set("TABLE"),
      ignoreTableProperties = false)
    assert(rows.forall(_.getString(1) == schema))
    assert(rows.map(_.getString(2)).toSet == Set(table))
    assert(rows.forall(_.getString(3) == "TABLE"))
    assert(rows.forall(_.getString(4) == ""))
  }
}
