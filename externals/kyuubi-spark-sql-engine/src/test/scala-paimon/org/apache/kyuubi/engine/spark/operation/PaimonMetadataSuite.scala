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

import java.lang.reflect.{InvocationHandler, Method, Proxy}
import java.sql.ResultSet
import java.util.Collections
import java.util.regex.Pattern

import scala.collection.mutable.ArrayBuffer

import org.apache.paimon.spark.catalog.SupportView
import org.apache.spark.sql.connector.catalog.CatalogPlugin
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import org.apache.kyuubi.Utils
import org.apache.kyuubi.config.KyuubiConf.OPERATION_GET_TABLES_IGNORE_TABLE_PROPERTIES
import org.apache.kyuubi.engine.spark.WithSparkSQLEngine
import org.apache.kyuubi.engine.spark.util.PaimonCatalogUtils
import org.apache.kyuubi.operation.HiveJDBCTestHelper

class PaimonMetadataSuite extends WithSparkSQLEngine with HiveJDBCTestHelper {
  private val root = Utils.createTempDir("paimon-metadata")
  private val table = Seq("paimon", "metadata", "orders", "TABLE", "orders table")
  private val views = Seq(
    Seq("paimon", "metadata", "v_orders", "VIEW", "orders view"),
    Seq("paimon", "metadata", "v_no_comment", "VIEW", ""),
    Seq("paimon", "metadata", "`v.dot`", "VIEW", "quoted view"))

  override protected def jdbcUrl: String = getJdbcUrl

  override def withKyuubiConf: Map[String, String] = Map(
    "spark.master" -> "local[2]",
    "spark.sql.catalogImplementation" -> "in-memory",
    "spark.sql.extensions" -> "org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions",
    "spark.sql.catalog.paimon" -> "org.apache.paimon.spark.SparkCatalog",
    "spark.sql.catalog.paimon.metastore" -> "jdbc",
    "spark.sql.catalog.paimon.uri" -> s"jdbc:sqlite:${root.resolve("catalog.db")}",
    "spark.sql.catalog.paimon.warehouse" -> root.resolve("warehouse").toUri.toString,
    "kyuubi.engine.share.level" -> "USER",
    OPERATION_GET_TABLES_IGNORE_TABLE_PROPERTIES.key -> "false")

  override def beforeAll(): Unit = {
    super.beforeAll()
    withJdbcStatement() { statement =>
      Seq("metadata", "other", "empty").foreach { db =>
        statement.execute(s"CREATE DATABASE paimon.$db")
      }
      statement.execute("CREATE TABLE paimon.metadata.orders (id INT) USING paimon " +
        "COMMENT 'orders table'")
      statement.execute("INSERT INTO paimon.metadata.orders VALUES (1), (2)")
      statement.execute("CREATE VIEW paimon.metadata.v_orders COMMENT 'orders view' " +
        "AS SELECT id FROM paimon.metadata.orders WHERE id = 1")
      statement.execute("CREATE VIEW paimon.metadata.v_no_comment " +
        "AS SELECT id FROM paimon.metadata.orders")
      statement.execute("CREATE VIEW paimon.metadata.`v.dot` COMMENT 'quoted view' " +
        "AS SELECT id FROM paimon.metadata.orders")
      statement.execute("CREATE VIEW paimon.other.v_orders COMMENT 'other namespace' " +
        "AS SELECT id FROM paimon.metadata.orders")
      val result = statement.executeQuery("SELECT id FROM paimon.metadata.v_orders")
      try {
        assert(result.next())
        assert(result.getInt(1) == 1)
        assert(!result.next())
      } finally {
        result.close()
      }
    }
  }

  override def afterAll(): Unit = {
    try super.afterAll()
    finally Utils.deleteDirectoryRecursively(root.toFile)
  }

  private def readRows(result: ResultSet): Seq[Seq[String]] = {
    val rows = ArrayBuffer.empty[Seq[String]]
    try {
      while (result.next()) {
        rows += Seq("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE", "REMARKS")
          .map(result.getString)
      }
    } finally {
      result.close()
    }
    rows.toSeq.sortBy(_.mkString("|"))
  }

  private def checkTables(
      types: Array[String],
      expected: Seq[Seq[String]],
      schema: String = "metadata",
      pattern: String = "%"): Unit = {
    withJdbcStatement() { statement =>
      val actual = readRows(statement.getConnection.getMetaData
        .getTables("paimon", schema, pattern, types))
      assert(actual === expected.sortBy(_.mkString("|")))
    }
  }

  test("list Paimon tables and persistent views with comments") {
    checkTables(Array("TABLE", "VIEW"), table +: views)
  }

  test("table-only requests exclude persistent views") {
    checkTables(Array("TABLE"), Seq(table))
  }

  test("view-only requests exclude tables") {
    checkTables(Array("VIEW"), views)
  }

  test("null table types include persistent views") {
    checkTables(null, table +: views)
  }

  test("table types are case insensitive") {
    checkTables(Array("view"), views)
  }

  test("view names use JDBC patterns and escaped underscores") {
    checkTables(Array("VIEW"), Seq(views.head), pattern = "v\\_orders")
  }

  test("quoted view names retain their identifier quoting") {
    checkTables(Array("VIEW"), Seq(views.last), pattern = "%dot%")
  }

  test("view identifiers retain the matched namespace") {
    checkTables(
      Array("VIEW"),
      Seq(
        Seq("paimon", "other", "v_orders", "VIEW", "other namespace")),
      schema = "other")
  }

  test("empty and unmatched namespaces return no views") {
    checkTables(Array("VIEW"), Seq.empty, schema = "empty")
    checkTables(Array("VIEW"), Seq.empty, schema = "missing")
  }

  test("identities-only mode retains the existing table-only fast path") {
    withSessionConf()(Map(OPERATION_GET_TABLES_IGNORE_TABLE_PROPERTIES.key -> "true"))() {
      checkTables(Array("VIEW"), Seq(table.updated(4, "")))
    }
  }

  test("catalogs without Paimon SupportView are not inspected for views") {
    val catalog = new CatalogPlugin {
      override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = ()
      override def name(): String = "plain"
    }
    assert(PaimonCatalogUtils.getViews(catalog, Array("metadata"), Pattern.compile(".*")).isEmpty)
  }

  private def failingCatalog(listFailure: Boolean): CatalogPlugin = {
    Proxy.newProxyInstance(
      classOf[SupportView].getClassLoader,
      Array(classOf[CatalogPlugin], classOf[SupportView]),
      new InvocationHandler {
        override def invoke(proxy: AnyRef, method: Method, arguments: Array[AnyRef]): AnyRef = {
          method.getName match {
            case "listViews" if listFailure => throw new IllegalStateException("list views failed")
            case "listViews" => Collections.singletonList("broken_view")
            case "loadView" => throw new IllegalStateException("load view failed")
            case other => throw new UnsupportedOperationException(other)
          }
        }
      }).asInstanceOf[CatalogPlugin]
  }

  test("view listing failures preserve their original cause") {
    val failure = intercept[IllegalStateException] {
      PaimonCatalogUtils.getViews(failingCatalog(true), Array("metadata"), Pattern.compile(".*"))
    }
    assert(failure.getMessage == "list views failed")
  }

  test("unmatched views are not loaded and matched load failures are propagated") {
    val catalog = failingCatalog(false)
    assert(PaimonCatalogUtils.getViews(
      catalog,
      Array("metadata"),
      Pattern.compile("other")).isEmpty)
    val failure = intercept[IllegalStateException] {
      PaimonCatalogUtils.getViews(catalog, Array("metadata"), Pattern.compile(".*"))
    }
    assert(failure.getMessage == "load view failed")
  }
}
