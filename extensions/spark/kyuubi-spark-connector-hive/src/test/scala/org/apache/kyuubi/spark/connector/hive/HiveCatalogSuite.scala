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

package org.apache.kyuubi.spark.connector.hive

import java.net.URI
import java.util
import java.util.Collections

import scala.collection.JavaConverters._
import scala.util.Try

import com.google.common.collect.Maps
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.{NoSuchNamespaceException, NoSuchTableException, TableAlreadyExistsException}
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.execution.datasources.v2.orc.OrcScan
import org.apache.spark.sql.hive.kyuubi.connector.HiveBridgeHelper._
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import org.apache.kyuubi.spark.connector.hive.HiveTableCatalog.IdentifierHelper
import org.apache.kyuubi.spark.connector.hive.KyuubiHiveConnectorConf.READ_CONVERT_METASTORE_ORC
import org.apache.kyuubi.spark.connector.hive.read.HiveScan

class HiveCatalogSuite extends KyuubiHiveTest {

  val emptyProps: util.Map[String, String] = Collections.emptyMap[String, String]
  val schema: StructType = new StructType()
    .add("id", IntegerType)
    .add("data", StringType)

  val testNs: Array[String] = Array("db")
  val defaultNs: Array[String] = Array("default")
  val testIdent: Identifier = Identifier.of(testNs, "test_table")

  var catalog: HiveTableCatalog = _

  private def newCatalog(): HiveTableCatalog = {
    val catalog = new HiveTableCatalog
    val catalogName = "hive"
    val properties = Maps.newHashMap[String, String]()
    properties.put("javax.jdo.option.ConnectionURL", "jdbc:derby:memory:memorydb;create=true")
    properties.put("javax.jdo.option.ConnectionDriverName", "org.apache.derby.jdbc.EmbeddedDriver")
    catalog.initialize(catalogName, new CaseInsensitiveStringMap(properties))
    catalog
  }

  private def filterV2TableProperties(
      properties: util.Map[String, String]): Map[String, String] = {
    properties.asScala.filter(kv => !TABLE_RESERVED_PROPERTIES.contains(kv._1))
      .filter(!_._1.startsWith(TableCatalog.OPTION_PREFIX)).toMap
  }

  def makeQualifiedPathWithWarehouse(path: String): URI = {
    val p = new Path(catalog.conf.warehousePath, path)
    val fs = p.getFileSystem(catalog.hadoopConfiguration())
    fs.makeQualified(p).toUri
  }

  private def checkMetadata(expected: Map[String, String], actual: Map[String, String]): Unit = {
    // remove location and comment that are automatically added by HMS unless they are expected
    val toRemove =
      NAMESPACE_RESERVED_PROPERTIES.filter(expected.contains)
    assert(expected -- toRemove === actual)
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    catalog = newCatalog()
    catalog.createNamespace(Array("ns"), emptyProps)
    catalog.createNamespace(Array("ns2"), emptyProps)
    catalog.createNamespace(Array("db"), emptyProps)
  }

  override def afterEach(): Unit = {
    super.afterEach()
    catalog.dropNamespace(Array("ns"), true)
    catalog.dropNamespace(Array("ns2"), true)
    catalog.dropNamespace(Array("db"), true)
    catalog = null
  }

  test("get catalog name") {
    withSparkSession() { _ =>
      val catalog = new HiveTableCatalog
      val catalogName = "hive"
      catalog.initialize(catalogName, CaseInsensitiveStringMap.empty())
      assert(catalog.name() == catalogName)
    }
  }

  test("supports namespaces") {
    withSparkSession() { spark =>
      try {
        spark.sql("USE hive")
        assert(Try { spark.sql("CREATE NAMESPACE IF NOT EXISTS snns1") }.isSuccess)
      } finally {
        spark.sql("DROP NAMESPACE IF EXISTS snns1")
      }
    }
  }

  test("nonexistent table") {
    withSparkSession() { spark =>
      val exception = intercept[AnalysisException] {
        spark.table("hive.ns1.nonexistent_table")
      }
      assert(exception.message.contains("[TABLE_OR_VIEW_NOT_FOUND] " +
        "The table or view `hive`.`ns1`.`nonexistent_table` cannot be found.")
        || exception.message.contains("Table or view not found: hive.ns1.nonexistent_table"))
    }
  }

  test("listTables") {

    val ident1 = Identifier.of(Array("ns"), "test_table_1")
    val ident2 = Identifier.of(Array("ns"), "test_table_2")
    val ident3 = Identifier.of(Array("ns2"), "test_table_1")

    assert(catalog.listTables(Array("ns")).isEmpty)

    catalog.createTable(ident1, schema, Array.empty[Transform], emptyProps)

    assert(catalog.listTables(Array("ns")).toSet == Set(ident1))
    assert(catalog.listTables(Array("ns2")).isEmpty)

    catalog.createTable(ident3, schema, Array.empty[Transform], emptyProps)
    catalog.createTable(ident2, schema, Array.empty[Transform], emptyProps)

    assert(catalog.listTables(Array("ns")).toSet == Set(ident1, ident2))
    assert(catalog.listTables(Array("ns2")).toSet == Set(ident3))

    catalog.dropTable(ident1)

    assert(catalog.listTables(Array("ns")).toSet == Set(ident2))

    catalog.dropTable(ident2)

    assert(catalog.listTables(Array("ns")).isEmpty)
    assert(catalog.listTables(Array("ns2")).toSet == Set(ident3))

    catalog.dropTable(ident3)
  }

  test("createTable") {
    assert(!catalog.tableExists(testIdent))

    val table =
      catalog.createTable(testIdent, schema, Array.empty[Transform], emptyProps)

    val parsed = CatalystSqlParser.parseMultipartIdentifier(table.name)
    assert(parsed == Seq("db", "test_table"))
    assert(table.schema == schema)
    assert(filterV2TableProperties(table.properties) == Map())

    assert(catalog.tableExists(testIdent))
    catalog.dropTable(testIdent)
  }

  test("createTable: with properties") {
    val properties = new util.HashMap[String, String]()
    properties.put("property", "value")

    assert(!catalog.tableExists(testIdent))

    val table = catalog.createTable(testIdent, schema, Array.empty[Transform], properties)

    val parsed = CatalystSqlParser.parseMultipartIdentifier(table.name)
    assert(parsed == Seq("db", "test_table"))
    assert(table.schema == schema)
    assert(filterV2TableProperties(table.properties).asJava == properties)

    assert(catalog.tableExists(testIdent))
    catalog.dropTable(testIdent)
  }

  test("createTable: table already exists") {
    assert(!catalog.tableExists(testIdent))

    val table = catalog.createTable(testIdent, schema, Array.empty[Transform], emptyProps)

    val exc = intercept[TableAlreadyExistsException] {
      catalog.createTable(testIdent, schema, Array.empty[Transform], emptyProps)
    }

    assert(exc.message.contains(testIdent.name()))
    assert(exc.message.contains("already exists"))

    assert(catalog.tableExists(testIdent))
    catalog.dropTable(testIdent)
  }

  test("tableExists") {
    assert(!catalog.tableExists(testIdent))

    catalog.createTable(testIdent, schema, Array.empty[Transform], emptyProps)

    assert(catalog.tableExists(testIdent))

    catalog.dropTable(testIdent)

    assert(!catalog.tableExists(testIdent))
  }

  test("createTable: location") {
    val properties = new util.HashMap[String, String]()
    properties.put(TableCatalog.PROP_PROVIDER, "parquet")
    assert(!catalog.tableExists(testIdent))

    // default location
    val t1 = catalog.createTable(
      testIdent,
      schema,
      Array.empty[Transform],
      properties).asInstanceOf[HiveTable]
    assert(t1.catalogTable.location ===
      catalog.catalog.defaultTablePath(testIdent.asTableIdentifier))
    catalog.dropTable(testIdent)

    // relative path
    properties.put(TableCatalog.PROP_LOCATION, "relative/path")
    val t2 = catalog.createTable(
      testIdent,
      schema,
      Array.empty[Transform],
      properties).asInstanceOf[HiveTable]
    assert(t2.catalogTable.location === makeQualifiedPathWithWarehouse("db.db/relative/path"))
    catalog.dropTable(testIdent)

    // absolute path without scheme
    properties.put(TableCatalog.PROP_LOCATION, "/absolute/path")
    val t3 = catalog.createTable(
      testIdent,
      schema,
      Array.empty[Transform],
      properties).asInstanceOf[HiveTable]
    assert(t3.catalogTable.location.toString === "file:/absolute/path")
    catalog.dropTable(testIdent)

    // absolute path with scheme
    properties.put(TableCatalog.PROP_LOCATION, "file:/absolute/path")
    val t4 = catalog.createTable(
      testIdent,
      schema,
      Array.empty[Transform],
      properties).asInstanceOf[HiveTable]
    assert(t4.catalogTable.location.toString === "file:/absolute/path")
    catalog.dropTable(testIdent)
  }

  test("loadTable") {
    val table = catalog.createTable(testIdent, schema, Array.empty[Transform], emptyProps)
    val loaded = catalog.loadTable(testIdent)

    assert(table.name == loaded.name)
    assert(table.schema == loaded.schema)
    assert(table.properties == loaded.properties)
    catalog.dropTable(testIdent)
  }

  test("loadTable: table does not exist") {
    intercept[NoSuchTableException] {
      catalog.loadTable(testIdent)
    }
  }

  test("invalidateTable") {
    val table = catalog.createTable(testIdent, schema, Array.empty[Transform], emptyProps)
    // Hive v2 don't cache table
    catalog.invalidateTable(testIdent)

    val loaded = catalog.loadTable(testIdent)

    assert(table.name == loaded.name)
    assert(table.schema == loaded.schema)
    assert(table.properties == loaded.properties)
    catalog.dropTable(testIdent)
  }

  test("listNamespaces: fail if missing namespace") {
    catalog.dropNamespace(testNs, true)
    assert(catalog.namespaceExists(testNs) === false)

    val exc = intercept[NoSuchNamespaceException] {
      assert(catalog.listNamespaces(testNs) === Array())
    }

    assert(exc.getMessage.contains(testNs.quoted))
    assert(catalog.namespaceExists(testNs) === false)
  }

  test("loadNamespaceMetadata: fail missing namespace") {
    catalog.dropNamespace(testNs, true)
    val exc = intercept[NoSuchNamespaceException] {
      catalog.loadNamespaceMetadata(testNs)
    }

    assert(exc.getMessage.contains(testNs.quoted))
  }

  test("loadNamespaceMetadata: non-empty metadata") {
    catalog.dropNamespace(testNs, true)
    assert(catalog.namespaceExists(testNs) === false)

    catalog.createNamespace(testNs, Map("property" -> "value").asJava)

    val metadata = catalog.loadNamespaceMetadata(testNs)

    assert(catalog.namespaceExists(testNs) === true)
    checkMetadata(metadata.asScala.toMap, Map("property" -> "value"))

    catalog.dropNamespace(testNs, cascade = false)
  }

  test("loadNamespaceMetadata: empty metadata") {
    catalog.dropNamespace(testNs, true)
    assert(catalog.namespaceExists(testNs) === false)

    catalog.createNamespace(testNs, emptyProps)

    val metadata = catalog.loadNamespaceMetadata(testNs)

    assert(catalog.namespaceExists(testNs) === true)
    checkMetadata(metadata.asScala.toMap, emptyProps.asScala.toMap)

    catalog.dropNamespace(testNs, cascade = false)
  }

  test("Support Parquet/Orc provider is splitable") {
    val parquet_table = Identifier.of(testNs, "parquet_table")
    val parProps: util.Map[String, String] = new util.HashMap[String, String]()
    parProps.put(TableCatalog.PROP_PROVIDER, "parquet")
    val pt = catalog.createTable(parquet_table, schema, Array.empty[Transform], parProps)
    val parScan = pt.asInstanceOf[HiveTable]
      .newScanBuilder(CaseInsensitiveStringMap.empty()).build().asInstanceOf[HiveScan]
    assert(parScan.isSplitable(new Path("empty")))

    val orc_table = Identifier.of(testNs, "orc_table")
    val orcProps: util.Map[String, String] = new util.HashMap[String, String]()
    orcProps.put(TableCatalog.PROP_PROVIDER, "orc")
    val ot = catalog.createTable(orc_table, schema, Array.empty[Transform], orcProps)

    val orcScan = ot.asInstanceOf[HiveTable]
      .newScanBuilder(CaseInsensitiveStringMap.empty()).build().asInstanceOf[OrcScan]
    assert(orcScan.isSplitable(new Path("empty")))

    withSparkSession(Map(READ_CONVERT_METASTORE_ORC.key -> "false")) {
      _ =>
        val orcScan = ot.asInstanceOf[HiveTable]
          .newScanBuilder(CaseInsensitiveStringMap.empty()).build().asInstanceOf[HiveScan]
        assert(orcScan.isSplitable(new Path("empty")))
    }

  }
}
