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

package org.apache.kyuubi.plugin.lineage.helper

import scala.reflect.io.File

import org.apache.spark.SparkConf
import org.apache.spark.kyuubi.lineage.{LineageConf, SparkContextHelper}
import org.apache.spark.sql.{DataFrame, SparkListenerExtensionTest, SparkSession, SQLContext}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.sources.{BaseRelation, InsertableRelation, SchemaRelationProvider}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.plugin.lineage.Lineage

abstract class SparkSQLLineageParserHelperSuite extends KyuubiFunSuite
  with SparkListenerExtensionTest {

  def catalogName: String = "org.apache.spark.sql.connector.catalog.InMemoryTableCatalog"

  val DEFAULT_CATALOG = LineageConf.DEFAULT_CATALOG
  override protected val catalogImpl: String = "hive"

  override def sparkConf(): SparkConf = {
    super.sparkConf()
      .set(
        "spark.sql.catalog.v2_catalog",
        catalogName)
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.sql("create database if not exists test_db")
    spark.sql("create database if not exists test_db0")
    spark.sql("create table if not exists test_db0.test_table0" +
      " (key int, value string) using parquet")
    spark.sql("create table if not exists test_db0.test_table_part0" +
      " (key int, value string, pid string) using parquet" +
      "  partitioned by(pid)")
    spark.sql("create table if not exists test_db0.test_table1" +
      " (key int, value string) using parquet")
    spark.sql("create table test_db.test_table_from_dir" +
      " (`a0` string, `b0` string) using parquet")
  }

  override def afterAll(): Unit = {
    Seq(
      "test_db0.test_table0",
      "test_db0.test_table1",
      "test_db0.test_table_part0",
      "test_db.test_table_from_dir").foreach { t =>
      spark.sql(s"drop table if exists $t")
    }
    spark.sql("drop database if exists test_db")
    spark.sql("drop database if exists test_db0")
    spark.stop()
    super.afterAll()
  }

  test("columns lineage extract - AlterViewAsCommand") {
    withView("alterviewascommand", "alterviewascommand1") { _ =>
      spark.sql("create view alterviewascommand as select key from test_db0.test_table0")
      val ret0 =
        extractLineage("alter view alterviewascommand as select key from test_db0.test_table0")
      assert(ret0 == Lineage(
        List(s"$DEFAULT_CATALOG.test_db0.test_table0"),
        List(s"$DEFAULT_CATALOG.default.alterviewascommand"),
        List((
          s"$DEFAULT_CATALOG.default.alterviewascommand.key",
          Set(s"$DEFAULT_CATALOG.test_db0.test_table0.key")))))

      spark.sql("create view alterviewascommand1 as select * from test_db0.test_table0")
      val ret1 =
        extractLineage("alter view alterviewascommand1 as select * from test_db0.test_table0")

      assert(ret1 == Lineage(
        List(s"$DEFAULT_CATALOG.test_db0.test_table0"),
        List(s"$DEFAULT_CATALOG.default.alterviewascommand1"),
        List(
          (
            s"$DEFAULT_CATALOG.default.alterviewascommand1.key",
            Set(s"$DEFAULT_CATALOG.test_db0.test_table0.key")),
          (
            s"$DEFAULT_CATALOG.default.alterviewascommand1.value",
            Set(s"$DEFAULT_CATALOG.test_db0.test_table0.value")))))
    }
  }

  test("columns lineage extract - DataSourceV2Relation") {
    val ddls =
      """
        |create table v2_catalog.db.tbb(col1 string, col2 string, col3 string)
        |""".stripMargin

    ddls.split("\n").filter(_.nonEmpty).foreach(spark.sql(_).collect())
    withView("test_view") { _ =>
      val result = extractLineage(
        "create view test_view(a, b, c) as" +
          " select col1 as a, col2 as b, col3 as c from v2_catalog.db.tbb")
      assert(result == Lineage(
        List("v2_catalog.db.tbb"),
        List(s"$DEFAULT_CATALOG.default.test_view"),
        List(
          (s"$DEFAULT_CATALOG.default.test_view.a", Set("v2_catalog.db.tbb.col1")),
          (s"$DEFAULT_CATALOG.default.test_view.b", Set("v2_catalog.db.tbb.col2")),
          (s"$DEFAULT_CATALOG.default.test_view.c", Set("v2_catalog.db.tbb.col3")))))
    }
  }

  test("columns lineage extract - AppendData/OverwriteByExpression") {
    val ddls =
      """
        |create table v2_catalog.db.tb0(col1 int, col2 string) partitioned by(col2)
        |""".stripMargin
    ddls.split("\n").filter(_.nonEmpty).foreach(spark.sql(_).collect())
    withTable("v2_catalog.db.tb0") { _ =>
      val ret0 =
        extractLineage(
          s"insert into table v2_catalog.db.tb0 " +
            s"select key as col1, value as col2 from test_db0.test_table0")
      assert(ret0 == Lineage(
        List(s"$DEFAULT_CATALOG.test_db0.test_table0"),
        List("v2_catalog.db.tb0"),
        List(
          ("v2_catalog.db.tb0.col1", Set(s"$DEFAULT_CATALOG.test_db0.test_table0.key")),
          ("v2_catalog.db.tb0.col2", Set(s"$DEFAULT_CATALOG.test_db0.test_table0.value")))))

      val ret1 =
        extractLineage(
          s"insert overwrite table v2_catalog.db.tb0 partition(col2) " +
            s"select key as col1, value as col2 from test_db0.test_table0")
      assert(ret1 == Lineage(
        List(s"$DEFAULT_CATALOG.test_db0.test_table0"),
        List("v2_catalog.db.tb0"),
        List(
          ("v2_catalog.db.tb0.col1", Set(s"$DEFAULT_CATALOG.test_db0.test_table0.key")),
          ("v2_catalog.db.tb0.col2", Set(s"$DEFAULT_CATALOG.test_db0.test_table0.value")))))

      val ret2 =
        extractLineage(
          s"insert overwrite table v2_catalog.db.tb0 partition(col2 = 'bb') " +
            s"select key as col1 from test_db0.test_table0")
      assert(ret2 == Lineage(
        List(s"$DEFAULT_CATALOG.test_db0.test_table0"),
        List("v2_catalog.db.tb0"),
        List(
          ("v2_catalog.db.tb0.col1", Set(s"$DEFAULT_CATALOG.test_db0.test_table0.key")),
          ("v2_catalog.db.tb0.col2", Set()))))
    }
  }

  test("columns lineage extract - CreateViewCommand") {
    withView("createviewcommand", "createviewcommand1", "createviewcommand2") { _ =>
      val ret0 = extractLineage(
        "create view createviewcommand(a, b) as select key, value from test_db0.test_table0")
      assert(ret0 == Lineage(
        List(s"$DEFAULT_CATALOG.test_db0.test_table0"),
        List(s"$DEFAULT_CATALOG.default.createviewcommand"),
        List(
          (
            s"$DEFAULT_CATALOG.default.createviewcommand.a",
            Set(s"$DEFAULT_CATALOG.test_db0.test_table0.key")),
          (
            s"$DEFAULT_CATALOG.default.createviewcommand.b",
            Set(s"$DEFAULT_CATALOG.test_db0.test_table0.value")))))

      val ret1 = extractLineage(
        "create view createviewcommand1 as select key, value from test_db0.test_table0")
      assert(ret1 == Lineage(
        List(s"$DEFAULT_CATALOG.test_db0.test_table0"),
        List(s"$DEFAULT_CATALOG.default.createviewcommand1"),
        List(
          (
            s"$DEFAULT_CATALOG.default.createviewcommand1.key",
            Set(s"$DEFAULT_CATALOG.test_db0.test_table0.key")),
          (
            s"$DEFAULT_CATALOG.default.createviewcommand1.value",
            Set(s"$DEFAULT_CATALOG.test_db0.test_table0.value")))))

      val ret2 = extractLineage(
        "create view createviewcommand2 as select * from test_db0.test_table0")
      assert(ret2 == Lineage(
        List(s"$DEFAULT_CATALOG.test_db0.test_table0"),
        List(s"$DEFAULT_CATALOG.default.createviewcommand2"),
        List(
          (
            s"$DEFAULT_CATALOG.default.createviewcommand2.key",
            Set(s"$DEFAULT_CATALOG.test_db0.test_table0.key")),
          (
            s"$DEFAULT_CATALOG.default.createviewcommand2.value",
            Set(s"$DEFAULT_CATALOG.test_db0.test_table0.value")))))
    }
  }

  test("columns lineage extract - CreateDataSourceTableAsSelectCommand") {
    withTable("createdatasourcetableasselectcommand", "createdatasourcetableasselectcommand1") {
      _ =>
        val ret0 =
          extractLineage("create table createdatasourcetableasselectcommand using parquet" +
            " AS SELECT key, value FROM test_db0.test_table0")
        assert(ret0 == Lineage(
          List(s"$DEFAULT_CATALOG.test_db0.test_table0"),
          List(s"$DEFAULT_CATALOG.default.createdatasourcetableasselectcommand"),
          List(
            (
              s"$DEFAULT_CATALOG.default.createdatasourcetableasselectcommand.key",
              Set(s"$DEFAULT_CATALOG.test_db0.test_table0.key")),
            (
              s"$DEFAULT_CATALOG.default.createdatasourcetableasselectcommand.value",
              Set(s"$DEFAULT_CATALOG.test_db0.test_table0.value")))))

        val ret1 =
          extractLineage("create table createdatasourcetableasselectcommand1 using parquet" +
            " AS SELECT * FROM test_db0.test_table0")
        assert(ret1 == Lineage(
          List(s"$DEFAULT_CATALOG.test_db0.test_table0"),
          List(s"$DEFAULT_CATALOG.default.createdatasourcetableasselectcommand1"),
          List(
            (
              s"$DEFAULT_CATALOG.default.createdatasourcetableasselectcommand1.key",
              Set(s"$DEFAULT_CATALOG.test_db0.test_table0.key")),
            (
              s"$DEFAULT_CATALOG.default.createdatasourcetableasselectcommand1.value",
              Set(s"$DEFAULT_CATALOG.test_db0.test_table0.value")))))
    }
  }

  test("columns lineage extract - CreateHiveTableAsSelectCommand") {
    withTable("createhivetableasselectcommand", "createhivetableasselectcommand1") { _ =>
      val ret0 = extractLineage("create table createhivetableasselectcommand using hive" +
        " as select key, value from test_db0.test_table0")
      assert(ret0 == Lineage(
        List(s"$DEFAULT_CATALOG.test_db0.test_table0"),
        List(s"$DEFAULT_CATALOG.default.createhivetableasselectcommand"),
        List(
          (
            s"$DEFAULT_CATALOG.default.createhivetableasselectcommand.key",
            Set(s"$DEFAULT_CATALOG.test_db0.test_table0.key")),
          (
            s"$DEFAULT_CATALOG.default.createhivetableasselectcommand.value",
            Set(s"$DEFAULT_CATALOG.test_db0.test_table0.value")))))

      val ret1 = extractLineage("create table createhivetableasselectcommand1 using hive" +
        " as select * from test_db0.test_table0")
      assert(ret1 == Lineage(
        List(s"$DEFAULT_CATALOG.test_db0.test_table0"),
        List(s"$DEFAULT_CATALOG.default.createhivetableasselectcommand1"),
        List(
          (
            s"$DEFAULT_CATALOG.default.createhivetableasselectcommand1.key",
            Set(s"$DEFAULT_CATALOG.test_db0.test_table0.key")),
          (
            s"$DEFAULT_CATALOG.default.createhivetableasselectcommand1.value",
            Set(s"$DEFAULT_CATALOG.test_db0.test_table0.value")))))
    }
  }

  test("columns lineage extract - OptimizedCreateHiveTableAsSelectCommand") {
    withTable("optimizedcreatehivetableasselectcommand") { _ =>
      val ret =
        extractLineage(
          "create table optimizedcreatehivetableasselectcommand stored as parquet " +
            "as select * from test_db0.test_table0")
      assert(ret == Lineage(
        List(s"$DEFAULT_CATALOG.test_db0.test_table0"),
        List(s"$DEFAULT_CATALOG.default.optimizedcreatehivetableasselectcommand"),
        List(
          (
            s"$DEFAULT_CATALOG.default.optimizedcreatehivetableasselectcommand.key",
            Set(s"$DEFAULT_CATALOG.test_db0.test_table0.key")),
          (
            s"$DEFAULT_CATALOG.default.optimizedcreatehivetableasselectcommand.value",
            Set(s"$DEFAULT_CATALOG.test_db0.test_table0.value")))))
    }
  }

  test("columns lineage extract - CreateTableAsSelect") {
    withTable(
      "v2_catalog.db.createhivetableasselectcommand",
      "v2_catalog.db.createhivetableasselectcommand1") { _ =>
      val ret0 = extractLineage("create table v2_catalog.db.createhivetableasselectcommand" +
        " as select key, value from test_db0.test_table0")
      assert(ret0 == Lineage(
        List(s"$DEFAULT_CATALOG.test_db0.test_table0"),
        List("v2_catalog.db.createhivetableasselectcommand"),
        List(
          (
            "v2_catalog.db.createhivetableasselectcommand.key",
            Set(s"$DEFAULT_CATALOG.test_db0.test_table0.key")),
          (
            "v2_catalog.db.createhivetableasselectcommand.value",
            Set(s"$DEFAULT_CATALOG.test_db0.test_table0.value")))))

      val ret1 = extractLineage("create table v2_catalog.db.createhivetableasselectcommand1" +
        " as select * from test_db0.test_table0")
      assert(ret1 == Lineage(
        List(s"$DEFAULT_CATALOG.test_db0.test_table0"),
        List("v2_catalog.db.createhivetableasselectcommand1"),
        List(
          (
            "v2_catalog.db.createhivetableasselectcommand1.key",
            Set(s"$DEFAULT_CATALOG.test_db0.test_table0.key")),
          (
            "v2_catalog.db.createhivetableasselectcommand1.value",
            Set(s"$DEFAULT_CATALOG.test_db0.test_table0.value")))))
    }
  }

  test("columns lineage extract - InsertIntoDataSourceCommand") {
    val tableName = "insertintodatasourcecommand"
    withTable(tableName) { _ =>
      val schema = new StructType()
        .add("a", IntegerType, nullable = true)
        .add("b", StringType, nullable = true)
      val newTable = CatalogTable(
        identifier = TableIdentifier(tableName, None),
        tableType = CatalogTableType.MANAGED,
        storage = CatalogStorageFormat(
          locationUri = None,
          inputFormat = None,
          outputFormat = None,
          serde = None,
          compressed = false,
          properties = Map.empty),
        schema = schema,
        provider = Some(classOf[SimpleInsertSource].getName))
      spark.sessionState.catalog.createTable(newTable, ignoreIfExists = false)

      val ret0 =
        extractLineage(
          s"insert into table  $tableName select key, value from test_db0.test_table0")
      assert(ret0 == Lineage(
        List(s"$DEFAULT_CATALOG.test_db0.test_table0"),
        List(s"$DEFAULT_CATALOG.default.insertintodatasourcecommand"),
        List(
          (
            s"$DEFAULT_CATALOG.default.insertintodatasourcecommand.a",
            Set(s"$DEFAULT_CATALOG.test_db0.test_table0.key")),
          (
            s"$DEFAULT_CATALOG.default.insertintodatasourcecommand.b",
            Set(s"$DEFAULT_CATALOG.test_db0.test_table0.value")))))

      val ret1 =
        extractLineage(
          s"insert into table  $tableName select * from test_db0.test_table0")
      assert(ret1 == Lineage(
        List(s"$DEFAULT_CATALOG.test_db0.test_table0"),
        List(s"$DEFAULT_CATALOG.default.insertintodatasourcecommand"),
        List(
          (
            s"$DEFAULT_CATALOG.default.insertintodatasourcecommand.a",
            Set(s"$DEFAULT_CATALOG.test_db0.test_table0.key")),
          (
            s"$DEFAULT_CATALOG.default.insertintodatasourcecommand.b",
            Set(s"$DEFAULT_CATALOG.test_db0.test_table0.value")))))

      val ret2 =
        extractLineage(
          s"insert into table  $tableName " +
            s"select (select key from test_db0.test_table1 limit 1) + 1 as aa, " +
            s"value as bb from test_db0.test_table0")
      assert(ret2 == Lineage(
        List(s"$DEFAULT_CATALOG.test_db0.test_table1", s"$DEFAULT_CATALOG.test_db0.test_table0"),
        List(s"$DEFAULT_CATALOG.default.insertintodatasourcecommand"),
        List(
          (
            s"$DEFAULT_CATALOG.default.insertintodatasourcecommand.a",
            Set(s"$DEFAULT_CATALOG.test_db0.test_table1.key")),
          (
            s"$DEFAULT_CATALOG.default.insertintodatasourcecommand.b",
            Set(s"$DEFAULT_CATALOG.test_db0.test_table0.value")))))

    }
  }

  test("columns lineage extract - InsertIntoHadoopFsRelationCommand") {
    val tableName = "insertintohadoopfsrelationcommand"
    withTable(tableName) { _ =>
      spark.sql(s"CREATE TABLE $tableName (a int, b string) USING parquet")
      val ret0 =
        extractLineage(
          s"insert into table $tableName select key, value from test_db0.test_table0")

      assert(ret0 == Lineage(
        List(s"$DEFAULT_CATALOG.test_db0.test_table0"),
        List(s"$DEFAULT_CATALOG.default.insertintohadoopfsrelationcommand"),
        List(
          (
            s"$DEFAULT_CATALOG.default.insertintohadoopfsrelationcommand.a",
            Set(s"$DEFAULT_CATALOG.test_db0.test_table0.key")),
          (
            s"$DEFAULT_CATALOG.default.insertintohadoopfsrelationcommand.b",
            Set(s"$DEFAULT_CATALOG.test_db0.test_table0.value")))))
    }

  }

  test("columns lineage extract - InsertIntoDatasourceDirCommand") {
    val tableDirectory = getClass.getResource("/").getPath + "table_directory"
    val directory = File(tableDirectory).createDirectory()
    val ret0 = extractLineage(s"""
                                 |INSERT OVERWRITE DIRECTORY '${directory.path}'
                                 |USING parquet
                                 |SELECT * FROM test_db0.test_table_part0""".stripMargin)
    assert(ret0 == Lineage(
      List(s"$DEFAULT_CATALOG.test_db0.test_table_part0"),
      List(s"""`${directory.path}`"""),
      List(
        (s"""`${directory.path}`.key""", Set(s"$DEFAULT_CATALOG.test_db0.test_table_part0.key")),
        (
          s"""`${directory.path}`.value""",
          Set(s"$DEFAULT_CATALOG.test_db0.test_table_part0.value")),
        (s"""`${directory.path}`.pid""", Set(s"$DEFAULT_CATALOG.test_db0.test_table_part0.pid")))))
  }

  test("columns lineage extract - InsertIntoHiveDirCommand") {
    val tableDirectory = getClass.getResource("/").getPath + "table_directory"
    val directory = File(tableDirectory).createDirectory()
    val ret0 = extractLineage(s"""
                                 |INSERT OVERWRITE DIRECTORY '${directory.path}'
                                 |USING parquet
                                 |SELECT * FROM test_db0.test_table_part0""".stripMargin)
    assert(ret0 == Lineage(
      List(s"$DEFAULT_CATALOG.test_db0.test_table_part0"),
      List(s"""`${directory.path}`"""),
      List(
        (s"""`${directory.path}`.key""", Set(s"$DEFAULT_CATALOG.test_db0.test_table_part0.key")),
        (
          s"""`${directory.path}`.value""",
          Set(s"$DEFAULT_CATALOG.test_db0.test_table_part0.value")),
        (s"""`${directory.path}`.pid""", Set(s"$DEFAULT_CATALOG.test_db0.test_table_part0.pid")))))
  }

  test("columns lineage extract - InsertIntoHiveTable") {
    val tableName = "insertintohivetable"
    withTable(tableName) { _ =>
      spark.sql(s"CREATE TABLE $tableName (a int, b string) USING hive")
      val ret0 =
        extractLineage(
          s"insert into table $tableName select * from test_db0.test_table0")

      assert(ret0 == Lineage(
        List(s"$DEFAULT_CATALOG.test_db0.test_table0"),
        List(s"$DEFAULT_CATALOG.default.$tableName"),
        List(
          (
            s"$DEFAULT_CATALOG.default.$tableName.a",
            Set(s"$DEFAULT_CATALOG.test_db0.test_table0.key")),
          (
            s"$DEFAULT_CATALOG.default.$tableName.b",
            Set(s"$DEFAULT_CATALOG.test_db0.test_table0.value")))))
    }

  }

  test("columns lineage extract - logical relation sql") {
    val ret0 = extractLineage("select key, value from test_db0.test_table0")
    assert(ret0 == Lineage(
      List(s"$DEFAULT_CATALOG.test_db0.test_table0"),
      List(),
      List(
        ("key", Set(s"$DEFAULT_CATALOG.test_db0.test_table0.key")),
        ("value", Set(s"$DEFAULT_CATALOG.test_db0.test_table0.value")))))

    val ret1 = extractLineage("select * from test_db0.test_table_part0")
    assert(ret1 == Lineage(
      List(s"$DEFAULT_CATALOG.test_db0.test_table_part0"),
      List(),
      List(
        ("key", Set(s"$DEFAULT_CATALOG.test_db0.test_table_part0.key")),
        ("value", Set(s"$DEFAULT_CATALOG.test_db0.test_table_part0.value")),
        ("pid", Set(s"$DEFAULT_CATALOG.test_db0.test_table_part0.pid")))))

  }

  test("columns lineage extract - not generate lineage sql") {
    val ret0 = extractLineage("create table test_table1(a string, b string, c string)")
    assert(ret0 == Lineage(List[String](), List[String](), List[(String, Set[String])]()))
  }

  test("columns lineage extract - data source V2 sql") {
    val ddls =
      """
        |create table v2_catalog.db.tb(col1 string, col2 string, col3 string)
        |""".stripMargin

    ddls.split("\n").filter(_.nonEmpty).foreach(spark.sql(_).collect())
    withTable("v2_catalog.db.tb") { _ =>
      val sql0 = "select col1 from v2_catalog.db.tb"
      val ret0 = extractLineage(sql0)
      assert(ret0 == Lineage(
        List("v2_catalog.db.tb"),
        List(),
        List("col1" -> Set("v2_catalog.db.tb.col1"))))

      val sql1 = "select col1, hash(hash(col1)) as col2 from v2_catalog.db.tb"
      val ret1 = extractLineage(sql1)
      assert(ret1 == Lineage(
        List("v2_catalog.db.tb"),
        List(),
        List("col1" -> Set("v2_catalog.db.tb.col1"), "col2" -> Set("v2_catalog.db.tb.col1"))))

      val sql2 =
        "select col1, case col1 when '1' then 's1' else col1 end col2 from v2_catalog.db.tb"
      val ret2 = extractLineage(sql2)
      assert(ret2 == Lineage(
        List("v2_catalog.db.tb"),
        List(),
        List("col1" -> Set("v2_catalog.db.tb.col1"), "col2" -> Set("v2_catalog.db.tb.col1"))))

      val sql3 =
        "select col1 as col2, 'col2' as col2, 'col2', first(col3) as col2 " +
          "from v2_catalog.db.tb group by col1"
      val ret3 = extractLineage(sql3)
      assert(ret3 == Lineage(
        List("v2_catalog.db.tb"),
        List[String](),
        List(
          ("col2", Set("v2_catalog.db.tb.col1")),
          ("col2", Set[String]()),
          ("col2", Set[String]()),
          ("col2", Set("v2_catalog.db.tb.col3")))))

      val sql4 =
        "select col1 as col2, sum(hash(col1) + hash(hash(col1))) " +
          "from v2_catalog.db.tb group by col1"
      val ret4 = extractLineage(sql4)
      assert(ret4 == Lineage(
        List("v2_catalog.db.tb"),
        List(),
        List(
          ("col2", Set("v2_catalog.db.tb.col1")),
          ("sum((hash(col1) + hash(hash(col1))))", Set("v2_catalog.db.tb.col1")))))
      val sql5 =
        s"""
           | select t1.col2, count(t2.col3)
           | from
           | (select col1 as col2 from v2_catalog.db.tb) t1 join
           | (select col1 as col3 from v2_catalog.db.tb) t2
           |  on t1.col2 = t2.col3
           |  group by 1
           |""".stripMargin
      val ret5 = extractLineage(sql5)
      assert(ret5 == Lineage(
        List("v2_catalog.db.tb"),
        List(),
        List(
          ("col2", Set("v2_catalog.db.tb.col1")),
          ("count(col3)", Set("v2_catalog.db.tb.col1")))))
    }
  }

  test("columns lineage extract - base sql") {
    val ddls =
      List(
        "CREATE TABLE tmp0 AS SELECT * FROM VALUES(1),(2),(3) AS t(tmp0_0)",
        "CREATE TABLE tmp1 AS select c1 as tmp1_0, c2 as tmp1_1,  c3 as tmp1_2," +
          "concat(c1, c2) as tmp1_3 from" +
          " VALUES(1,'a',4),(2, 'b', 4),(3, 'c', 4) AS t(c1, c2, c3)")

    ddls.foreach(spark.sql(_).collect())

    withTable("tmp0", "tmp1") { _ =>
      val sql0 =
        """
          |select tmp0_0 as a0, tmp1_0 as a1 from tmp0 join tmp1 where tmp1_0 = tmp0_0
          |""".stripMargin
      val sql0ExpectResult = Lineage(
        List(s"$DEFAULT_CATALOG.default.tmp0", s"$DEFAULT_CATALOG.default.tmp1"),
        List(),
        List(
          "a0" -> Set(s"$DEFAULT_CATALOG.default.tmp0.tmp0_0"),
          "a1" -> Set(s"$DEFAULT_CATALOG.default.tmp1.tmp1_0")))

      val sql1 =
        """
          |select count(tmp1_0) as cnt, tmp1_1 from tmp1 group by tmp1_1
          |""".stripMargin
      val sql1ExpectResult = Lineage(
        List(s"$DEFAULT_CATALOG.default.tmp1"),
        List(),
        List(
          "cnt" -> Set(s"$DEFAULT_CATALOG.default.tmp1.tmp1_0"),
          "tmp1_1" -> Set(s"$DEFAULT_CATALOG.default.tmp1.tmp1_1")))

      val ret0 = extractLineage(sql0)
      assert(ret0 == sql0ExpectResult)
      val ret1 = extractLineage(sql1)
      assert(ret1 == sql1ExpectResult)
    }
  }

  test("columns lineage extract - CTE sql") {
    val ddls =
      List(
        "create table test_db.goods_detail0(goods_id string, cat_id string)",
        "create table v2_catalog.test_db_v2.goods_detail1" +
          "(goods_id string, cat_id string, product_id string)",
        "create table v2_catalog.test_db_v2.mall_icon_schedule" +
          "(relation_id string, icon_id string," +
          " icon_type string, is_enabled string, start_time date, end_time date)",
        "create table v2_catalog.test_db_v2.mall_icon" +
          "(id string, name string, is_enabled string, start_time date, end_time date)")
    ddls.foreach(spark.sql(_).collect())
    withTable(
      "test_db.goods_detail0",
      "v2_catalog.test_db_v2.goods_detail1",
      "v2_catalog.test_db_v2.mall_icon_schedule",
      "v2_catalog.test_db_v2.mall_icon") { _ =>
      val sql0 =
        """WITH base_goods_detail AS (
          |SELECT goods_id
          |, CASE
          |WHEN cat_id = 1 THEN 'car'
          |ELSE 'other'
          |END AS cate_grory
          |FROM test_db.goods_detail0
          |GROUP BY goods_id, CASE
          |WHEN cat_id = 1 THEN 'car'
          |ELSE 'other'
          |END
          |),
          |goods_cat AS (
          |SELECT t1.goods_id, t1.cat_id, t1.product_id, t2.start_time, t2.end_time
          |FROM v2_catalog.test_db_v2.goods_detail1 t1
          |JOIN (
          |SELECT t1.relation_id, t1.start_time AS start_time,
          |t2.end_time AS end_time, t2.id, t2.is_enabled
          |FROM  v2_catalog.test_db_v2.mall_icon_schedule t1
          |JOIN v2_catalog.test_db_v2.mall_icon t2 ON t1.icon_id = t2.id
          |WHERE t2.name LIKE '%test%'
          |AND t2.is_enabled = 'Y'
          |AND t1.icon_type = 'short'
          |AND t1.is_enabled = 'Y'
          |) t2
          |ON t1.product_id = t2.relation_id
          |),
          |goods_cat_new AS (
          |SELECT t1.goods_id, t1.cate_grory, t2.cat_id, t2.product_id, t2.start_time
          |, t2.end_time
          |FROM base_goods_detail t1
          |JOIN goods_cat t2 ON t1.goods_id = t2.goods_id
          |)
          |SELECT *
          |FROM goods_cat_new
          |LIMIT 10""".stripMargin

      val ret0 = extractLineage(sql0)
      assert(ret0 == Lineage(
        List(
          s"$DEFAULT_CATALOG.test_db.goods_detail0",
          "v2_catalog.test_db_v2.goods_detail1",
          "v2_catalog.test_db_v2.mall_icon_schedule",
          "v2_catalog.test_db_v2.mall_icon"),
        List(),
        List(
          ("goods_id", Set(s"$DEFAULT_CATALOG.test_db.goods_detail0.goods_id")),
          ("cate_grory", Set(s"$DEFAULT_CATALOG.test_db.goods_detail0.cat_id")),
          ("cat_id", Set("v2_catalog.test_db_v2.goods_detail1.cat_id")),
          ("product_id", Set("v2_catalog.test_db_v2.goods_detail1.product_id")),
          ("start_time", Set("v2_catalog.test_db_v2.mall_icon_schedule.start_time")),
          ("end_time", Set("v2_catalog.test_db_v2.mall_icon.end_time")))))
    }
  }

  test("columns lineage extract - join sql ") {
    val ddls =
      """
        |create table v2_catalog.db.tb1(col1 string, col2 string, col3 string)
        |create table v2_catalog.db.tb2(col1 string, col2 string, col3 string)
        |""".stripMargin
    ddls.split("\n").filter(_.nonEmpty).foreach(spark.sql(_).collect())
    withTable("v2_catalog.db.tb1", "v2_catalog.db.tb2") { _ =>
      val sql0 =
        """
          |select t1.col1 as a1, t2.col1 as b1, concat(t1.col1, t2.col1) as ab1,
          |concat(concat(t1.col1, t2.col2), concat(t1.col2, t2.col3)) as ab2
          |from v2_catalog.db.tb1 t1 join v2_catalog.db.tb2 t2
          |on t1.col1 = t2.col1
          |""".stripMargin

      val ret0 = extractLineage(sql0)
      assert(
        ret0 == Lineage(
          List("v2_catalog.db.tb1", "v2_catalog.db.tb2"),
          List(),
          List(
            ("a1", Set("v2_catalog.db.tb1.col1")),
            ("b1", Set("v2_catalog.db.tb2.col1")),
            ("ab1", Set("v2_catalog.db.tb1.col1", "v2_catalog.db.tb2.col1")),
            (
              "ab2",
              Set(
                "v2_catalog.db.tb1.col1",
                "v2_catalog.db.tb1.col2",
                "v2_catalog.db.tb2.col2",
                "v2_catalog.db.tb2.col3")))))
    }
  }

  test("columns lineage extract - union sql") {
    val ddls =
      """
        |create table test_db.test_table0(a int, b string, c string)
        |create table test_db.test_table1(a int, b string, c string)
        |""".stripMargin
    ddls.split("\n").filter(_.nonEmpty).foreach(spark.sql(_).collect())
    withTable("test_db.test_table0", "test_db.test_table1") { _ =>
      val sql0 =
        """
          |select a, b, c from (
          |select a, b, b as c from test_db.test_table0
          |union
          |select a, b, c as c from test_db.test_table1
          |) a
          |""".stripMargin
      val ret0 = extractLineage(sql0)
      assert(ret0 == Lineage(
        List(s"$DEFAULT_CATALOG.test_db.test_table0", s"$DEFAULT_CATALOG.test_db.test_table1"),
        List(),
        List(
          (
            "a",
            Set(
              s"$DEFAULT_CATALOG.test_db.test_table0.a",
              s"$DEFAULT_CATALOG.test_db.test_table1.a")),
          (
            "b",
            Set(
              s"$DEFAULT_CATALOG.test_db.test_table0.b",
              s"$DEFAULT_CATALOG.test_db.test_table1.b")),
          (
            "c",
            Set(
              s"$DEFAULT_CATALOG.test_db.test_table0.b",
              s"$DEFAULT_CATALOG.test_db.test_table1.c")))))
    }
  }

  test("columns lineage extract - agg and union sql") {
    val ddls =
      List(
        "create table test_db.test_order_item(stat_date string, pay_time date," +
          "channel_id string, sub_channel_id string," +
          "user_type string, country_name string," +
          "order_id string, goods_count int, shop_price int, is_valid_order int)",
        "create table test_db.test_p0_order_item(pay_time date," +
          "channel_id string, sub_channel_id string," +
          "user_type string, country_name string, is_valid_item int)")
    ddls.foreach(spark.sql(_).collect())
    withTable("test_db.test_order_item", "test_db.test_p0_order_item") { _ =>
      val sql0 =
        """
          |SELECT stat_date, channel_id, sub_channel_id
          |, user_type, country_name, SUM(get_count) AS get_count0
          |, SUM(get_amount) as get_amount0,
          |cast(unix_timestamp() as bigint) AS add_time
          |FROM (
          |SELECT stat_date, channel_id, sub_channel_id, user_type
          |,country_name, COUNT(DISTINCT order_id) AS get_count
          |, SUM(goods_count * shop_price) AS get_amount
          |FROM test_db.test_order_item
          |WHERE 1 = 1 AND is_valid_order = 1
          |GROUP BY
          |stat_date, channel_id, sub_channel_id, user_type, country_name
          |) a
          |GROUP BY
          |stat_date, channel_id, sub_channel_id, user_type, country_name
          |""".stripMargin
      val ret0 = extractLineage(sql0)
      assert(ret0 == Lineage(
        List(s"$DEFAULT_CATALOG.test_db.test_order_item"),
        List(),
        List(
          ("stat_date", Set(s"$DEFAULT_CATALOG.test_db.test_order_item.stat_date")),
          ("channel_id", Set(s"$DEFAULT_CATALOG.test_db.test_order_item.channel_id")),
          ("sub_channel_id", Set(s"$DEFAULT_CATALOG.test_db.test_order_item.sub_channel_id")),
          ("user_type", Set(s"$DEFAULT_CATALOG.test_db.test_order_item.user_type")),
          ("country_name", Set(s"$DEFAULT_CATALOG.test_db.test_order_item.country_name")),
          ("get_count0", Set(s"$DEFAULT_CATALOG.test_db.test_order_item.order_id")),
          (
            "get_amount0",
            Set(
              s"$DEFAULT_CATALOG.test_db.test_order_item.goods_count",
              s"$DEFAULT_CATALOG.test_db.test_order_item.shop_price")),
          ("add_time", Set[String]()))))
      val sql1 =
        """
          |SELECT channel_id, sub_channel_id, country_name, SUM(get_count) AS get_count0
          |, SUM(get_amount) as get_amount0, cast(unix_timestamp() as bigint) AS add_time
          |FROM (
          |SELECT
          |channel_id, sub_channel_id, country_name, COUNT(DISTINCT order_id) AS get_count,
          |SUM(goods_count * shop_price) AS get_amount
          |FROM test_db.test_order_item
          |WHERE 1 = 1 AND is_valid_order = 1
          |GROUP BY
          |channel_id, sub_channel_id, country_name
          |UNION ALL
          |SELECT
          |channel_id, sub_channel_id, country_name, 0 AS get_count, 0 AS get_amount
          |FROM test_db.test_p0_order_item
          |WHERE 1 = 1
          |AND is_valid_item = 1
          |GROUP BY
          |channel_id, sub_channel_id, country_name
          |) a
          |GROUP BY a.channel_id, a.sub_channel_id, a.country_name
          |""".stripMargin
      val ret1 = extractLineage(sql1)
      assert(ret1 == Lineage(
        List(
          s"$DEFAULT_CATALOG.test_db.test_order_item",
          s"$DEFAULT_CATALOG.test_db.test_p0_order_item"),
        List(),
        List(
          (
            "channel_id",
            Set(
              s"$DEFAULT_CATALOG.test_db.test_order_item.channel_id",
              s"$DEFAULT_CATALOG.test_db.test_p0_order_item.channel_id")),
          (
            "sub_channel_id",
            Set(
              s"$DEFAULT_CATALOG.test_db.test_order_item.sub_channel_id",
              s"$DEFAULT_CATALOG.test_db.test_p0_order_item.sub_channel_id")),
          (
            "country_name",
            Set(
              s"$DEFAULT_CATALOG.test_db.test_order_item.country_name",
              s"$DEFAULT_CATALOG.test_db.test_p0_order_item.country_name")),
          ("get_count0", Set(s"$DEFAULT_CATALOG.test_db.test_order_item.order_id")),
          (
            "get_amount0",
            Set(
              s"$DEFAULT_CATALOG.test_db.test_order_item.goods_count",
              s"$DEFAULT_CATALOG.test_db.test_order_item.shop_price")),
          ("add_time", Set[String]()))))
    }
  }

  test("columns lineage extract - agg sql") {
    val sql0 = """select key as a, count(*) as b, 1 as c from test_db0.test_table0 group by key"""
    val ret0 = extractLineage(sql0)
    assert(ret0 == Lineage(
      List(s"$DEFAULT_CATALOG.test_db0.test_table0"),
      List(),
      List(
        ("a", Set(s"$DEFAULT_CATALOG.test_db0.test_table0.key")),
        ("b", Set(s"$DEFAULT_CATALOG.test_db0.test_table0.__count__")),
        ("c", Set()))))

    val sql1 = """select count(*) as a, 1 as b from test_db0.test_table0"""
    val ret1 = extractLineage(sql1)
    assert(ret1 == Lineage(
      List(s"$DEFAULT_CATALOG.test_db0.test_table0"),
      List(),
      List(
        ("a", Set(s"$DEFAULT_CATALOG.test_db0.test_table0.__count__")),
        ("b", Set()))))

    val sql2 = """select every(key == 1) as a, 1 as b from test_db0.test_table0"""
    val ret2 = extractLineage(sql2)
    assert(ret2 == Lineage(
      List(s"$DEFAULT_CATALOG.test_db0.test_table0"),
      List(),
      List(
        ("a", Set(s"$DEFAULT_CATALOG.test_db0.test_table0.key")),
        ("b", Set()))))

    val sql3 = """select count(*) as a, 1 as b from test_db0.test_table0"""
    val ret3 = extractLineage(sql3)
    assert(ret3 == Lineage(
      List(s"$DEFAULT_CATALOG.test_db0.test_table0"),
      List(),
      List(
        ("a", Set(s"$DEFAULT_CATALOG.test_db0.test_table0.__count__")),
        ("b", Set()))))

    val sql4 = """select first(key) as a, 1 as b from test_db0.test_table0"""
    val ret4 = extractLineage(sql4)
    assert(ret4 == Lineage(
      List(s"$DEFAULT_CATALOG.test_db0.test_table0"),
      List(),
      List(
        ("a", Set(s"$DEFAULT_CATALOG.test_db0.test_table0.key")),
        ("b", Set()))))

    val sql5 = """select avg(key) as a, 1 as b from test_db0.test_table0"""
    val ret5 = extractLineage(sql5)
    assert(ret5 == Lineage(
      List(s"$DEFAULT_CATALOG.test_db0.test_table0"),
      List(),
      List(
        ("a", Set(s"$DEFAULT_CATALOG.test_db0.test_table0.key")),
        ("b", Set()))))

    val sql6 =
      """select count(value) + sum(key) as a,
        | 1 as b from test_db0.test_table0""".stripMargin
    val ret6 = extractLineage(sql6)
    assert(ret6 == Lineage(
      List(s"$DEFAULT_CATALOG.test_db0.test_table0"),
      List(),
      List(
        (
          "a",
          Set(
            s"$DEFAULT_CATALOG.test_db0.test_table0.value",
            s"$DEFAULT_CATALOG.test_db0.test_table0.key")),
        ("b", Set()))))

    val sql7 = """select count(*) + sum(key) as a, 1 as b from test_db0.test_table0"""
    val ret7 = extractLineage(sql7)
    assert(ret7 == Lineage(
      List(s"$DEFAULT_CATALOG.test_db0.test_table0"),
      List(),
      List(
        (
          "a",
          Set(
            s"$DEFAULT_CATALOG.test_db0.test_table0.__count__",
            s"$DEFAULT_CATALOG.test_db0.test_table0.key")),
        ("b", Set()))))

  }

  test("colums lineage extract - catch table") {
    val ddls =
      """
        |create table table0(a int, b string, c string)
        |create table table1(a int, b string, c string)
        |""".stripMargin
    ddls.split("\n").filter(_.nonEmpty).foreach(spark.sql(_).collect())
    withTable("table0", "table1") { _ =>
      spark.sql("cache table t0_cached select a as a0, b as b0 from table0 where a = 1 ")
      val sql0 =
        """
          |select b.a as aa, t0_cached.b0 as bb from t0_cached join table1 b on b.a = t0_cached.a0
          |""".stripMargin
      val ret0 = extractLineage(sql0)
      assert(ret0 == Lineage(
        List(s"$DEFAULT_CATALOG.default.table1", s"$DEFAULT_CATALOG.default.table0"),
        List(),
        List(
          ("aa", Set(s"$DEFAULT_CATALOG.default.table1.a")),
          ("bb", Set(s"$DEFAULT_CATALOG.default.table0.b")))))

      val df0 = spark.sql("select a as a0, b as b0 from table0 where a = 2")
      df0.cache()
      val df1 = spark.sql("select a, b from table1")
      val df = df0.join(df1).select(df0("a0").alias("aa"), df1("b").alias("bb"))
      val analyzed = df.queryExecution.analyzed
      val ret1 = SparkSQLLineageParseHelper(spark).transformToLineage(0, analyzed).get
      assert(ret1 == Lineage(
        List(s"$DEFAULT_CATALOG.default.table0", s"$DEFAULT_CATALOG.default.table1"),
        List(),
        List(
          ("aa", Set(s"$DEFAULT_CATALOG.default.table0.a")),
          ("bb", Set(s"$DEFAULT_CATALOG.default.table1.b")))))
    }
  }

  test("columns lineage extract - subquery sql") {
    val ddls =
      """
        |create table table0(a int, b string, c string)
        |create table table1(a int, b string, c string)
        |""".stripMargin
    ddls.split("\n").filter(_.nonEmpty).foreach(spark.sql(_).collect())
    withTable("table0", "table1") { _ =>
      val sql0 =
        """
          |select a as aa, bb, cc from (select b as bb, c as cc from table1) t0, table0
          |""".stripMargin
      val ret0 = extractLineage(sql0)
      assert(ret0 == Lineage(
        List(s"$DEFAULT_CATALOG.default.table0", s"$DEFAULT_CATALOG.default.table1"),
        List(),
        List(
          ("aa", Set(s"$DEFAULT_CATALOG.default.table0.a")),
          ("bb", Set(s"$DEFAULT_CATALOG.default.table1.b")),
          ("cc", Set(s"$DEFAULT_CATALOG.default.table1.c")))))

      val sql1 =
        """
          |select (select a from table1) as aa, b as bb from table1
          |""".stripMargin
      val ret1 = extractLineage(sql1)
      assert(ret1 == Lineage(
        List(s"$DEFAULT_CATALOG.default.table1"),
        List(),
        List(
          ("aa", Set(s"$DEFAULT_CATALOG.default.table1.a")),
          ("bb", Set(s"$DEFAULT_CATALOG.default.table1.b")))))

      val sql2 =
        """
          |select (select count(*) from table0) as aa, b as bb from table1
          |""".stripMargin
      val ret2 = extractLineage(sql2)
      assert(ret2 == Lineage(
        List(s"$DEFAULT_CATALOG.default.table0", s"$DEFAULT_CATALOG.default.table1"),
        List(),
        List(
          ("aa", Set(s"$DEFAULT_CATALOG.default.table0.__count__")),
          ("bb", Set(s"$DEFAULT_CATALOG.default.table1.b")))))

      // ListQuery
      val sql3 =
        """
          |select * from table0 where table0.a in (select a from table1)
          |""".stripMargin
      val ret3 = extractLineage(sql3)
      assert(ret3 == Lineage(
        List(s"$DEFAULT_CATALOG.default.table0"),
        List(),
        List(
          ("a", Set(s"$DEFAULT_CATALOG.default.table0.a")),
          ("b", Set(s"$DEFAULT_CATALOG.default.table0.b")),
          ("c", Set(s"$DEFAULT_CATALOG.default.table0.c")))))

      // Exists
      val sql4 =
        """
          |select * from table0 where exists (select * from table1 where table0.c = table1.c)
          |""".stripMargin
      val ret4 = extractLineage(sql4)
      assert(ret4 == Lineage(
        List(s"$DEFAULT_CATALOG.default.table0"),
        List(),
        List(
          ("a", Set(s"$DEFAULT_CATALOG.default.table0.a")),
          ("b", Set(s"$DEFAULT_CATALOG.default.table0.b")),
          ("c", Set(s"$DEFAULT_CATALOG.default.table0.c")))))

      val sql5 =
        """
          |select * from table0 where exists (select * from table1 where c = "odone")
          |""".stripMargin
      val ret5 = extractLineage(sql5)
      assert(ret5 == Lineage(
        List(s"$DEFAULT_CATALOG.default.table0"),
        List(),
        List(
          ("a", Set(s"$DEFAULT_CATALOG.default.table0.a")),
          ("b", Set(s"$DEFAULT_CATALOG.default.table0.b")),
          ("c", Set(s"$DEFAULT_CATALOG.default.table0.c")))))

      val sql6 =
        """
          |select * from table0 where not exists (select * from table1 where c = "odone")
          |""".stripMargin
      val ret6 = extractLineage(sql6)
      assert(ret6 == Lineage(
        List(s"$DEFAULT_CATALOG.default.table0"),
        List(),
        List(
          ("a", Set(s"$DEFAULT_CATALOG.default.table0.a")),
          ("b", Set(s"$DEFAULT_CATALOG.default.table0.b")),
          ("c", Set(s"$DEFAULT_CATALOG.default.table0.c")))))

      val sql7 =
        """
          |select * from table0 where table0.a not in (select a from table1)
          |""".stripMargin
      val ret7 = extractLineage(sql7)
      assert(ret7 == Lineage(
        List(s"$DEFAULT_CATALOG.default.table0"),
        List(),
        List(
          ("a", Set(s"$DEFAULT_CATALOG.default.table0.a")),
          ("b", Set(s"$DEFAULT_CATALOG.default.table0.b")),
          ("c", Set(s"$DEFAULT_CATALOG.default.table0.c")))))

      val sql8 =
        """
          |select (select a from table1) + 1, b as bb from table1
          |""".stripMargin
      val ret8 = extractLineage(sql8)
      assert(ret8 == Lineage(
        List(s"$DEFAULT_CATALOG.default.table1"),
        List(),
        List(
          ("(scalarsubquery() + 1)", Set(s"$DEFAULT_CATALOG.default.table1.a")),
          ("bb", Set(s"$DEFAULT_CATALOG.default.table1.b")))))

      val sql9 =
        """
          |select (select a from table1 limit 1) + 1 as aa, b as bb from table1
          |""".stripMargin
      val ret9 = extractLineage(sql9)
      assert(ret9 == Lineage(
        List(s"$DEFAULT_CATALOG.default.table1"),
        List(),
        List(
          ("aa", Set(s"$DEFAULT_CATALOG.default.table1.a")),
          ("bb", Set(s"$DEFAULT_CATALOG.default.table1.b")))))

      val sql10 =
        """
          |select (select a from table1 limit 1) + (select a from table0 limit 1) + 1 as aa,
          | b as bb from table1
          |""".stripMargin
      val ret10 = extractLineage(sql10)
      assert(ret10 == Lineage(
        List(s"$DEFAULT_CATALOG.default.table1", s"$DEFAULT_CATALOG.default.table0"),
        List(),
        List(
          ("aa", Set(s"$DEFAULT_CATALOG.default.table1.a", s"$DEFAULT_CATALOG.default.table0.a")),
          ("bb", Set(s"$DEFAULT_CATALOG.default.table1.b")))))

      val sql11 =
        """
          |select tmp.a, b from (select * from table1) tmp;
          |""".stripMargin

      val ret11 = extractLineage(sql11)
      assert(ret11 == Lineage(
        List(s"$DEFAULT_CATALOG.default.table1"),
        List(),
        List(
          ("a", Set(s"$DEFAULT_CATALOG.default.table1.a")),
          ("b", Set(s"$DEFAULT_CATALOG.default.table1.b")))))

      val sql12 =
        """
          |select (select sum(a) from table0 where table1.b = table0.b) as aa, b from table1
          |""".stripMargin
      val ret12 = extractLineage(sql12)
      assert(ret12 == Lineage(
        List(s"$DEFAULT_CATALOG.default.table0", s"$DEFAULT_CATALOG.default.table1"),
        List(),
        List(
          ("aa", Set(s"$DEFAULT_CATALOG.default.table0.a")),
          ("b", Set(s"$DEFAULT_CATALOG.default.table1.b")))))
    }
  }

  test("test group by") {
    withTable("t1", "t2", "v2_catalog.db.t1", "v2_catalog.db.t2") { _ =>
      spark.sql("CREATE TABLE t1 (a string, b string, c string) USING hive")
      spark.sql("CREATE TABLE t2 (a string, b string, c string) USING hive")
      spark.sql("CREATE TABLE v2_catalog.db.t1 (a string, b string, c string)")
      spark.sql("CREATE TABLE v2_catalog.db.t2 (a string, b string, c string)")
      val ret0 =
        extractLineage(
          s"insert into table t1 select a," +
            s"concat_ws('/', collect_set(b))," +
            s"count(distinct(b)) * count(distinct(c))" +
            s"from t2 group by a")
      assert(ret0 == Lineage(
        List(s"$DEFAULT_CATALOG.default.t2"),
        List(s"$DEFAULT_CATALOG.default.t1"),
        List(
          (s"$DEFAULT_CATALOG.default.t1.a", Set(s"$DEFAULT_CATALOG.default.t2.a")),
          (s"$DEFAULT_CATALOG.default.t1.b", Set(s"$DEFAULT_CATALOG.default.t2.b")),
          (
            s"$DEFAULT_CATALOG.default.t1.c",
            Set(s"$DEFAULT_CATALOG.default.t2.b", s"$DEFAULT_CATALOG.default.t2.c")))))

      val ret1 =
        extractLineage(
          s"insert into table v2_catalog.db.t1 select a," +
            s"concat_ws('/', collect_set(b))," +
            s"count(distinct(b)) * count(distinct(c))" +
            s"from v2_catalog.db.t2 group by a")
      assert(ret1 == Lineage(
        List("v2_catalog.db.t2"),
        List("v2_catalog.db.t1"),
        List(
          ("v2_catalog.db.t1.a", Set("v2_catalog.db.t2.a")),
          ("v2_catalog.db.t1.b", Set("v2_catalog.db.t2.b")),
          ("v2_catalog.db.t1.c", Set("v2_catalog.db.t2.b", "v2_catalog.db.t2.c")))))

      val ret2 =
        extractLineage(
          s"insert into table v2_catalog.db.t1 select a," +
            s"count(distinct(cast(b as int)+cast(c as int)))," +
            s"count(distinct(b)) * count(distinct(c))" +
            s"from v2_catalog.db.t2 group by a")
      assert(ret2 == Lineage(
        List("v2_catalog.db.t2"),
        List("v2_catalog.db.t1"),
        List(
          ("v2_catalog.db.t1.a", Set("v2_catalog.db.t2.a")),
          ("v2_catalog.db.t1.b", Set("v2_catalog.db.t2.b", "v2_catalog.db.t2.c")),
          ("v2_catalog.db.t1.c", Set("v2_catalog.db.t2.b", "v2_catalog.db.t2.c")))))
    }
  }

  test("test grouping sets") {
    withTable("t1", "t2") { _ =>
      spark.sql("CREATE TABLE t1 (a string, b string, c string) USING hive")
      spark.sql("CREATE TABLE t2 (a string, b string, c string, d string) USING hive")
      val ret0 =
        extractLineage(
          s"insert into table t1 select a,b,GROUPING__ID " +
            s"from t2 group by a,b,c,d grouping sets ((a,b,c), (a,b,d))")
      assert(ret0 == Lineage(
        List(s"$DEFAULT_CATALOG.default.t2"),
        List(s"$DEFAULT_CATALOG.default.t1"),
        List(
          (s"$DEFAULT_CATALOG.default.t1.a", Set(s"$DEFAULT_CATALOG.default.t2.a")),
          (s"$DEFAULT_CATALOG.default.t1.b", Set(s"$DEFAULT_CATALOG.default.t2.b")),
          (s"$DEFAULT_CATALOG.default.t1.c", Set()))))
    }
  }

  test("test cache table with window function") {
    withTable("t1", "t2") { _ =>
      spark.sql("CREATE TABLE t1 (a string, b string) USING hive")
      spark.sql("CREATE TABLE t2 (a string, b string) USING hive")

      spark.sql(
        s"cache table c1 select * from (" +
          s"select a, b, row_number() over (partition by a order by b asc ) rank from t2)" +
          s" where rank=1")
      val ret0 = extractLineage("insert overwrite table t1 select a, b from c1")
      assert(ret0 == Lineage(
        List(s"$DEFAULT_CATALOG.default.t2"),
        List(s"$DEFAULT_CATALOG.default.t1"),
        List(
          (s"$DEFAULT_CATALOG.default.t1.a", Set(s"$DEFAULT_CATALOG.default.t2.a")),
          (s"$DEFAULT_CATALOG.default.t1.b", Set(s"$DEFAULT_CATALOG.default.t2.b")))))

      val ret1 = extractLineage("insert overwrite table t1 select a, rank from c1")
      assert(ret1 == Lineage(
        List(s"$DEFAULT_CATALOG.default.t2"),
        List(s"$DEFAULT_CATALOG.default.t1"),
        List(
          (s"$DEFAULT_CATALOG.default.t1.a", Set(s"$DEFAULT_CATALOG.default.t2.a")),
          (
            s"$DEFAULT_CATALOG.default.t1.b",
            Set(s"$DEFAULT_CATALOG.default.t2.a", s"$DEFAULT_CATALOG.default.t2.b")))))

      spark.sql(
        s"cache table c2 select * from (" +
          s"select b, a, row_number() over (partition by a order by b asc ) rank from t2)" +
          s" where rank=1")
      val ret2 = extractLineage("insert overwrite table t1 select a, b from c2")
      assert(ret2 == Lineage(
        List(s"$DEFAULT_CATALOG.default.t2"),
        List(s"$DEFAULT_CATALOG.default.t1"),
        List(
          (s"$DEFAULT_CATALOG.default.t1.a", Set(s"$DEFAULT_CATALOG.default.t2.a")),
          (s"$DEFAULT_CATALOG.default.t1.b", Set(s"$DEFAULT_CATALOG.default.t2.b")))))

      spark.sql(
        s"cache table c3 select * from (" +
          s"select a as aa, b as bb, row_number() over (partition by a order by b asc ) rank" +
          s" from t2) where rank=1")
      val ret3 = extractLineage("insert overwrite table t1 select aa, bb from c3")
      assert(ret3 == Lineage(
        List(s"$DEFAULT_CATALOG.default.t2"),
        List(s"$DEFAULT_CATALOG.default.t1"),
        List(
          (s"$DEFAULT_CATALOG.default.t1.a", Set(s"$DEFAULT_CATALOG.default.t2.a")),
          (s"$DEFAULT_CATALOG.default.t1.b", Set(s"$DEFAULT_CATALOG.default.t2.b")))))
    }
  }

  test("test count()") {
    withTable("t1", "t2") { _ =>
      spark.sql("CREATE TABLE t1 (a string, b string, c string) USING hive")
      spark.sql("CREATE TABLE t2 (a string, b string, c string) USING hive")
      val ret0 = extractLineage("insert into t1 select 1,2,(select count(distinct" +
        " ifnull(get_json_object(a, '$.b.imei'), get_json_object(a, '$.b.android_id'))) from t2)")

      assert(ret0 == Lineage(
        List(s"$DEFAULT_CATALOG.default.t2"),
        List(s"$DEFAULT_CATALOG.default.t1"),
        List(
          (s"$DEFAULT_CATALOG.default.t1.a", Set()),
          (s"$DEFAULT_CATALOG.default.t1.b", Set()),
          (s"$DEFAULT_CATALOG.default.t1.c", Set(s"$DEFAULT_CATALOG.default.t2.a")))))
    }
  }

  test("test create view from view") {
    withTable("t1") { _ =>
      spark.sql("CREATE TABLE t1 (a string, b string, c string) USING hive")
      withView("t2") { _ =>
        spark.sql("CREATE VIEW t2 as select * from t1")
        val ret0 =
          extractLineage(
            s"create or replace view view_tst comment 'view'" +
              s" as select a as k,b" +
              s" from t2" +
              s" where a in ('HELLO') and c = 'HELLO'")
        assert(ret0 == Lineage(
          List(s"$DEFAULT_CATALOG.default.t1"),
          List(s"$DEFAULT_CATALOG.default.view_tst"),
          List(
            (s"$DEFAULT_CATALOG.default.view_tst.k", Set(s"$DEFAULT_CATALOG.default.t1.a")),
            (s"$DEFAULT_CATALOG.default.view_tst.b", Set(s"$DEFAULT_CATALOG.default.t1.b")))))
      }
    }
  }

  test("test for skip parsing permanent view") {
    withTable("t1") { _ =>
      SparkContextHelper.setConf(LineageConf.SKIP_PARSING_PERMANENT_VIEW_ENABLED, true)
      spark.sql("CREATE TABLE t1 (a string, b string, c string) USING hive")
      withView("t2") { _ =>
        spark.sql("CREATE VIEW t2 as select * from t1")
        val ret0 =
          extractLineage(
            s"select a as k, b" +
              s" from t2" +
              s" where a in ('HELLO') and c = 'HELLO'")
        assert(ret0 == Lineage(
          List(s"$DEFAULT_CATALOG.default.t2"),
          List(),
          List(
            ("k", Set(s"$DEFAULT_CATALOG.default.t2.a")),
            ("b", Set(s"$DEFAULT_CATALOG.default.t2.b")))))
      }
    }
  }

  test("test the statement with FROM xxx INSERT xxx") {
    withTable("t1", "t2", "t3") { _ =>
      spark.sql("CREATE TABLE t1 (a string, b string) USING hive")
      spark.sql("CREATE TABLE t2 (a string, b string) USING hive")
      spark.sql("CREATE TABLE t3 (a string, b string) USING hive")
      val ret0 = extractLineage("from (select a,b from t1)" +
        " insert overwrite table t2 select a,b where a=1" +
        " insert overwrite table t3 select a,b where b=1")
      assert(ret0 == Lineage(
        List(s"$DEFAULT_CATALOG.default.t1"),
        List(s"$DEFAULT_CATALOG.default.t2", s"$DEFAULT_CATALOG.default.t3"),
        List(
          (s"$DEFAULT_CATALOG.default.t2.a", Set(s"$DEFAULT_CATALOG.default.t1.a")),
          (s"$DEFAULT_CATALOG.default.t2.b", Set(s"$DEFAULT_CATALOG.default.t1.b")),
          (s"$DEFAULT_CATALOG.default.t3.a", Set(s"$DEFAULT_CATALOG.default.t1.a")),
          (s"$DEFAULT_CATALOG.default.t3.b", Set(s"$DEFAULT_CATALOG.default.t1.b")))))
    }
  }

  test("test lateral view explode") {
    withTable("t1", "t2") { _ =>
      spark.sql("CREATE TABLE t1 (a string, b string, c string, d string) USING hive")
      spark.sql("CREATE TABLE t2 (a string, b string, c string, d string) USING hive")

      val ret0 = extractLineage("insert into t1 select 1, t2.b, cc.action, t2.d " +
        "from t2 lateral view explode(split(c,'\\},\\{')) cc as action")
      assert(ret0 == Lineage(
        List(s"$DEFAULT_CATALOG.default.t2"),
        List(s"$DEFAULT_CATALOG.default.t1"),
        List(
          (s"$DEFAULT_CATALOG.default.t1.a", Set()),
          (s"$DEFAULT_CATALOG.default.t1.b", Set(s"$DEFAULT_CATALOG.default.t2.b")),
          (s"$DEFAULT_CATALOG.default.t1.c", Set(s"$DEFAULT_CATALOG.default.t2.c")),
          (s"$DEFAULT_CATALOG.default.t1.d", Set(s"$DEFAULT_CATALOG.default.t2.d")))))

      val ret1 = extractLineage("insert into t1 select 1, t2.b, cc.action0, dd.action1 " +
        "from t2 " +
        "lateral view explode(split(c,'\\},\\{')) cc as action0 " +
        "lateral view explode(split(d,'\\},\\{')) dd as action1")
      assert(ret1 == Lineage(
        List(s"$DEFAULT_CATALOG.default.t2"),
        List(s"$DEFAULT_CATALOG.default.t1"),
        List(
          (s"$DEFAULT_CATALOG.default.t1.a", Set()),
          (s"$DEFAULT_CATALOG.default.t1.b", Set(s"$DEFAULT_CATALOG.default.t2.b")),
          (s"$DEFAULT_CATALOG.default.t1.c", Set(s"$DEFAULT_CATALOG.default.t2.c")),
          (s"$DEFAULT_CATALOG.default.t1.d", Set(s"$DEFAULT_CATALOG.default.t2.d")))))

      val ret2 = extractLineage("insert into t1 select 1, t2.b, dd.pos, dd.action1 " +
        "from t2 " +
        "lateral view posexplode(split(d,'\\},\\{')) dd as pos, action1")
      assert(ret2 == Lineage(
        List(s"$DEFAULT_CATALOG.default.t2"),
        List(s"$DEFAULT_CATALOG.default.t1"),
        List(
          (s"$DEFAULT_CATALOG.default.t1.a", Set()),
          (s"$DEFAULT_CATALOG.default.t1.b", Set(s"$DEFAULT_CATALOG.default.t2.b")),
          (s"$DEFAULT_CATALOG.default.t1.c", Set(s"$DEFAULT_CATALOG.default.t2.d")),
          (s"$DEFAULT_CATALOG.default.t1.d", Set(s"$DEFAULT_CATALOG.default.t2.d")))))
    }
  }

  test("test directory to table") {
    val inputFile = getClass.getResource("/").getPath + "input_file"
    val sourceFile = File(inputFile).createFile()
    withView("temp_view") { _ =>
      {
        spark.sql(
          s"""
             |CREATE OR REPLACE TEMPORARY VIEW temp_view (
             | `a` STRING COMMENT '',
             | `b` STRING COMMENT ''
             |) USING csv OPTIONS(
             |  sep='\t',
             |  path='${sourceFile.path}'
             |);
             |""".stripMargin).collect()

        val ret0 = extractLineageWithoutExecuting(
          s"""
             |INSERT OVERWRITE TABLE test_db.test_table_from_dir
             |SELECT `a`, `b` FROM temp_view
             |""".stripMargin)

        assert(ret0 == Lineage(
          List(),
          List(s"spark_catalog.test_db.test_table_from_dir"),
          List(
            (s"spark_catalog.test_db.test_table_from_dir.a0", Set()),
            (s"spark_catalog.test_db.test_table_from_dir.b0", Set()))))
      }
    }
  }

  test("columns lineage extract - collect input tables by plan") {
    val ddls =
      """
        |create table v2_catalog.db.tb1(col1 string, col2 string, col3 string)
        |create table v2_catalog.db.tb2(col1 string, col2 string, col3 string)
        |create table v2_catalog.db.tb3(col1 string, col2 string, col3 string)
        |""".stripMargin
    ddls.split("\n").filter(_.nonEmpty).foreach(spark.sql(_).collect())
    withTable("v2_catalog.db.tb1", "v2_catalog.db.tb2", "v2_catalog.db.tb3") { _ =>
      val sql0 =
        """
          |insert overwrite v2_catalog.db.tb3
          |select t1.col1, t1.col2 , t1.col3
          |from v2_catalog.db.tb1 t1 join v2_catalog.db.tb2 t2
          |on t1.col1 = t2.col1
          |""".stripMargin

      val ret0 = extractLineage(sql0)
      assert(
        ret0 == Lineage(
          List("v2_catalog.db.tb1", "v2_catalog.db.tb2"),
          List("v2_catalog.db.tb3"),
          List(
            ("v2_catalog.db.tb3.col1", Set("v2_catalog.db.tb1.col1")),
            ("v2_catalog.db.tb3.col2", Set("v2_catalog.db.tb1.col2")),
            ("v2_catalog.db.tb3.col3", Set("v2_catalog.db.tb1.col3")))))

      val sql1 =
        """
          |insert overwrite v2_catalog.db.tb3
          |select t1.col1, t1.col2 , t1.col3
          |from v2_catalog.db.tb1 t1 left semi join v2_catalog.db.tb2 t2
          |on t1.col1 = t2.col1
          |""".stripMargin

      val ret1 = extractLineage(sql1)
      assert(
        ret1 == Lineage(
          List("v2_catalog.db.tb1", "v2_catalog.db.tb2"),
          List("v2_catalog.db.tb3"),
          List(
            ("v2_catalog.db.tb3.col1", Set("v2_catalog.db.tb1.col1")),
            ("v2_catalog.db.tb3.col2", Set("v2_catalog.db.tb1.col2")),
            ("v2_catalog.db.tb3.col3", Set("v2_catalog.db.tb1.col3")))))
    }
  }

  protected def extractLineageWithoutExecuting(sql: String): Lineage = {
    val parsed = spark.sessionState.sqlParser.parsePlan(sql)
    val analyzed = spark.sessionState.analyzer.execute(parsed)
    spark.sessionState.analyzer.checkAnalysis(analyzed)
    SparkSQLLineageParseHelper(spark).transformToLineage(0, analyzed).get
  }

  private def extractLineage(sql: String): Lineage = {
    val parsed = spark.sessionState.sqlParser.parsePlan(sql)
    val qe = spark.sessionState.executePlan(parsed)
    val analyzed = qe.analyzed
    SparkSQLLineageParseHelper(spark).transformToLineage(0, analyzed).get
  }

}

case class SimpleInsert(userSpecifiedSchema: StructType)(@transient val sparkSession: SparkSession)
  extends BaseRelation with InsertableRelation {

  override def sqlContext: SQLContext = sparkSession.sqlContext

  override def schema: StructType = userSpecifiedSchema

  override def insert(input: DataFrame, overwrite: Boolean): Unit = {
    input.collect
  }
}

class SimpleInsertSource extends SchemaRelationProvider {
  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      schema: StructType): BaseRelation = {
    SimpleInsert(schema)(sqlContext.sparkSession)
  }
}
