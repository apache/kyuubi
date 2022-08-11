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

package org.apache.spark.kyuubi

import scala.collection.immutable.List
import scala.reflect.io.File

import org.apache.spark.sql.{DataFrame, SparkSession, SQLContext}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.sources.{BaseRelation, InsertableRelation, SchemaRelationProvider}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.engine.spark.KyuubiSparkUtil.isSparkVersionAtMost
import org.apache.kyuubi.engine.spark.WithSparkSQLEngine
import org.apache.kyuubi.engine.spark.events.Lineage

class SparkSQLLineageParserHelperSuite extends WithSparkSQLEngine with KyuubiFunSuite {
  val catalogName =
    if (isSparkVersionAtMost("3.1")) "org.apache.spark.sql.connector.InMemoryTableCatalog"
    else "org.apache.spark.sql.connector.catalog.InMemoryTableCatalog"

  override def withKyuubiConf: Map[String, String] = Map(
    "spark.sql.catalog.v2_catalog" -> catalogName)

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.sql(s"create database if not exists test_db0")
    spark.sql(s"create table if not exists test_db0.test_table0" +
      s" (key int, value string) using parquet")
    spark.sql(s"create table if not exists test_db0.test_table_part0" +
      s" (key int, value string, pid string) using parquet" +
      s"  partitioned by(pid)")
  }

  override def afterAll(): Unit = {
    Seq("test_db0.test_table0", "test_db0.test_table_part0").foreach { t =>
      spark.sql(s"drop table if exists $t")
    }
    spark.sql(s"drop database if exists test_db0")
    spark.stop()
    super.afterAll()
  }

  protected def withTable(t: String)(f: String => Unit): Unit = {
    try {
      f(t)
    } finally {
      spark.sql(s"DROP TABLE IF EXISTS $t")
    }
  }

  test("columns lineage extract - AlterViewAsCommand") {
    spark.sql(s"create view alterviewascommand as select key from test_db0.test_table0")
    val ret0 =
      exectractLineage(s"alter view alterviewascommand as select key from test_db0.test_table0")
    assert(ret0 == Lineage(
      List("test_db0.test_table0"),
      List("default.alterviewascommand"),
      List(("default.alterviewascommand.key", Set("test_db0.test_table0.key")))))

    spark.sql(s"create view alterviewascommand1 as select * from test_db0.test_table0")
    val ret1 =
      exectractLineage(s"alter view alterviewascommand1 as select * from test_db0.test_table0")
    assert(ret1 == Lineage(
      List("test_db0.test_table0"),
      List("default.alterviewascommand1"),
      List(
        ("default.alterviewascommand1.key", Set("test_db0.test_table0.key")),
        ("default.alterviewascommand1.value", Set("test_db0.test_table0.value")))))
  }

  test("columns lineage extract - CreateViewCommand") {
    val ret0 = exectractLineage(
      "create view createviewcommand(a, b) as select key, value from test_db0.test_table0")
    assert(ret0 == Lineage(
      List("test_db0.test_table0"),
      List("default.createviewcommand"),
      List(
        ("default.createviewcommand.a", Set("test_db0.test_table0.key")),
        ("default.createviewcommand.b", Set("test_db0.test_table0.value")))))

    val ret1 = exectractLineage(
      "create view createviewcommand1 as select key, value from test_db0.test_table0")
    assert(ret1 == Lineage(
      List("test_db0.test_table0"),
      List("default.createviewcommand1"),
      List(
        ("default.createviewcommand1.key", Set("test_db0.test_table0.key")),
        ("default.createviewcommand1.value", Set("test_db0.test_table0.value")))))

    val ret2 = exectractLineage(
      "create view createviewcommand2 as select * from test_db0.test_table0")
    assert(ret2 == Lineage(
      List("test_db0.test_table0"),
      List("default.createviewcommand2"),
      List(
        ("default.createviewcommand2.key", Set("test_db0.test_table0.key")),
        ("default.createviewcommand2.value", Set("test_db0.test_table0.value")))))
  }

  test("columns lineage extract - CreateDataSourceTableAsSelectCommand") {
    val ret0 = exectractLineage("create table createdatasourcetableasselectcommand using parquet" +
      s" AS SELECT key, value FROM test_db0.test_table0")
    assert(ret0 == Lineage(
      List("test_db0.test_table0"),
      List("default.createdatasourcetableasselectcommand"),
      List(
        ("default.createdatasourcetableasselectcommand.key", Set("test_db0.test_table0.key")),
        (
          "default.createdatasourcetableasselectcommand.value",
          Set("test_db0.test_table0.value")))))

    val ret1 = exectractLineage("create table createdatasourcetableasselectcommand1 using parquet" +
      s" AS SELECT * FROM test_db0.test_table0")
    assert(ret1 == Lineage(
      List("test_db0.test_table0"),
      List("default.createdatasourcetableasselectcommand1"),
      List(
        ("default.createdatasourcetableasselectcommand1.key", Set("test_db0.test_table0.key")),
        (
          "default.createdatasourcetableasselectcommand1.value",
          Set("test_db0.test_table0.value")))))
  }

  test("columns lineage extract - CreateHiveTableAsSelectCommand") {
    val ret0 = exectractLineage("create table createhivetableasselectcommand using hive" +
      s" as select key, value from test_db0.test_table0")
    assert(ret0 == Lineage(
      List("test_db0.test_table0"),
      List("default.createhivetableasselectcommand"),
      List(
        ("default.createhivetableasselectcommand.key", Set("test_db0.test_table0.key")),
        ("default.createhivetableasselectcommand.value", Set("test_db0.test_table0.value")))))

    val ret1 = exectractLineage("create table createhivetableasselectcommand1 using hive" +
      s" as select * from test_db0.test_table0")
    assert(ret1 == Lineage(
      List("test_db0.test_table0"),
      List("default.createhivetableasselectcommand1"),
      List(
        ("default.createhivetableasselectcommand1.key", Set("test_db0.test_table0.key")),
        ("default.createhivetableasselectcommand1.value", Set("test_db0.test_table0.value")))))
  }

  test("columns lineage extract - OptimizedCreateHiveTableAsSelectCommand") {
    val ret =
      exectractLineage(
        "create table optimizedcreatehivetableasselectcommand stored as parquet " +
          "as select * from test_db0.test_table0")
    assert(ret == Lineage(
      List("test_db0.test_table0"),
      List("default.optimizedcreatehivetableasselectcommand"),
      List(
        ("default.optimizedcreatehivetableasselectcommand.key", Set("test_db0.test_table0.key")),
        (
          "default.optimizedcreatehivetableasselectcommand.value",
          Set("test_db0.test_table0.value")))))
  }

  test("columns lineage extract - CreateTableAsSelect") {
    val ret0 = exectractLineage("create table v2_catalog.db.createhivetableasselectcommand" +
      s" as select key, value from test_db0.test_table0")
    assert(ret0 == Lineage(
      List("test_db0.test_table0"),
      List("v2_catalog.db.createhivetableasselectcommand"),
      List(
        ("v2_catalog.db.createhivetableasselectcommand.key", Set("test_db0.test_table0.key")),
        (
          "v2_catalog.db.createhivetableasselectcommand.value",
          Set("test_db0.test_table0.value")))))

    val ret1 = exectractLineage("create table v2_catalog.db.createhivetableasselectcommand1" +
      s" as select * from test_db0.test_table0")
    assert(ret1 == Lineage(
      List("test_db0.test_table0"),
      List("v2_catalog.db.createhivetableasselectcommand1"),
      List(
        ("v2_catalog.db.createhivetableasselectcommand1.key", Set("test_db0.test_table0.key")),
        (
          "v2_catalog.db.createhivetableasselectcommand1.value",
          Set("test_db0.test_table0.value")))))
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
        exectractLineage(
          s"insert into table  $tableName select key, value from test_db0.test_table0")
      assert(ret0 == Lineage(
        List("test_db0.test_table0"),
        List("default.insertintodatasourcecommand"),
        List(
          ("default.insertintodatasourcecommand.a", Set("test_db0.test_table0.key")),
          ("default.insertintodatasourcecommand.b", Set("test_db0.test_table0.value")))))

      val ret1 =
        exectractLineage(
          s"insert into table  $tableName select * from test_db0.test_table0")
      assert(ret1 == Lineage(
        List("test_db0.test_table0"),
        List("default.insertintodatasourcecommand"),
        List(
          ("default.insertintodatasourcecommand.a", Set("test_db0.test_table0.key")),
          ("default.insertintodatasourcecommand.b", Set("test_db0.test_table0.value")))))

    }
  }

  test("columns lineage extract - InsertIntoHadoopFsRelationCommand") {
    val tableName = "insertintohadoopfsrelationcommand"
    withTable(tableName) { _ =>
      spark.sql(s"CREATE TABLE $tableName (a int, b string) USING parquet")
      val ret0 =
        exectractLineage(
          s"insert into table $tableName select key, value from test_db0.test_table0")

      assert(ret0 == Lineage(
        List("test_db0.test_table0"),
        List("default.insertintohadoopfsrelationcommand"),
        List(
          ("default.insertintohadoopfsrelationcommand.a", Set("test_db0.test_table0.key")),
          ("default.insertintohadoopfsrelationcommand.b", Set("test_db0.test_table0.value")))))
    }

  }

  test("columns lineage extract - InsertIntoDatasourceDirCommand") {
    val tableDirectory = getClass.getResource("/").getPath + "table_directory"
    val directory = File(tableDirectory).createDirectory()
    val ret0 = exectractLineage(s"""
                                   |INSERT OVERWRITE DIRECTORY '$directory.path'
                                   |USING parquet
                                   |SELECT * FROM test_db0.test_table_part0""".stripMargin)
    assert(ret0 == Lineage(
      List("test_db0.test_table_part0"),
      List(s"""`$directory.path`"""),
      List(
        (s"""`$directory.path`.key""", Set("test_db0.test_table_part0.key")),
        (s"""`$directory.path`.value""", Set("test_db0.test_table_part0.value")),
        (s"""`$directory.path`.pid""", Set("test_db0.test_table_part0.pid")))))
  }

  test("columns lineage extract - InsertIntoHiveDirCommand") {
    val tableDirectory = getClass.getResource("/").getPath + "table_directory"
    val directory = File(tableDirectory).createDirectory()
    val ret0 = exectractLineage(s"""
                                   |INSERT OVERWRITE DIRECTORY '$directory.path'
                                   |USING parquet
                                   |SELECT * FROM test_db0.test_table_part0""".stripMargin)
    assert(ret0 == Lineage(
      List("test_db0.test_table_part0"),
      List(s"""`$directory.path`"""),
      List(
        (s"""`$directory.path`.key""", Set("test_db0.test_table_part0.key")),
        (s"""`$directory.path`.value""", Set("test_db0.test_table_part0.value")),
        (s"""`$directory.path`.pid""", Set("test_db0.test_table_part0.pid")))))
  }

  test("columns lineage extract - InsertIntoHiveTable") {
    val tableName = "insertintohivetable"
    withTable(tableName) { _ =>
      spark.sql(s"CREATE TABLE $tableName (a int, b string) USING hive")
      val ret0 =
        exectractLineage(
          s"insert into table $tableName select * from test_db0.test_table0")

      assert(ret0 == Lineage(
        List("test_db0.test_table0"),
        List(s"default.$tableName"),
        List(
          (s"default.$tableName.a", Set("test_db0.test_table0.key")),
          (s"default.$tableName.b", Set("test_db0.test_table0.value")))))
    }

  }

  test("columns lineage extract - logical relation sql") {
    val ret0 = exectractLineage("select key, value from test_db0.test_table0")
    assert(ret0 == Lineage(
      List("test_db0.test_table0"),
      List(),
      List(
        ("key", Set("test_db0.test_table0.key")),
        ("value", Set("test_db0.test_table0.value")))))

    val ret1 = exectractLineage("select * from test_db0.test_table_part0")
    assert(ret1 == Lineage(
      List("test_db0.test_table_part0"),
      List(),
      List(
        ("key", Set("test_db0.test_table_part0.key")),
        ("value", Set("test_db0.test_table_part0.value")),
        ("pid", Set("test_db0.test_table_part0.pid")))))

  }

  test("columns lineage extract - not generate lineage sql") {
    val ret0 = exectractLineage("create table test_table1(a string, b string, c string)")
    assert(ret0 == Lineage(List(), List(), List()))
  }

  test("columns lineage extract - data source V2 sql") {
    val ddls =
      """
        |create table v2_catalog.db.tb(col1 string, col2 string, col3 string)
        |""".stripMargin

    ddls.split("\n").filter(_.nonEmpty).foreach(spark.sql(_).collect())

    val sql0 = "select col1 from v2_catalog.db.tb"
    val ret0 = exectractLineage(sql0)
    assert(ret0 == Lineage(
      List("v2_catalog.db.tb"),
      List(),
      List("col1" -> Set("v2_catalog.db.tb.col1"))))

    val sql1 = "select col1, hash(hash(col1)) as col2 from v2_catalog.db.tb"
    val ret1 = exectractLineage(sql1)
    assert(ret1 == Lineage(
      List("v2_catalog.db.tb"),
      List(),
      List("col1" -> Set("v2_catalog.db.tb.col1"), "col2" -> Set("v2_catalog.db.tb.col1"))))

    val sql2 = "select col1, case col1 when '1' then 's1' else col1 end col2 from v2_catalog.db.tb"
    val ret2 = exectractLineage(sql2)
    assert(ret2 == Lineage(
      List("v2_catalog.db.tb"),
      List(),
      List("col1" -> Set("v2_catalog.db.tb.col1"), "col2" -> Set("v2_catalog.db.tb.col1"))))

    val sql3 =
      "select col1 as col2, 'col2' as col2, 'col2', col3 as col2 " +
        "from v2_catalog.db.tb group by col1"
    val ret3 = exectractLineage(sql3)
    assert(ret3 == Lineage(
      List("v2_catalog.db.tb"),
      List(),
      List(
        ("col2", Set("v2_catalog.db.tb.col1")),
        ("col2", Set()),
        ("col2", Set()),
        ("col2", Set("v2_catalog.db.tb.col3")))))

    val sql4 =
      "select col1 as col2, sum(hash(col1) + hash(hash(col1))) from v2_catalog.db.tb group by col1"
    val ret4 = exectractLineage(sql4)
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
    val ret5 = exectractLineage(sql5)
    assert(ret5 == Lineage(
      List("v2_catalog.db.tb"),
      List(),
      List(
        ("col2", Set("v2_catalog.db.tb.col1")),
        ("count(col3)", Set("v2_catalog.db.tb.col1")))))
  }

  test("columns lineage extract - base sql") {
    val ddls =
      List(
        "CREATE TABLE tmp0 AS SELECT * FROM VALUES(1),(2),(3) AS t(tmp0_0)",
        "CREATE TABLE tmp1 AS select c1 as tmp1_0, c2 as tmp1_1,  c3 as tmp1_2," +
          "concat(c1, c2) as tmp1_3 from" +
          " VALUES(1,'a',4),(2, 'b', 4),(3, 'c', 4) AS t(c1, c2, c3)")

    ddls.foreach(spark.sql(_).collect())

    val sql0 =
      """
        |select tmp0_0 as a0, tmp1_0 as a1 from tmp0 join tmp1 where tmp1_0 = tmp0_0
        |""".stripMargin
    val sql0ExpectResult = Lineage(
      List("default.tmp0", "default.tmp1"),
      List(),
      List(
        "a0" -> Set("default.tmp0.tmp0_0"),
        "a1" -> Set("default.tmp1.tmp1_0")))

    val sql1 =
      """
        |select count(tmp1_0) as cnt, tmp1_1 from tmp1 group by tmp1_1
        |""".stripMargin
    val sql1ExpectResult = Lineage(
      List("default.tmp1"),
      List(),
      List(
        "cnt" -> Set("default.tmp1.tmp1_0"),
        "tmp1_1" -> Set("default.tmp1.tmp1_1")))

    val ret0 = exectractLineage(sql0)
    assert(ret0 == sql0ExpectResult)
    val ret1 = exectractLineage(sql1)
    assert(ret1 == sql1ExpectResult)
  }

  test("columns lineage extract - CTE sql") {
    val ddls =
      List(
        "create database if not exists test_db",
        "create table test_db.goods_detail0(goods_id string, cat_id string)",
        "create table v2_catalog.test_db_v2.goods_detail1" +
          "(goods_id string, cat_id string, product_id string)",
        "create table v2_catalog.test_db_v2.mall_icon_schedule" +
          "(relation_id string, icon_id string," +
          " icon_type string, is_enabled string, start_time date, end_time date)",
        "create table v2_catalog.test_db_v2.mall_icon" +
          "(id string, name string, is_enabled string, start_time date, end_time date)")
    ddls.foreach(spark.sql(_).collect())

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

    val ret0 = exectractLineage(sql0)
    assert(ret0 == Lineage(
      List(
        "test_db.goods_detail0",
        "v2_catalog.test_db_v2.goods_detail1",
        "v2_catalog.test_db_v2.mall_icon_schedule",
        "v2_catalog.test_db_v2.mall_icon"),
      List(),
      List(
        ("goods_id", Set("test_db.goods_detail0.goods_id")),
        ("cate_grory", Set("test_db.goods_detail0.cat_id")),
        ("cat_id", Set("v2_catalog.test_db_v2.goods_detail1.cat_id")),
        ("product_id", Set("v2_catalog.test_db_v2.goods_detail1.product_id")),
        ("start_time", Set("v2_catalog.test_db_v2.mall_icon_schedule.start_time")),
        ("end_time", Set("v2_catalog.test_db_v2.mall_icon.end_time")))))
  }

  test("columns lineage extract - join sql ") {
    val ddls =
      """
        |create table v2_catalog.db.tb1(col1 string, col2 string, col3 string)
        |create table v2_catalog.db.tb2(col1 string, col2 string, col3 string)
        |""".stripMargin
    ddls.split("\n").filter(_.nonEmpty).foreach(spark.sql(_).collect())

    val sql0 =
      """
        |select t1.col1 as a1, t2.col1 as b1, concat(t1.col1, t2.col1) as ab1,
        |concat(concat(t1.col1, t2.col2), concat(t1.col2, t2.col3)) as ab2
        |from v2_catalog.db.tb1 t1 join v2_catalog.db.tb2 t2
        |on t1.col1 = t2.col1
        |""".stripMargin

    val ret0 = exectractLineage(sql0)
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

  test("columns lineage extract - union sql") {
    val ddls =
      """
        |create database if not exists test_db
        |create table test_db.test_table0(a int, b string, c string)
        |create table test_db.test_table1(a int, b string, c string)
        |""".stripMargin
    ddls.split("\n").filter(_.nonEmpty).foreach(spark.sql(_).collect())
    val sql0 =
      """
        |select a, b, c from (
        |select a, b, b as c from test_db.test_table0
        |union
        |select a, b, c as c from test_db.test_table1
        |) a
        |""".stripMargin
    val ret0 = exectractLineage(sql0)
    assert(ret0 == Lineage(
      List("test_db.test_table0", "test_db.test_table1"),
      List(),
      List(
        ("a", Set("test_db.test_table0.a", "test_db.test_table1.a")),
        ("b", Set("test_db.test_table0.b", "test_db.test_table1.b")),
        ("c", Set("test_db.test_table0.b", "test_db.test_table1.c")))))
  }

  test("columns lineage extract - agg and union sql") {
    val ddls =
      List(
        "create database if not exists test_db",
        "create table test_db.test_order_item(stat_date string, pay_time date," +
          "channel_id string, sub_channel_id string," +
          "user_type string, country_name string," +
          "order_id string, goods_count int, shop_price int, is_valid_order int)",
        "create table test_db.test_p0_order_item(pay_time date," +
          "channel_id string, sub_channel_id string," +
          "user_type string, country_name string, is_valid_item int)")
    ddls.foreach(spark.sql(_).collect())
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
        |channel_id, sub_channel_id, country_name
        |) a
        |""".stripMargin
    val ret0 = exectractLineage(sql0)
    assert(ret0 == Lineage(
      List("test_db.test_order_item"),
      List(),
      List(
        ("stat_date", Set("test_db.test_order_item.stat_date")),
        ("channel_id", Set("test_db.test_order_item.channel_id")),
        ("sub_channel_id", Set("test_db.test_order_item.sub_channel_id")),
        ("user_type", Set("test_db.test_order_item.user_type")),
        ("country_name", Set("test_db.test_order_item.country_name")),
        ("get_count0", Set("test_db.test_order_item.order_id")),
        (
          "get_amount0",
          Set("test_db.test_order_item.goods_count", "test_db.test_order_item.shop_price")),
        ("add_time", Set()))))
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
    val ret1 = exectractLineage(sql1)
    assert(ret1 == Lineage(
      List("test_db.test_order_item", "test_db.test_p0_order_item"),
      List(),
      List(
        (
          "channel_id",
          Set("test_db.test_order_item.channel_id", "test_db.test_p0_order_item.channel_id")),
        (
          "sub_channel_id",
          Set(
            "test_db.test_order_item.sub_channel_id",
            "test_db.test_p0_order_item.sub_channel_id")),
        (
          "country_name",
          Set("test_db.test_order_item.country_name", "test_db.test_p0_order_item.country_name")),
        ("get_count0", Set("test_db.test_order_item.order_id")),
        (
          "get_amount0",
          Set("test_db.test_order_item.goods_count", "test_db.test_order_item.shop_price")),
        ("add_time", Set()))))
  }

  private def exectractLineage(sql: String): Lineage = {
    val parsed = spark.sessionState.sqlParser.parsePlan(sql)
    val analyzed = spark.sessionState.analyzer.execute(parsed)
    val optimized = spark.sessionState.optimizer.execute(analyzed)
    SparkSQLLineageParseHelper(spark).transformToLineage("", optimized).get
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
