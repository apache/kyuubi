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

package org.apache.kyuubi.plugin.spark.authz

import org.apache.commons.lang3.StringUtils
import org.apache.spark.SPARK_VERSION
import org.apache.spark.sql.{DataFrame, SparkSession}

import org.apache.kyuubi.{KyuubiFunSuite, Utils}
import org.apache.kyuubi.plugin.spark.authz.OperationType._

abstract class PrivilegesBuilderSuite extends KyuubiFunSuite {

  protected val catalogImpl: String

  protected val isSparkV2: Boolean = SPARK_VERSION.split("\\.").head == "2"
  protected val isSparkV32OrGreater: Boolean = {
    val parts = SPARK_VERSION.split("\\.").map(_.toInt)
    (parts.head > 3) || (parts.head == 3 && parts(1) >= 2)
  }
  protected lazy val spark: SparkSession = SparkSession.builder()
    .master("local")
    .config("spark.ui.enabled", "false")
    .config(
      "spark.sql.warehouse.dir",
      Utils.createTempDir(namePrefix = "spark-warehouse").toString)
    .config("spark.sql.catalogImplementation", catalogImpl)
    .getOrCreate()

  protected val sql: String => DataFrame = spark.sql

  protected def withTable(t: String)(f: String => Unit): Unit = {
    try {
      f(t)
    } finally {
      sql(s"DROP TABLE IF EXISTS $t")
    }
  }

  protected def withDatabase(t: String)(f: String => Unit): Unit = {
    try {
      f(t)
    } finally {
      sql(s"DROP DATABASE IF EXISTS $t")
    }
  }

  protected val reusedDb: String = getClass.getSimpleName
  protected val reusedTable: String = reusedDb + "." + getClass.getSimpleName
  protected val reusedPartTable: String = reusedTable + "_part"

  override def beforeAll(): Unit = {
    sql(s"CREATE DATABASE IF NOT EXISTS $reusedDb")
    sql(s"CREATE TABLE IF NOT EXISTS $reusedTable" +
      s" (key int, value string) USING parquet")
    sql(s"CREATE TABLE IF NOT EXISTS $reusedPartTable" +
      s" (key int, value string, pid string) USING parquet" +
      s"  PARTITIONED BY(pid)")
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    Seq(reusedTable, reusedPartTable).foreach { t =>
      sql(s"DROP TABLE IF EXISTS $t")
    }
    sql(s"DROP DATABASE IF EXISTS $reusedDb")
    spark.stop()
    super.afterAll()
  }

  test("AlterDatabasePropertiesCommand") {
    val plan = sql("ALTER DATABASE default SET DBPROPERTIES (abc = '123')").queryExecution.analyzed
    val operationType = OperationType(plan.nodeName)
    assert(operationType === ALTERDATABASE)
    val tuple = PrivilegesBuilder.build(plan)
    assert(tuple._1.isEmpty)
    assert(tuple._2.size === 1)
    val po = tuple._2.head
    assert(po.actionType === PrivilegeObjectActionType.OTHER)
    assert(po.typ === PrivilegeObjectType.DATABASE)
    assert(po.dbname === "default")
    assert(po.objectName === "default")
    assert(po.columns.isEmpty)
    val accessType = AccessType(po, operationType, isInput = false)
    assert(accessType === AccessType.ALTER)
  }

  test("AlterTableRenameCommand") {
    withTable(s"$reusedDb.efg") { t =>
      sql(s"CREATE TABLE IF NOT EXISTS ${reusedTable}_old" +
        s" (key int, value string) USING parquet")
      // toLowerCase because of: SPARK-38587
      val plan =
        sql(s"ALTER TABLE ${reusedDb.toLowerCase}.${getClass.getSimpleName}_old" +
          s" RENAME TO $t").queryExecution.analyzed
      val operationType = OperationType(plan.nodeName)
      assert(operationType === ALTERTABLE_RENAME)
      val tuple = PrivilegesBuilder.build(plan)
      assert(tuple._1.isEmpty)
      assert(tuple._2.size === 2)
      tuple._2.foreach { po =>
        assert(po.typ === PrivilegeObjectType.TABLE_OR_VIEW)
        assert(po.dbname equalsIgnoreCase reusedDb)
        assert(Set(reusedDb + "_old", "efg").contains(po.objectName))
        assert(po.columns.isEmpty)
        val accessType = AccessType(po, operationType, isInput = false)
        assert(Set(AccessType.CREATE, AccessType.DROP).contains(accessType))
      }
    }
  }

  test("CreateDatabaseCommand") {
    withDatabase("CreateDatabaseCommand") { db =>
      val plan = sql(s"CREATE DATABASE $db").queryExecution.analyzed
      val operationType = OperationType(plan.nodeName)
      assert(operationType === CREATEDATABASE)
      val tuple = PrivilegesBuilder.build(plan)
      assert(tuple._1.isEmpty)
      assert(tuple._2.size === 1)
      val po = tuple._2.head
      assert(po.actionType === PrivilegeObjectActionType.OTHER)
      assert(po.typ === PrivilegeObjectType.DATABASE)
      assert(po.dbname === "CreateDatabaseCommand")
      assert(po.objectName === "CreateDatabaseCommand")
      assert(po.columns.isEmpty)
      val accessType = AccessType(po, operationType, isInput = false)
      assert(accessType === AccessType.CREATE)
    }
  }

  test("DropDatabaseCommand") {
    withDatabase("DropDatabaseCommand") { db =>
      sql(s"CREATE DATABASE $db")
      val plan = sql(s"DROP DATABASE DropDatabaseCommand").queryExecution.analyzed
      val operationType = OperationType(plan.nodeName)
      assert(operationType === DROPDATABASE)
      val tuple = PrivilegesBuilder.build(plan)
      assert(tuple._1.isEmpty)
      assert(tuple._2.size === 1)
      val po = tuple._2.head
      assert(po.actionType === PrivilegeObjectActionType.OTHER)
      assert(po.typ === PrivilegeObjectType.DATABASE)
      assert(po.dbname === "DropDatabaseCommand")
      assert(po.objectName === "DropDatabaseCommand")
      assert(po.columns.isEmpty)
      val accessType = AccessType(po, operationType, isInput = false)
      assert(accessType === AccessType.DROP)
    }
  }

  test("AlterTableAddColumnsCommand") {
    val plan = sql(s"ALTER TABLE $reusedTable" +
      s" ADD COLUMNS (a int)").queryExecution.analyzed
    val operationType = OperationType(plan.nodeName)
    assert(operationType === ALTERTABLE_ADDCOLS)
    val tuple = PrivilegesBuilder.build(plan)
    assert(tuple._1.isEmpty)
    assert(tuple._2.size === 1)
    val po = tuple._2.head
    assert(po.actionType === PrivilegeObjectActionType.OTHER)
    assert(po.typ === PrivilegeObjectType.TABLE_OR_VIEW)
    assert(po.dbname equalsIgnoreCase reusedDb)
    assert(po.objectName === getClass.getSimpleName)
    assert(po.columns.head === "a")
    val accessType = AccessType(po, operationType, isInput = false)
    assert(accessType === AccessType.ALTER)
  }

  test("AlterTableAddPartitionCommand") {
    val plan = sql(s"ALTER TABLE $reusedPartTable ADD IF NOT EXISTS PARTITION (pid=1)")
      .queryExecution.analyzed
    val operationType = OperationType(plan.nodeName)
    assert(operationType === ALTERTABLE_ADDPARTS)
    val tuple = PrivilegesBuilder.build(plan)
    assert(tuple._1.isEmpty)
    assert(tuple._2.size === 1)
    val po = tuple._2.head
    assert(po.actionType === PrivilegeObjectActionType.OTHER)
    assert(po.typ === PrivilegeObjectType.TABLE_OR_VIEW)
    assert(po.dbname equalsIgnoreCase reusedDb)
    assert(po.objectName === reusedPartTable.split("\\.").last)
    assert(po.columns.head === "pid")
    val accessType = AccessType(po, operationType, isInput = false)
    assert(accessType === AccessType.ALTER)
  }

  test("AlterTableDropPartitionCommand") {
    val plan = sql(s"ALTER TABLE $reusedPartTable DROP IF EXISTS PARTITION (pid=1)")
      .queryExecution.analyzed
    val operationType = OperationType(plan.nodeName)
    assert(operationType === ALTERTABLE_DROPPARTS)
    val tuple = PrivilegesBuilder.build(plan)
    assert(tuple._1.isEmpty)
    assert(tuple._2.size === 1)
    val po = tuple._2.head
    assert(po.actionType === PrivilegeObjectActionType.OTHER)
    assert(po.typ === PrivilegeObjectType.TABLE_OR_VIEW)
    assert(po.dbname equalsIgnoreCase reusedDb)
    assert(po.objectName === reusedPartTable.split("\\.").last)
    assert(po.columns.head === "pid")
    val accessType = AccessType(po, operationType, isInput = false)
    assert(accessType === AccessType.ALTER)
  }

  // ALTER TABLE default.StudentInfo PARTITION (age='10') RENAME TO PARTITION (age='15');
  test("AlterTableRenamePartitionCommand") {
    sql(s"ALTER TABLE $reusedPartTable ADD IF NOT EXISTS PARTITION (pid=1)")
    val plan = sql(s"ALTER TABLE $reusedPartTable PARTITION (pid=1) " +
      s"RENAME TO PARTITION (PID=10)")
      .queryExecution.analyzed
    val operationType = OperationType(plan.nodeName)
    assert(operationType === ALTERTABLE_RENAMEPART)
    val tuple = PrivilegesBuilder.build(plan)
    assert(tuple._1.isEmpty)
    assert(tuple._2.size === 1)
    val po = tuple._2.head
    assert(po.actionType === PrivilegeObjectActionType.OTHER)
    assert(po.typ === PrivilegeObjectType.TABLE_OR_VIEW)
    assert(po.dbname equalsIgnoreCase reusedDb)
    assert(po.objectName === reusedPartTable.split("\\.").last)
    assert(po.columns.head === "pid")
    val accessType = AccessType(po, operationType, isInput = false)
    assert(accessType === AccessType.ALTER)
  }

  test("AlterTableSetLocationCommand") {
    sql(s"ALTER TABLE $reusedPartTable ADD IF NOT EXISTS PARTITION (pid=1)")
    val newLoc = spark.conf.get("spark.sql.warehouse.dir") + "/new_location"
    val plan = sql(s"ALTER TABLE $reusedPartTable PARTITION (pid=1)" +
      s" SET LOCATION '$newLoc'")
      .queryExecution.analyzed
    val operationType = OperationType(plan.nodeName)
    assert(operationType === ALTERTABLE_LOCATION)
    val tuple = PrivilegesBuilder.build(plan)
    assert(tuple._1.isEmpty)
    assert(tuple._2.size === 1)
    val po = tuple._2.head
    assert(po.actionType === PrivilegeObjectActionType.OTHER)
    assert(po.typ === PrivilegeObjectType.TABLE_OR_VIEW)
    assert(po.dbname === reusedDb)
    assert(po.objectName === reusedPartTable.split("\\.").last)
    assert(po.columns.head === "pid")
    val accessType = AccessType(po, operationType, isInput = false)
    assert(accessType === AccessType.ALTER)
  }

  test("AlterTable(Un)SetPropertiesCommand") {
    Seq(
      " SET TBLPROPERTIES (key='AlterTableSetPropertiesCommand')",
      "UNSET TBLPROPERTIES (key)").foreach { t =>
      val plan = sql(s"ALTER TABLE $reusedTable" +
        " SET TBLPROPERTIES (key='AlterTableSetPropertiesCommand')")
        .queryExecution.analyzed
      val operationType = OperationType(plan.nodeName)
      assert(operationType === ALTERTABLE_PROPERTIES)
      val tuple = PrivilegesBuilder.build(plan)
      assert(tuple._1.isEmpty)
      assert(tuple._2.size === 1)
      val po = tuple._2.head
      assert(po.actionType === PrivilegeObjectActionType.OTHER)
      assert(po.typ === PrivilegeObjectType.TABLE_OR_VIEW)
      assert(po.dbname === reusedDb)
      assert(po.objectName === reusedTable.split("\\.").last)
      assert(po.columns.isEmpty)
      val accessType = AccessType(po, operationType, isInput = false)
      assert(accessType === AccessType.ALTER)
    }
  }

  test("AlterViewAsCommand") {
    sql(s"CREATE VIEW AlterViewAsCommand AS SELECT * FROM $reusedTable")
    val plan = sql(s"ALTER VIEW AlterViewAsCommand AS SELECT * FROM $reusedPartTable")
      .queryExecution.analyzed
    val operationType = OperationType(plan.nodeName)
    assert(operationType === ALTERVIEW_AS)
    val tuple = PrivilegesBuilder.build(plan)
    assert(tuple._1.size === 1)
    val po0 = tuple._1.head
    assert(po0.actionType === PrivilegeObjectActionType.OTHER)
    assert(po0.typ === PrivilegeObjectType.TABLE_OR_VIEW)
    assert(po0.dbname equalsIgnoreCase reusedDb)
    assert(po0.objectName equalsIgnoreCase reusedPartTable.split("\\.").last)
    // ignore this check as it behaves differently across spark versions
    // assert(po0.columns === Seq("key", "value", "pid"))
    val accessType0 = AccessType(po0, operationType, isInput = true)
    assert(accessType0 === AccessType.SELECT)

    assert(tuple._2.size === 1)
    val po = tuple._2.head
    assert(po.actionType === PrivilegeObjectActionType.OTHER)
    assert(po.typ === PrivilegeObjectType.TABLE_OR_VIEW)
    assert(po.dbname === "default")
    assert(po.objectName === "AlterViewAsCommand")
    assert(po.columns.isEmpty)
    val accessType = AccessType(po, operationType, isInput = false)
    assert(accessType === AccessType.ALTER)
  }

  test("AnalyzeColumnCommand") {
    val plan = sql(s"ANALYZE TABLE $reusedPartTable PARTITION (pid=1)" +
      s" COMPUTE STATISTICS FOR COLUMNS key").queryExecution.analyzed
    val operationType = OperationType(plan.nodeName)
    assert(operationType === ANALYZE_TABLE)
    val tuple = PrivilegesBuilder.build(plan)
    assert(tuple._1.size === 1)
    val po0 = tuple._1.head
    assert(po0.actionType === PrivilegeObjectActionType.OTHER)
    assert(po0.typ === PrivilegeObjectType.TABLE_OR_VIEW)
    assert(po0.dbname equalsIgnoreCase reusedDb)
    assert(po0.objectName equalsIgnoreCase reusedPartTable.split("\\.").last)
    // ignore this check as it behaves differently across spark versions
    assert(po0.columns === Seq("key"))
    val accessType0 = AccessType(po0, operationType, isInput = true)
    assert(accessType0 === AccessType.SELECT)

    assert(tuple._2.size === 0)
  }

  test("AnalyzePartitionCommand") {
    val plan = sql(s"ANALYZE TABLE $reusedPartTable" +
      s" PARTITION (pid = 1) COMPUTE STATISTICS").queryExecution.analyzed
    val operationType = OperationType(plan.nodeName)
    assert(operationType === ANALYZE_TABLE)
    val tuple = PrivilegesBuilder.build(plan)
    assert(tuple._1.size === 1)
    val po0 = tuple._1.head
    assert(po0.actionType === PrivilegeObjectActionType.OTHER)
    assert(po0.typ === PrivilegeObjectType.TABLE_OR_VIEW)
    assert(po0.dbname equalsIgnoreCase reusedDb)
    assert(po0.objectName equalsIgnoreCase reusedPartTable.split("\\.").last)
    // ignore this check as it behaves differently across spark versions
    assert(po0.columns === Seq("pid"))
    val accessType0 = AccessType(po0, operationType, isInput = true)
    assert(accessType0 === AccessType.SELECT)

    assert(tuple._2.size === 0)
  }

  test("AnalyzeTableCommand") {
    val plan = sql(s"ANALYZE TABLE $reusedPartTable COMPUTE STATISTICS")
      .queryExecution.analyzed
    val operationType = OperationType(plan.nodeName)
    assert(operationType === ANALYZE_TABLE)
    val tuple = PrivilegesBuilder.build(plan)
    assert(tuple._1.size === 1)
    val po0 = tuple._1.head
    assert(po0.actionType === PrivilegeObjectActionType.OTHER)
    assert(po0.typ === PrivilegeObjectType.TABLE_OR_VIEW)
    assert(po0.dbname equalsIgnoreCase reusedDb)
    assert(po0.objectName equalsIgnoreCase reusedPartTable.split("\\.").last)
    // ignore this check as it behaves differently across spark versions
    assert(po0.columns.isEmpty)
    val accessType0 = AccessType(po0, operationType, isInput = true)
    assert(accessType0 === AccessType.SELECT)

    assert(tuple._2.size === 0)
  }

  test("AnalyzeTablesCommand") {
    assume(isSparkV32OrGreater)
    val plan = sql(s"ANALYZE TABLES IN $reusedDb COMPUTE STATISTICS")
      .queryExecution.analyzed
    val operationType = OperationType(plan.nodeName)
    assert(operationType === ANALYZE_TABLE)
    val tuple = PrivilegesBuilder.build(plan)
    assert(tuple._1.size === 1)
    val po0 = tuple._1.head
    assert(po0.actionType === PrivilegeObjectActionType.OTHER)
    assert(po0.typ === PrivilegeObjectType.DATABASE)
    assert(po0.dbname equalsIgnoreCase reusedDb)
    assert(po0.objectName equalsIgnoreCase reusedDb)
    // ignore this check as it behaves differently across spark versions
    assert(po0.columns.isEmpty)
    val accessType0 = AccessType(po0, operationType, isInput = true)
    assert(accessType0 === AccessType.SELECT)

    assert(tuple._2.size === 0)
  }

  test("RefreshTableCommand / RefreshTable") {
    val plan = sql(s"REFRESH TABLE $reusedTable").queryExecution.analyzed
    val operationType = OperationType(plan.nodeName)
    assert(operationType === QUERY)
    val tuple = PrivilegesBuilder.build(plan)
    assert(tuple._1.size === 1)
    val po0 = tuple._1.head
    assert(po0.actionType === PrivilegeObjectActionType.OTHER)
    assert(po0.typ === PrivilegeObjectType.TABLE_OR_VIEW)
    assert(po0.dbname equalsIgnoreCase reusedDb)
    assert(po0.objectName equalsIgnoreCase reusedDb)
    assert(po0.columns.isEmpty)
    val accessType0 = AccessType(po0, operationType, isInput = true)
    assert(accessType0 === AccessType.SELECT)

    assert(tuple._2.size === 0)
  }

  test("ShowTablesCommand") {
    val plan = sql(s"SHOW TABLES IN $reusedDb")
      .queryExecution.analyzed
    val operationType = OperationType(plan.nodeName)
    assert(operationType === SHOWTABLES)
    val tuple = PrivilegesBuilder.build(plan)
    assert(tuple._1.size === 1)
    val po0 = tuple._1.head
    assert(po0.actionType === PrivilegeObjectActionType.OTHER)
    assert(po0.typ === PrivilegeObjectType.DATABASE)
    assert(po0.dbname equalsIgnoreCase reusedDb)
    assert(po0.objectName equalsIgnoreCase reusedDb)
    assert(po0.columns.isEmpty)
    val accessType0 = AccessType(po0, operationType, isInput = true)
    assert(accessType0 === AccessType.USE)

    assert(tuple._2.size === 0)
  }

  test("CacheTable") {
    val plan = sql(s"CACHE LAZY TABLE $reusedTable").queryExecution.analyzed
    val operationType = OperationType(plan.nodeName)
    assert(operationType === CREATEVIEW)
    val tuple = PrivilegesBuilder.build(plan)
    if (isSparkV32OrGreater) {
      assert(tuple._1.size === 1)
      val po0 = tuple._1.head
      assert(po0.actionType === PrivilegeObjectActionType.OTHER)
      assert(po0.typ === PrivilegeObjectType.TABLE_OR_VIEW)
      assert(po0.dbname equalsIgnoreCase reusedDb)
      assert(po0.objectName equalsIgnoreCase reusedDb)
      assert(po0.columns.head === "key")
      val accessType0 = AccessType(po0, operationType, isInput = true)
      assert(accessType0 === AccessType.SELECT)
    } else {
      assert(tuple._1.isEmpty)
    }

    assert(tuple._2.size === 1)
    val po = tuple._2.head
    assert(po.actionType === PrivilegeObjectActionType.OTHER)
    assert(po.typ === PrivilegeObjectType.TABLE_OR_VIEW)
    assert(po.dbname equalsIgnoreCase reusedDb)
    assert(po.objectName equalsIgnoreCase reusedDb)
    assert(po.columns.isEmpty)
    val accessType = AccessType(po, operationType, isInput = false)
    assert(accessType === AccessType.CREATE)
  }

  test("CacheTableAsSelect") {
    val plan = sql(s"CACHE TABLE CacheTableAsSelect AS SELECT * FROM $reusedTable")
      .queryExecution.analyzed
    val operationType = OperationType(plan.nodeName)
    assert(operationType === CREATEVIEW)
    val tuple = PrivilegesBuilder.build(plan)
    assert(tuple._1.size === 1)
    val po0 = tuple._1.head
    assert(po0.actionType === PrivilegeObjectActionType.OTHER)
    assert(po0.typ === PrivilegeObjectType.TABLE_OR_VIEW)
    assert(po0.dbname equalsIgnoreCase reusedDb)
    assert(po0.objectName equalsIgnoreCase reusedTable.split("\\.").last)
    if (isSparkV32OrGreater) {
      assert(po0.columns.head === "key")
    } else {
      assert(po0.columns.isEmpty)
    }
    val accessType0 = AccessType(po0, operationType, isInput = true)
    assert(accessType0 === AccessType.SELECT)

    assert(tuple._2.size === 1)
    val po = tuple._2.head
    assert(po.actionType === PrivilegeObjectActionType.OTHER)
    assert(po.typ === PrivilegeObjectType.TABLE_OR_VIEW)
    assert(StringUtils.isEmpty(po.dbname))
    assert(po.objectName === "CacheTableAsSelect")
    assert(po.columns.isEmpty)
    val accessType = AccessType(po, operationType, isInput = false)
    assert(accessType === AccessType.CREATE)
  }

  test("CreateViewCommand") {
    val plan = sql(s"CREATE VIEW CreateViewCommand(a, b) AS SELECT key, value FROM $reusedTable")
      .queryExecution.analyzed
    val operationType = OperationType(plan.nodeName)
    assert(operationType === CREATEVIEW)
    val tuple = PrivilegesBuilder.build(plan)
    assert(tuple._1.size === 1)
    val po0 = tuple._1.head
    assert(po0.actionType === PrivilegeObjectActionType.OTHER)
    assert(po0.typ === PrivilegeObjectType.TABLE_OR_VIEW)
    assert(po0.dbname equalsIgnoreCase reusedDb)
    assert(po0.objectName equalsIgnoreCase reusedTable.split("\\.").last)
    if (isSparkV32OrGreater) {
      assert(po0.columns === Seq("key", "value"))
    } else {
      assert(po0.columns.isEmpty)
    }
    val accessType0 = AccessType(po0, operationType, isInput = true)
    assert(accessType0 === AccessType.SELECT)

    assert(tuple._2.size === 1)
    val po = tuple._2.head
    assert(po.actionType === PrivilegeObjectActionType.OTHER)
    assert(po.typ === PrivilegeObjectType.TABLE_OR_VIEW)
    assert(po.dbname === "default")
    assert(po.objectName === "CreateViewCommand")
    assert(po.columns.isEmpty)
    val accessType = AccessType(po, operationType, isInput = false)
    assert(accessType === AccessType.CREATE)
  }

  test("CreateDataSourceTableCommand") {
    val tableName = s"CreateDataSourceTableCommand"
    withTable(tableName) { _ =>
      val plan = sql(s"CREATE TABLE $tableName(a int, b string) USING parquet")
        .queryExecution.analyzed
      val operationType = OperationType(plan.nodeName)
      assert(operationType === CREATETABLE)
      val tuple = PrivilegesBuilder.build(plan)
      assert(tuple._1.size === 0)
      assert(tuple._2.size === 1)
      val po = tuple._2.head
      assert(po.actionType === PrivilegeObjectActionType.OTHER)
      assert(po.typ === PrivilegeObjectType.TABLE_OR_VIEW)
      assert(po.dbname === "default")
      assert(po.objectName === tableName)
      assert(po.columns.isEmpty)
      val accessType = AccessType(po, operationType, isInput = false)
      assert(accessType === AccessType.CREATE)
    }
  }
}

class InMemoryPrivilegeBuilderSuite extends PrivilegesBuilderSuite {
  override protected val catalogImpl: String = "in-memory"

  // some hive version does not support set database location
  test("AlterDatabaseSetLocationCommand") {
    assume(!isSparkV2)
    val plan = sql("ALTER DATABASE default SET LOCATION 'some where i belong'")
      .queryExecution.analyzed
    val operationType = OperationType(plan.nodeName)
    assert(operationType === ALTERDATABASE_LOCATION)
    val tuple = PrivilegesBuilder.build(plan)
    assert(tuple._1.isEmpty)
    assert(tuple._2.size === 1)
    val po = tuple._2.head
    assert(po.actionType === PrivilegeObjectActionType.OTHER)
    assert(po.typ === PrivilegeObjectType.DATABASE)
    assert(po.dbname === "default")
    assert(po.objectName === "default")
    assert(po.columns.isEmpty)
    val accessType = AccessType(po, operationType, isInput = false)
    assert(accessType === AccessType.ALTER)
  }
}

class HiveCatalogPrivilegeBuilderSuite extends PrivilegesBuilderSuite {
  override protected val catalogImpl: String = "hive"

  test("AlterTableSerDePropertiesCommand") {
    assume(!isSparkV2)
    withTable("AlterTableSerDePropertiesCommand") { t =>
      sql(s"CREATE TABLE $t (key int, pid int) USING hive PARTITIONED BY (pid)")
      sql(s"ALTER TABLE $t ADD IF NOT EXISTS PARTITION (pid=1)")
      val plan = sql(s"ALTER TABLE $t PARTITION (pid=1)" +
        s" SET SERDEPROPERTIES ( key1 = 'some key')")
        .queryExecution.analyzed
      val operationType = OperationType(plan.nodeName)
      assert(operationType === ALTERTABLE_SERDEPROPERTIES)
      val tuple = PrivilegesBuilder.build(plan)
      assert(tuple._1.isEmpty)
      assert(tuple._2.size === 1)
      val po = tuple._2.head
      assert(po.actionType === PrivilegeObjectActionType.OTHER)
      assert(po.typ === PrivilegeObjectType.TABLE_OR_VIEW)
      assert(po.dbname === "default")
      assert(po.objectName === t)
      assert(po.columns.head === "pid")
      val accessType = AccessType(po, operationType, isInput = false)
      assert(accessType === AccessType.ALTER)
    }
  }

  test("CreateTableCommand") {
    withTable("CreateTableCommand") { _ =>
      val plan = sql(s"CREATE TABLE CreateTableCommand(a int, b string) USING hive")
        .queryExecution.analyzed
      val operationType = OperationType(plan.nodeName)
      assert(operationType === CREATETABLE)
      val tuple = PrivilegesBuilder.build(plan)
      assert(tuple._1.size === 0)
      assert(tuple._2.size === 1)
      val po = tuple._2.head
      assert(po.actionType === PrivilegeObjectActionType.OTHER)
      assert(po.typ === PrivilegeObjectType.TABLE_OR_VIEW)
      assert(po.dbname === "default")
      assert(po.objectName === "CreateTableCommand")
      assert(po.columns.isEmpty)
      val accessType = AccessType(po, operationType, isInput = false)
      assert(accessType === AccessType.CREATE)
    }
  }
}
