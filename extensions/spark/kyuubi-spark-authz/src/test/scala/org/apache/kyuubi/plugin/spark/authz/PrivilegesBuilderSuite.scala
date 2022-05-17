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

import scala.reflect.io.File

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{DataFrame, SparkSession, SQLContext}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.sources.{BaseRelation, InsertableRelation, SchemaRelationProvider}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
// scalastyle:off
import org.scalatest.funsuite.AnyFunSuite

import org.apache.kyuubi.plugin.spark.authz.OperationType._
import org.apache.kyuubi.plugin.spark.authz.ranger.AccessType
import org.apache.kyuubi.plugin.spark.authz.util.AuthZUtils

abstract class PrivilegesBuilderSuite extends AnyFunSuite
  with SparkSessionProvider with BeforeAndAfterAll with BeforeAndAfterEach {
// scalastyle:on

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

  protected def checkColumns(plan: LogicalPlan, cols: Seq[String]): Unit = {
    val (in, out) = PrivilegesBuilder.build(plan)
    assert(out.isEmpty, "Queries shall not check output privileges")
    val po = in.head
    assert(po.actionType === PrivilegeObjectActionType.OTHER)
    assert(po.privilegeObjectType === PrivilegeObjectType.TABLE_OR_VIEW)
    assert(po.columns === cols)
  }

  protected def checkColumns(query: String, cols: Seq[String]): Unit = {
    checkColumns(sql(query).queryExecution.optimizedPlan, cols)
  }

  protected val reusedDb: String = getClass.getSimpleName
  protected val reusedTable: String = reusedDb + "." + getClass.getSimpleName
  protected val reusedTableShort: String = reusedTable.split("\\.").last
  protected val reusedPartTable: String = reusedTable + "_part"
  protected val reusedPartTableShort: String = reusedPartTable.split("\\.").last

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

  override def beforeEach(): Unit = {
    sql("CLEAR CACHE")
    super.beforeEach()
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
    assert(po.privilegeObjectType === PrivilegeObjectType.DATABASE)
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
        assert(po.privilegeObjectType === PrivilegeObjectType.TABLE_OR_VIEW)
        assert(po.dbname equalsIgnoreCase reusedDb)
        assert(Set(reusedDb + "_old", "efg").contains(po.objectName))
        assert(po.columns.isEmpty)
        val accessType = ranger.AccessType(po, operationType, isInput = false)
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
      assert(po.privilegeObjectType === PrivilegeObjectType.DATABASE)
      assert(po.dbname === "CreateDatabaseCommand")
      assert(po.objectName === "CreateDatabaseCommand")
      assert(po.columns.isEmpty)
      val accessType = ranger.AccessType(po, operationType, isInput = false)
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
      assert(po.privilegeObjectType === PrivilegeObjectType.DATABASE)
      assert(po.dbname === "DropDatabaseCommand")
      assert(po.objectName === "DropDatabaseCommand")
      assert(po.columns.isEmpty)
      val accessType = ranger.AccessType(po, operationType, isInput = false)
      assert(accessType === AccessType.DROP)
    }
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
    assert(po.privilegeObjectType === PrivilegeObjectType.TABLE_OR_VIEW)
    assert(po.dbname equalsIgnoreCase reusedDb)
    assert(po.objectName === reusedPartTableShort)
    assert(po.columns.head === "pid")
    val accessType = ranger.AccessType(po, operationType, isInput = false)
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
    assert(po.privilegeObjectType === PrivilegeObjectType.TABLE_OR_VIEW)
    assert(po.dbname equalsIgnoreCase reusedDb)
    assert(po.objectName === reusedPartTableShort)
    assert(po.columns.head === "pid")
    val accessType = ranger.AccessType(po, operationType, isInput = false)
    assert(accessType === AccessType.ALTER)
  }

  test("AlterTableRecoverPartitionsCommand") {
    // AlterTableRecoverPartitionsCommand exists in the version below 3.2
    assume(!isSparkV32OrGreater)
    val tableName = reusedDb + "." + "TableToMsck"
    withTable(tableName) { _ =>
      sql(
        s"""
           |CREATE TABLE $tableName
           |(key int, value string, pid string)
           |USING parquet
           |PARTITIONED BY (pid)""".stripMargin)
      val sqlStr =
        s"""
           |MSCK REPAIR TABLE $tableName
           |""".stripMargin
      val plan = sql(sqlStr).queryExecution.analyzed
      val operationType = OperationType(plan.nodeName)
      assert(operationType === MSCK)
      val (inputs, outputs) = PrivilegesBuilder.build(plan)

      assert(inputs.isEmpty)

      assert(outputs.size === 1)
      outputs.foreach { po =>
        assert(po.actionType === PrivilegeObjectActionType.OTHER)
        assert(po.privilegeObjectType === PrivilegeObjectType.TABLE_OR_VIEW)
        assert(po.dbname equalsIgnoreCase reusedDb)
        assert(po.objectName equalsIgnoreCase tableName.split("\\.").last)
        assert(po.columns.isEmpty)
        val accessType = ranger.AccessType(po, operationType, isInput = false)
        assert(accessType === AccessType.ALTER)
      }
    }
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
    assert(po.privilegeObjectType === PrivilegeObjectType.TABLE_OR_VIEW)
    assert(po.dbname equalsIgnoreCase reusedDb)
    assert(po.objectName === reusedPartTableShort)
    assert(po.columns.head === "pid")
    val accessType = ranger.AccessType(po, operationType, isInput = false)
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
    assert(po.privilegeObjectType === PrivilegeObjectType.TABLE_OR_VIEW)
    assert(po.dbname === reusedDb)
    assert(po.objectName === reusedPartTableShort)
    assert(po.columns.head === "pid")
    val accessType = ranger.AccessType(po, operationType, isInput = false)
    assert(accessType === AccessType.ALTER)
  }

  test("AlterTable(Un)SetPropertiesCommand") {
    Seq(
      " SET TBLPROPERTIES (key='AlterTableSetPropertiesCommand')",
      "UNSET TBLPROPERTIES (key)").foreach { clause =>
      val plan = sql(s"ALTER TABLE $reusedTable $clause")
        .queryExecution.analyzed
      val operationType = OperationType(plan.nodeName)
      assert(operationType === ALTERTABLE_PROPERTIES)
      val tuple = PrivilegesBuilder.build(plan)
      assert(tuple._1.isEmpty)
      assert(tuple._2.size === 1)
      val po = tuple._2.head
      assert(po.actionType === PrivilegeObjectActionType.OTHER)
      assert(po.privilegeObjectType === PrivilegeObjectType.TABLE_OR_VIEW)
      assert(po.dbname === reusedDb)
      assert(po.objectName === reusedTable.split("\\.").last)
      assert(po.columns.isEmpty)
      val accessType = ranger.AccessType(po, operationType, isInput = false)
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
    assert(po0.privilegeObjectType === PrivilegeObjectType.TABLE_OR_VIEW)
    assert(po0.dbname equalsIgnoreCase reusedDb)
    assert(po0.objectName equalsIgnoreCase reusedPartTableShort)
    // ignore this check as it behaves differently across spark versions
    // assert(po0.columns === Seq("key", "value", "pid"))
    val accessType0 = ranger.AccessType(po0, operationType, isInput = true)
    assert(accessType0 === AccessType.SELECT)

    assert(tuple._2.size === 1)
    val po = tuple._2.head
    assert(po.actionType === PrivilegeObjectActionType.OTHER)
    assert(po.privilegeObjectType === PrivilegeObjectType.TABLE_OR_VIEW)
    assert(po.dbname === (if (isSparkV2) null else "default"))
    assert(po.objectName === "AlterViewAsCommand")
    assert(po.columns.isEmpty)
    val accessType = ranger.AccessType(po, operationType, isInput = false)
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
    assert(po0.privilegeObjectType === PrivilegeObjectType.TABLE_OR_VIEW)
    assert(po0.dbname equalsIgnoreCase reusedDb)
    assert(po0.objectName equalsIgnoreCase reusedPartTableShort)
    // ignore this check as it behaves differently across spark versions
    assert(po0.columns === Seq("key"))
    val accessType0 = ranger.AccessType(po0, operationType, isInput = true)
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
    assert(po0.privilegeObjectType === PrivilegeObjectType.TABLE_OR_VIEW)
    assert(po0.dbname equalsIgnoreCase reusedDb)
    assert(po0.objectName equalsIgnoreCase reusedPartTableShort)
    // ignore this check as it behaves differently across spark versions
    assert(po0.columns === Seq("pid"))
    val accessType0 = ranger.AccessType(po0, operationType, isInput = true)
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
    assert(po0.privilegeObjectType === PrivilegeObjectType.TABLE_OR_VIEW)
    assert(po0.dbname equalsIgnoreCase reusedDb)
    assert(po0.objectName equalsIgnoreCase reusedPartTableShort)
    // ignore this check as it behaves differently across spark versions
    assert(po0.columns.isEmpty)
    val accessType0 = ranger.AccessType(po0, operationType, isInput = true)
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
    assert(po0.privilegeObjectType === PrivilegeObjectType.DATABASE)
    assert(po0.dbname equalsIgnoreCase reusedDb)
    assert(po0.objectName equalsIgnoreCase reusedDb)
    // ignore this check as it behaves differently across spark versions
    assert(po0.columns.isEmpty)
    val accessType0 = ranger.AccessType(po0, operationType, isInput = true)
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
    assert(po0.privilegeObjectType === PrivilegeObjectType.TABLE_OR_VIEW)
    assert(po0.dbname equalsIgnoreCase reusedDb)
    assert(po0.objectName equalsIgnoreCase reusedDb)
    assert(po0.columns.isEmpty)
    val accessType0 = ranger.AccessType(po0, operationType, isInput = true)
    assert(accessType0 === AccessType.SELECT)

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
      assert(po0.privilegeObjectType === PrivilegeObjectType.TABLE_OR_VIEW)
      assert(po0.dbname equalsIgnoreCase reusedDb)
      assert(po0.objectName equalsIgnoreCase reusedDb)
      assert(po0.columns.head === "key")
      val accessType0 = ranger.AccessType(po0, operationType, isInput = true)
      assert(accessType0 === AccessType.SELECT)
    } else {
      assert(tuple._1.isEmpty)
    }

    assert(tuple._2.size === 1)
    val po = tuple._2.head
    assert(po.actionType === PrivilegeObjectActionType.OTHER)
    assert(po.privilegeObjectType === PrivilegeObjectType.TABLE_OR_VIEW)
    assert(po.dbname equalsIgnoreCase reusedDb)
    assert(po.objectName equalsIgnoreCase reusedDb)
    assert(po.columns.isEmpty)
    val accessType = ranger.AccessType(po, operationType, isInput = false)
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
    assert(po0.privilegeObjectType === PrivilegeObjectType.TABLE_OR_VIEW)
    assert(po0.dbname equalsIgnoreCase reusedDb)
    assert(po0.objectName equalsIgnoreCase reusedTable.split("\\.").last)
    if (isSparkV32OrGreater) {
      assert(po0.columns.head === "key")
    } else {
      assert(po0.columns.isEmpty)
    }
    val accessType0 = ranger.AccessType(po0, operationType, isInput = true)
    assert(accessType0 === AccessType.SELECT)

    assert(tuple._2.size === 1)
    val po = tuple._2.head
    assert(po.actionType === PrivilegeObjectActionType.OTHER)
    assert(po.privilegeObjectType === PrivilegeObjectType.TABLE_OR_VIEW)
    assert(StringUtils.isEmpty(po.dbname))
    assert(po.objectName === "CacheTableAsSelect")
    assert(po.columns.isEmpty)
    val accessType = ranger.AccessType(po, operationType, isInput = false)
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
    assert(po0.privilegeObjectType === PrivilegeObjectType.TABLE_OR_VIEW)
    assert(po0.dbname equalsIgnoreCase reusedDb)
    assert(po0.objectName equalsIgnoreCase reusedTable.split("\\.").last)
    if (isSparkV32OrGreater) {
      assert(po0.columns === Seq("key", "value"))
    } else {
      assert(po0.columns.isEmpty)
    }
    val accessType0 = ranger.AccessType(po0, operationType, isInput = true)
    assert(accessType0 === AccessType.SELECT)

    assert(tuple._2.size === 1)
    val po = tuple._2.head
    assert(po.actionType === PrivilegeObjectActionType.OTHER)
    assert(po.privilegeObjectType === PrivilegeObjectType.TABLE_OR_VIEW)
    assert(po.dbname === (if (isSparkV2) null else "default"))
    assert(po.objectName === "CreateViewCommand")
    assert(po.columns.isEmpty)
    val accessType = ranger.AccessType(po, operationType, isInput = false)
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
      assert(po.privilegeObjectType === PrivilegeObjectType.TABLE_OR_VIEW)
      assert(po.dbname === (if (isSparkV2) null else "default"))
      assert(po.objectName === tableName)
      assert(po.columns.isEmpty)
      val accessType = ranger.AccessType(po, operationType, isInput = false)
      assert(accessType === AccessType.CREATE)
    }
  }

  test("CreateFunctionCommand") {
    val plan = sql("CREATE FUNCTION CreateFunctionCommand AS 'class_name'")
      .queryExecution.analyzed
    val operationType = OperationType(plan.nodeName)
    assert(operationType === CREATEFUNCTION)
    val tuple = PrivilegesBuilder.build(plan)
    assert(tuple._1.size === 0)
    assert(tuple._2.size === 1)
    val po = tuple._2.head
    assert(po.actionType === PrivilegeObjectActionType.OTHER)
    assert(po.privilegeObjectType === PrivilegeObjectType.FUNCTION)
    val db = if (isSparkV33OrGreater) "default" else null
    assert(po.dbname === db)
    assert(po.objectName === "CreateFunctionCommand")
    assert(po.columns.isEmpty)
    val accessType = ranger.AccessType(po, operationType, isInput = false)
    assert(accessType === AccessType.CREATE)
  }

  test("DropFunctionCommand") {
    sql("CREATE FUNCTION DropFunctionCommand AS 'class_name'")
    val plan = sql("DROP FUNCTION DropFunctionCommand")
      .queryExecution.analyzed
    val operationType = OperationType(plan.nodeName)
    assert(operationType === DROPFUNCTION)
    val tuple = PrivilegesBuilder.build(plan)
    assert(tuple._1.size === 0)
    assert(tuple._2.size === 1)
    val po = tuple._2.head
    assert(po.actionType === PrivilegeObjectActionType.OTHER)
    assert(po.privilegeObjectType === PrivilegeObjectType.FUNCTION)
    val db = if (isSparkV33OrGreater) "default" else null
    assert(po.dbname === db)
    assert(po.objectName === "DropFunctionCommand")
    assert(po.columns.isEmpty)
    val accessType = ranger.AccessType(po, operationType, isInput = false)
    assert(accessType === AccessType.DROP)
  }

  test("RefreshFunctionCommand") {
    assume(AuthZUtils.isSparkVersionAtLeast("3.1"))
    sql(s"CREATE FUNCTION RefreshFunctionCommand AS '${getClass.getCanonicalName}'")
    val plan = sql("REFRESH FUNCTION RefreshFunctionCommand")
      .queryExecution.analyzed
    val operationType = OperationType(plan.nodeName)
    assert(operationType === RELOADFUNCTION)
    val tuple = PrivilegesBuilder.build(plan)
    assert(tuple._1.size === 0)
    assert(tuple._2.size === 1)
    val po = tuple._2.head
    assert(po.actionType === PrivilegeObjectActionType.OTHER)
    assert(po.privilegeObjectType === PrivilegeObjectType.FUNCTION)
    val db = if (isSparkV33OrGreater) "default" else null
    assert(po.dbname === db)
    assert(po.objectName === "RefreshFunctionCommand")
    assert(po.columns.isEmpty)
    val accessType = ranger.AccessType(po, operationType, isInput = false)
    assert(accessType === AccessType.NONE)
  }

  test("CreateTableLikeCommand") {
    withTable(reusedDb + ".CreateTableLikeCommand") { t =>
      val plan = sql(s"CREATE TABLE $t LIKE $reusedTable").queryExecution.analyzed
      val operationType = OperationType(plan.nodeName)

      assert(operationType === CREATETABLE)
      val tuple = PrivilegesBuilder.build(plan)
      assert(tuple._1.size === 1)
      val po0 = tuple._1.head
      assert(po0.actionType === PrivilegeObjectActionType.OTHER)
      assert(po0.privilegeObjectType === PrivilegeObjectType.TABLE_OR_VIEW)
      assert(po0.dbname equalsIgnoreCase reusedDb)
      assert(po0.objectName equalsIgnoreCase reusedTable.split("\\.").last)
      assert(po0.columns.isEmpty)
      val accessType0 = ranger.AccessType(po0, operationType, isInput = true)
      assert(accessType0 === AccessType.SELECT)

      assert(tuple._2.size === 1)
      val po = tuple._2.head
      assert(po.actionType === PrivilegeObjectActionType.OTHER)
      assert(po.privilegeObjectType === PrivilegeObjectType.TABLE_OR_VIEW)
      assert(po.dbname equalsIgnoreCase reusedDb)
      assert(po.objectName === "CreateTableLikeCommand")
      assert(po.columns.isEmpty)
      val accessType = ranger.AccessType(po, operationType, isInput = false)
      assert(accessType === AccessType.CREATE)
    }
  }

  test("CreateTempViewUsing") {
    val plan = sql("CREATE TEMPORARY VIEW CreateTempViewUsing (a int, b string) USING parquet")
      .queryExecution.analyzed
    val operationType = OperationType(plan.nodeName)

    assert(operationType === CREATEVIEW)
    val tuple = PrivilegesBuilder.build(plan)
    assert(tuple._1.size === 0)

    assert(tuple._2.size === 1)
    val po = tuple._2.head
    assert(po.actionType === PrivilegeObjectActionType.OTHER)
    assert(po.privilegeObjectType === PrivilegeObjectType.TABLE_OR_VIEW)
    assert(po.dbname === null)
    assert(po.objectName === "CreateTempViewUsing")
    assert(po.columns.isEmpty)
    val accessType = ranger.AccessType(po, operationType, isInput = false)
    assert(accessType === AccessType.CREATE)
  }

  test("DescribeColumnCommand") {
    val plan = sql(s"DESC TABLE $reusedTable key").queryExecution.analyzed
    val operationType = OperationType(plan.nodeName)
    assert(operationType === DESCTABLE)
    val tuple = PrivilegesBuilder.build(plan)
    assert(tuple._1.size === 1)
    val po = tuple._1.head
    assert(po.actionType === PrivilegeObjectActionType.OTHER)
    assert(po.privilegeObjectType === PrivilegeObjectType.TABLE_OR_VIEW)
    assert(po.dbname equalsIgnoreCase reusedDb)
    assert(po.objectName equalsIgnoreCase reusedTable.split("\\.").last)
    assert(po.columns === Seq("key"))
    val accessType = ranger.AccessType(po, operationType, isInput = false)
    assert(accessType === AccessType.SELECT)

    assert(tuple._2.size === 0)
  }

  test("DescribeTableCommand") {
    val plan = sql(s"DESC TABLE $reusedTable").queryExecution.analyzed
    val operationType = OperationType(plan.nodeName)
    assert(operationType === DESCTABLE)
    val tuple = PrivilegesBuilder.build(plan)
    assert(tuple._1.size === 1)
    val po = tuple._1.head
    assert(po.actionType === PrivilegeObjectActionType.OTHER)
    assert(po.privilegeObjectType === PrivilegeObjectType.TABLE_OR_VIEW)
    assert(po.dbname equalsIgnoreCase reusedDb)
    assert(po.objectName equalsIgnoreCase reusedTable.split("\\.").last)
    assert(po.columns.isEmpty)
    val accessType = ranger.AccessType(po, operationType, isInput = false)
    assert(accessType === AccessType.SELECT)

    assert(tuple._2.size === 0)
  }

  test("DescribeDatabaseCommand") {
    val plan = sql(s"DESC DATABASE $reusedDb").queryExecution.analyzed
    val operationType = OperationType(plan.nodeName)
    assert(operationType === DESCDATABASE)
    val tuple = PrivilegesBuilder.build(plan)
    assert(tuple._1.size === 1)
    val po = tuple._1.head
    assert(po.actionType === PrivilegeObjectActionType.OTHER)
    assert(po.privilegeObjectType === PrivilegeObjectType.DATABASE)
    assert(po.dbname equalsIgnoreCase reusedDb)
    assert(po.objectName equalsIgnoreCase reusedDb)
    assert(po.columns.isEmpty)
    val accessType = ranger.AccessType(po, operationType, isInput = false)
    assert(accessType === AccessType.USE)

    assert(tuple._2.size === 0)
  }

  test("SetDatabaseCommand") {
    try {
      val plan = sql(s"USE $reusedDb").queryExecution.analyzed
      val operationType = OperationType(plan.nodeName)
      assert(operationType === SWITCHDATABASE)
      val tuple = PrivilegesBuilder.build(plan)
      assert(tuple._1.size === 1)

      val po0 = tuple._1.head
      assert(po0.actionType === PrivilegeObjectActionType.OTHER)
      assert(po0.privilegeObjectType === PrivilegeObjectType.DATABASE)
      assert(po0.dbname equalsIgnoreCase reusedDb)
      assert(po0.objectName equalsIgnoreCase reusedDb)
      assert(po0.columns.isEmpty)
      val accessType0 = ranger.AccessType(po0, operationType, isInput = false)
      assert(accessType0 === AccessType.USE)

      assert(tuple._2.size === 0)
    } finally {
      sql("USE default")
    }
  }

  test("TruncateTableCommand") {
    val plan = sql(s"TRUNCATE TABLE $reusedPartTable PARTITION (pid=1)")
      .queryExecution.analyzed
    val operationType = OperationType(plan.nodeName)
    assert(operationType === TRUNCATETABLE)
    val tuple = PrivilegesBuilder.build(plan)
    assert(tuple._1.isEmpty)
    assert(tuple._2.size === 1)
    val po = tuple._2.head
    assert(po.actionType === PrivilegeObjectActionType.OTHER)
    assert(po.privilegeObjectType === PrivilegeObjectType.TABLE_OR_VIEW)
    assert(po.dbname equalsIgnoreCase reusedDb)
    assert(po.objectName === reusedPartTableShort)
    assert(po.columns.head === "pid")
    val accessType = ranger.AccessType(po, operationType, isInput = false)
    assert(accessType === AccessType.UPDATE)
  }

  test("ShowColumnsCommand") {
    val plan = sql(s"SHOW COLUMNS IN $reusedTable").queryExecution.analyzed
    val operationType = OperationType(plan.nodeName)
    assert(operationType === SHOWCOLUMNS)
    val tuple = PrivilegesBuilder.build(plan)
    assert(tuple._1.size === 1)
    val po0 = tuple._1.head
    assert(po0.actionType === PrivilegeObjectActionType.OTHER)
    assert(po0.privilegeObjectType === PrivilegeObjectType.TABLE_OR_VIEW)
    assert(po0.dbname equalsIgnoreCase reusedDb)
    assert(po0.objectName equalsIgnoreCase reusedTable.split("\\.").last)
    assert(po0.columns.isEmpty)
    val accessType0 = ranger.AccessType(po0, operationType, isInput = true)

    assert(accessType0 === AccessType.SELECT)
    assert(tuple._2.size === 0)
  }

  test("ShowCreateTableCommand") {
    val plan = sql(s"SHOW CREATE TABLE $reusedTable").queryExecution.analyzed
    val operationType = OperationType(plan.nodeName)
    assert(operationType === SHOW_CREATETABLE)
    val tuple = PrivilegesBuilder.build(plan)
    assert(tuple._1.size === 1)
    val po0 = tuple._1.head
    assert(po0.actionType === PrivilegeObjectActionType.OTHER)
    assert(po0.privilegeObjectType === PrivilegeObjectType.TABLE_OR_VIEW)
    assert(po0.dbname equalsIgnoreCase reusedDb)
    assert(po0.objectName equalsIgnoreCase reusedTable.split("\\.").last)
    assert(po0.columns.isEmpty)
    val accessType0 = ranger.AccessType(po0, operationType, isInput = true)
    assert(accessType0 === AccessType.SELECT)

    assert(tuple._2.size === 0)
  }

  test("ShowTablePropertiesCommand") {
    val plan = sql(s"SHOW TBLPROPERTIES $reusedTable ").queryExecution.analyzed
    val operationType = OperationType(plan.nodeName)
    assert(operationType === SHOW_TBLPROPERTIES)
    val tuple = PrivilegesBuilder.build(plan)
    assert(tuple._1.size === 1)
    val po0 = tuple._1.head
    assert(po0.actionType === PrivilegeObjectActionType.OTHER)
    assert(po0.privilegeObjectType === PrivilegeObjectType.TABLE_OR_VIEW)
    assert(po0.dbname equalsIgnoreCase reusedDb)
    assert(po0.objectName equalsIgnoreCase reusedTable.split("\\.").last)
    assert(po0.columns.isEmpty)
    val accessType0 = ranger.AccessType(po0, operationType, isInput = true)
    assert(accessType0 === AccessType.SELECT)

    assert(tuple._2.size === 0)
  }

  test("ShowFunctionsCommand") {
    val plan = sql("SHOW FUNCTIONS").queryExecution.analyzed
    val operationType = OperationType(plan.nodeName)
    assert(operationType === SHOWFUNCTIONS)
    val tuple = PrivilegesBuilder.build(plan)
    assert(tuple._1.size === 0)
    assert(tuple._2.size === 0)
  }

  test("ShowPartitionsCommand") {
    val plan = sql(s"SHOW PARTITIONS $reusedPartTable PARTITION (pid=1)")
      .queryExecution.analyzed
    val operationType = OperationType(plan.nodeName)
    assert(operationType === SHOWPARTITIONS)
    val tuple = PrivilegesBuilder.build(plan)
    assert(tuple._1.size === 1)
    val po0 = tuple._1.head
    assert(po0.actionType === PrivilegeObjectActionType.OTHER)
    assert(po0.privilegeObjectType === PrivilegeObjectType.TABLE_OR_VIEW)
    assert(po0.dbname equalsIgnoreCase reusedDb)
    assert(po0.objectName equalsIgnoreCase reusedPartTableShort)
    assert(po0.columns === Seq("pid"))
    val accessType0 = ranger.AccessType(po0, operationType, isInput = true)
    assert(accessType0 === AccessType.SELECT)

    assert(tuple._2.size === 0)
  }

  test("RepairTableCommand") {
    // only spark 3.2 or greater has RepairTableCommand
    assume(isSparkV32OrGreater)
    val tableName = reusedDb + "." + "TableToRepair"
    withTable(tableName) { _ =>
      sql(
        s"""
           |CREATE TABLE $tableName
           |(key int, value string, pid string)
           |USING parquet
           |PARTITIONED BY (pid)""".stripMargin)
      val sqlStr =
        s"""
           |MSCK REPAIR TABLE $tableName
           |""".stripMargin
      val plan = sql(sqlStr).queryExecution.analyzed
      val operationType = OperationType(plan.nodeName)
      assert(operationType === MSCK)
      val (inputs, outputs) = PrivilegesBuilder.build(plan)

      assert(inputs.isEmpty)

      assert(outputs.size === 1)
      outputs.foreach { po =>
        assert(po.actionType === PrivilegeObjectActionType.INSERT)
        assert(po.privilegeObjectType === PrivilegeObjectType.TABLE_OR_VIEW)
        assert(po.dbname equalsIgnoreCase reusedDb)
        assert(po.objectName equalsIgnoreCase tableName.split("\\.").last)
        assert(po.columns.isEmpty)
        val accessType = ranger.AccessType(po, operationType, isInput = false)
        assert(accessType === AccessType.UPDATE)
      }
    }
  }

  test("Query: Star") {
    val plan = sql(s"SELECT * FROM $reusedTable").queryExecution.optimizedPlan
    val po = PrivilegesBuilder.build(plan)._1.head
    assert(po.actionType === PrivilegeObjectActionType.OTHER)
    assert(po.privilegeObjectType === PrivilegeObjectType.TABLE_OR_VIEW)
    assert(po.dbname equalsIgnoreCase reusedDb)
    assert(po.objectName equalsIgnoreCase reusedTableShort)
    assert(po.columns.take(2) === Seq("key", "value"))
  }

  test("Query: Projection") {
    checkColumns(s"SELECT key FROM $reusedTable", Seq("key"))
  }

  test("Query: Alias") {
    checkColumns(s"select key as you from $reusedTable", Seq("key"))
  }

  test("Query: Literal") {
    checkColumns(s"select 1 from $reusedTable", Nil)
  }

  test("Query: Function") {
    checkColumns(
      s"select coalesce(max(key), pid, 1) from $reusedPartTable group by pid",
      Seq("key", "pid"))
  }

  test("Query: CTE") {
    assume(!isSparkV2)
    checkColumns(
      s"""
         |with t(c) as (select coalesce(max(key), pid, 1) from $reusedPartTable group by pid)
         |select c from t where c = 1""".stripMargin,
      Seq("key", "pid"))
  }

  test("Query: Nested query") {
    checkColumns(s"select max(key) from (select * from $reusedPartTable) t", Seq("key"))
  }

  test("Query: Where") {
    checkColumns(
      s"select key from $reusedPartTable where value = 1",
      Seq("key", "value"))
  }

  test("Query: Subquery") {
    val plan = sql(
      s"""
         |SELECT key
         |FROM $reusedTable
         |WHERE value IN (SELECT value
         |                FROM $reusedPartTable WHERE pid > 1)
         |""".stripMargin).queryExecution.optimizedPlan

    val (in, _) = PrivilegesBuilder.build(plan)

    assert(in.size === 2)
    assert(in.find(_.objectName equalsIgnoreCase reusedTableShort).head.columns ===
      Seq("key", "value"))
    assert(in.find(_.objectName equalsIgnoreCase reusedPartTableShort).head.columns ===
      Seq("value", "pid"))
  }

  test("Query: Subquery With Window") {
    val plan = sql(
      s"""
         |SELECT value, rank FROM(
         |SELECT *,
         |RANK() OVER (PARTITION BY key ORDER BY pid) AS rank
         |FROM $reusedPartTable)
         |""".stripMargin).queryExecution.optimizedPlan
    val operationType = OperationType(plan.nodeName)
    assert(operationType === QUERY)
    val tuple = PrivilegesBuilder.build(plan)
    assert(tuple._1.size === 1)
    tuple._1.foreach { po =>
      assert(po.actionType === PrivilegeObjectActionType.OTHER)
      assert(po.privilegeObjectType === PrivilegeObjectType.TABLE_OR_VIEW)
      assert(po.dbname equalsIgnoreCase reusedDb)
      assert(po.objectName startsWith reusedTableShort.toLowerCase)
      assert(
        po.columns === Seq("value", "pid", "key"),
        s"$reusedPartTable both 'key', 'value' and 'pid' should be authenticated")
      val accessType = ranger.AccessType(po, operationType, isInput = true)
      assert(accessType === AccessType.SELECT)
    }
    assert(tuple._2.size === 0)
  }

  test("Query: Subquery With Order") {
    val plan = sql(
      s"""
         |SELECT value FROM(
         |SELECT *
         |FROM $reusedPartTable
         |ORDER BY key, pid)
         |""".stripMargin).queryExecution.optimizedPlan
    val operationType = OperationType(plan.nodeName)
    assert(operationType === QUERY)
    val tuple = PrivilegesBuilder.build(plan)
    assert(tuple._1.size === 1)
    tuple._1.foreach { po =>
      assert(po.actionType === PrivilegeObjectActionType.OTHER)
      assert(po.privilegeObjectType === PrivilegeObjectType.TABLE_OR_VIEW)
      assert(po.dbname equalsIgnoreCase reusedDb)
      assert(po.objectName startsWith reusedTableShort.toLowerCase)
      assert(
        po.columns === Seq("value", "key", "pid"),
        s"$reusedPartTable both 'key', 'value' and 'pid' should be authenticated")
      val accessType = ranger.AccessType(po, operationType, isInput = true)
      assert(accessType === AccessType.SELECT)
    }
    assert(tuple._2.size === 0)
  }

  test("Query: INNER JOIN") {
    val sqlStr =
      s"""
         |SELECT t1.key, t1.value, t2.value
         |    FROM $reusedTable t1
         |    INNER JOIN $reusedPartTable t2
         |    ON t1.key = t2.key
         |    """.stripMargin

    val plan = sql(sqlStr).queryExecution.optimizedPlan
    val operationType = OperationType(plan.nodeName)
    assert(operationType === QUERY)
    val tuple = PrivilegesBuilder.build(plan)

    assert(tuple._1.size === 2)
    tuple._1.foreach { po =>
      assert(po.actionType === PrivilegeObjectActionType.OTHER)
      assert(po.privilegeObjectType === PrivilegeObjectType.TABLE_OR_VIEW)
      assert(po.dbname equalsIgnoreCase reusedDb)
      assert(po.objectName startsWith reusedTableShort.toLowerCase)
      assert(
        po.columns === Seq("key", "value"),
        s"$reusedPartTable 'key' is the join key and 'pid' is omitted")
      val accessType = ranger.AccessType(po, operationType, isInput = true)
      assert(accessType === AccessType.SELECT)
    }
    assert(tuple._2.size === 0)
  }

  test("Query: WHERE Without Project") {
    val sqlStr =
      s"""
         |SELECT t1.key, t1.value
         |    FROM $reusedTable t1
         |    WHERE key < 1
         |    """.stripMargin

    val plan = sql(sqlStr).queryExecution.optimizedPlan
    val operationType = OperationType(plan.nodeName)
    assert(operationType === QUERY)
    val tuple = PrivilegesBuilder.build(plan)

    assert(tuple._1.size === 1)
    tuple._1.foreach { po =>
      assert(po.actionType === PrivilegeObjectActionType.OTHER)
      assert(po.privilegeObjectType === PrivilegeObjectType.TABLE_OR_VIEW)
      assert(po.dbname equalsIgnoreCase reusedDb)
      assert(po.objectName startsWith reusedTableShort.toLowerCase)
      assert(
        po.columns === Seq("key", "value"),
        s"$reusedPartTable both 'key' and 'value' should be authenticated")
      val accessType = ranger.AccessType(po, operationType, isInput = true)
      assert(accessType === AccessType.SELECT)
    }
    assert(tuple._2.size === 0)
  }

  test("Query: JOIN Without Project") {
    val sqlStr =
      s"""
         |SELECT t1.key, t1.value, t2.key, t2.value
         |    FROM $reusedTable t1
         |    INNER JOIN $reusedPartTable t2
         |    ON t1.key = t2.key
         |    """.stripMargin

    val plan = sql(sqlStr).queryExecution.optimizedPlan
    val operationType = OperationType(plan.nodeName)
    assert(operationType === QUERY)
    val tuple = PrivilegesBuilder.build(plan)

    assert(tuple._1.size === 2)
    tuple._1.foreach { po =>
      assert(po.actionType === PrivilegeObjectActionType.OTHER)
      assert(po.privilegeObjectType === PrivilegeObjectType.TABLE_OR_VIEW)
      assert(po.dbname equalsIgnoreCase reusedDb)
      assert(po.objectName startsWith reusedTableShort.toLowerCase)
      assert(
        po.columns === Seq("key", "value"),
        s"$reusedPartTable both 'key' and 'value' should be authenticated")
      val accessType = ranger.AccessType(po, operationType, isInput = true)
      assert(accessType === AccessType.SELECT)
    }
    assert(tuple._2.size === 0)
  }

  test("Query: Window Without Project") {
    val plan = sql(
      s"""
         |SELECT key,
         |RANK() OVER (PARTITION BY key ORDER BY value) AS rank
         |FROM $reusedTable
         |""".stripMargin).queryExecution.optimizedPlan
    val operationType = OperationType(plan.nodeName)
    assert(operationType === QUERY)
    val tuple = PrivilegesBuilder.build(plan)
    assert(tuple._1.size === 1)
    tuple._1.foreach { po =>
      assert(po.actionType === PrivilegeObjectActionType.OTHER)
      assert(po.privilegeObjectType === PrivilegeObjectType.TABLE_OR_VIEW)
      assert(po.dbname equalsIgnoreCase reusedDb)
      assert(po.objectName startsWith reusedTableShort.toLowerCase)
      assert(
        po.columns === Seq("key", "value"),
        s"$reusedPartTable both 'key' and 'value' should be authenticated")
      val accessType = ranger.AccessType(po, operationType, isInput = true)
      assert(accessType === AccessType.SELECT)
    }
    assert(tuple._2.size === 0)
  }

  test("Query: Order By Without Project") {
    val plan = sql(
      s"""
         |SELECT key
         |FROM $reusedPartTable
         |ORDER BY key, value, pid
         |""".stripMargin).queryExecution.optimizedPlan
    val operationType = OperationType(plan.nodeName)
    assert(operationType === QUERY)
    val tuple = PrivilegesBuilder.build(plan)
    assert(tuple._1.size === 1)
    tuple._1.foreach { po =>
      assert(po.actionType === PrivilegeObjectActionType.OTHER)
      assert(po.privilegeObjectType === PrivilegeObjectType.TABLE_OR_VIEW)
      assert(po.dbname equalsIgnoreCase reusedDb)
      assert(po.objectName startsWith reusedTableShort.toLowerCase)
      assert(
        po.columns === Seq("key", "value", "pid"),
        s"$reusedPartTable both 'key', 'value' and 'pid' should be authenticated")
      val accessType = ranger.AccessType(po, operationType, isInput = true)
      assert(accessType === AccessType.SELECT)
    }
    assert(tuple._2.size === 0)
  }

  test("Query: Union") {
    val plan = sql(
      s"""
         |SELECT key, value
         |FROM $reusedTable
         |UNION
         |SELECT pid, value
         |FROM $reusedPartTable
         |""".stripMargin).queryExecution.optimizedPlan
    val (in, _) = PrivilegesBuilder.build(plan)
    assert(in.size === 2)
  }

  test("Query: CASE WHEN") {
    checkColumns(
      s"SELECT CASE WHEN key > 0 THEN 'big' ELSE 'small' END FROM $reusedTable",
      Seq("key"))
  }

  // ALTER TABLE ADD COLUMNS It should be run at end
  // to prevent affecting the cases that rely on the $reusedTable's table structure
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
    assert(po.privilegeObjectType === PrivilegeObjectType.TABLE_OR_VIEW)
    assert(po.dbname equalsIgnoreCase reusedDb)
    assert(po.objectName === getClass.getSimpleName)
    assert(po.columns.head === "a")
    val accessType = ranger.AccessType(po, operationType, isInput = false)
    assert(accessType === AccessType.ALTER)
  }

  test("AlterTableChangeColumnsCommand") {
    val plan = sql(s"ALTER TABLE $reusedTable" +
      s" ALTER COLUMN value COMMENT 'alter column'").queryExecution.analyzed
    val operationType = OperationType(plan.nodeName)
    assert(operationType === ALTERTABLE_REPLACECOLS)
    val tuple = PrivilegesBuilder.build(plan)
    assert(tuple._1.isEmpty)
    assert(tuple._2.size === 1)
    val po = tuple._2.head
    assert(po.actionType === PrivilegeObjectActionType.OTHER)
    assert(po.privilegeObjectType === PrivilegeObjectType.TABLE_OR_VIEW)
    assert(po.dbname equalsIgnoreCase reusedDb)
    assert(po.objectName === getClass.getSimpleName)
    assert(po.columns.head === "value")
    val accessType = ranger.AccessType(po, operationType, isInput = false)
    assert(accessType === AccessType.ALTER)
  }
}

class InMemoryPrivilegeBuilderSuite extends PrivilegesBuilderSuite {
  override protected val catalogImpl: String = "in-memory"

  // some hive version does not support set database location
  test("AlterDatabaseSetLocationCommand") {
    assume(!isSparkV2)
    val newLoc = spark.conf.get("spark.sql.warehouse.dir") + "/new_db_location"
    val plan = sql(s"ALTER DATABASE default SET LOCATION '$newLoc'")
      .queryExecution.analyzed
    val operationType = OperationType(plan.nodeName)
    assert(operationType === ALTERDATABASE_LOCATION)
    val tuple = PrivilegesBuilder.build(plan)
    assert(tuple._1.isEmpty)
    assert(tuple._2.size === 1)
    val po = tuple._2.head
    assert(po.actionType === PrivilegeObjectActionType.OTHER)
    assert(po.privilegeObjectType === PrivilegeObjectType.DATABASE)
    assert(po.dbname === "default")
    assert(po.objectName === "default")
    assert(po.columns.isEmpty)
    val accessType = ranger.AccessType(po, operationType, isInput = false)
    assert(accessType === AccessType.ALTER)
  }

  test("CreateDataSourceTableAsSelectCommand") {
    val plan = sql(s"CREATE TABLE CreateDataSourceTableAsSelectCommand USING parquet" +
      s" AS SELECT key, value FROM $reusedTable")
      .queryExecution.analyzed
    val operationType = OperationType(plan.nodeName)

    assert(operationType === CREATETABLE_AS_SELECT)
    val tuple = PrivilegesBuilder.build(plan)
    assert(tuple._1.size === 1)
    val po0 = tuple._1.head
    assert(po0.actionType === PrivilegeObjectActionType.OTHER)
    assert(po0.privilegeObjectType === PrivilegeObjectType.TABLE_OR_VIEW)
    assert(po0.dbname equalsIgnoreCase reusedDb)
    assert(po0.objectName equalsIgnoreCase reusedTable.split("\\.").last)
    assert(po0.columns === Seq("key", "value"))
    val accessType0 = ranger.AccessType(po0, operationType, isInput = true)
    assert(accessType0 === AccessType.SELECT)

    assert(tuple._2.size === 1)
    val po = tuple._2.head
    assert(po.actionType === PrivilegeObjectActionType.OTHER)
    assert(po.privilegeObjectType === PrivilegeObjectType.TABLE_OR_VIEW)
    assert(po.dbname === (if (isSparkV2) null else "default"))
    assert(po.objectName === "CreateDataSourceTableAsSelectCommand")
    if (catalogImpl == "hive") {
      assert(po.columns === Seq("key", "value"))
    } else {
      assert(po.columns.isEmpty)
    }
    val accessType = ranger.AccessType(po, operationType, isInput = false)
    assert(accessType === AccessType.CREATE)
  }
}

class HiveCatalogPrivilegeBuilderSuite extends PrivilegesBuilderSuite {

  override protected val catalogImpl: String = if (isSparkV2) "in-memory" else "hive"

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
      assert(po.privilegeObjectType === PrivilegeObjectType.TABLE_OR_VIEW)
      assert(po.dbname === "default")
      assert(po.objectName === t)
      assert(po.columns.head === "pid")
      val accessType = ranger.AccessType(po, operationType, isInput = false)
      assert(accessType === AccessType.ALTER)
    }
  }

  test("CreateTableCommand") {
    assume(!isSparkV2)
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
      assert(po.privilegeObjectType === PrivilegeObjectType.TABLE_OR_VIEW)
      assert(po.dbname === "default")
      assert(po.objectName === "CreateTableCommand")
      assert(po.columns.isEmpty)
      val accessType = ranger.AccessType(po, operationType, isInput = false)
      assert(accessType === AccessType.CREATE)
    }
  }

  test("CreateHiveTableAsSelectCommand") {
    assume(!isSparkV2)
    val plan = sql(s"CREATE TABLE CreateHiveTableAsSelectCommand USING hive" +
      s" AS SELECT key, value FROM $reusedTable")
      .queryExecution.analyzed
    val operationType = OperationType(plan.nodeName)

    assert(operationType === CREATETABLE_AS_SELECT)
    val tuple = PrivilegesBuilder.build(plan)
    assert(tuple._1.size === 1)
    val po0 = tuple._1.head
    assert(po0.actionType === PrivilegeObjectActionType.OTHER)
    assert(po0.privilegeObjectType === PrivilegeObjectType.TABLE_OR_VIEW)
    assert(po0.dbname equalsIgnoreCase reusedDb)
    assert(po0.objectName equalsIgnoreCase reusedTable.split("\\.").last)
    assert(po0.columns === Seq("key", "value"))
    val accessType0 = ranger.AccessType(po0, operationType, isInput = true)
    assert(accessType0 === AccessType.SELECT)

    assert(tuple._2.size === 1)
    val po = tuple._2.head
    assert(po.actionType === PrivilegeObjectActionType.OTHER)
    assert(po.privilegeObjectType === PrivilegeObjectType.TABLE_OR_VIEW)
    assert(po.dbname === "default")
    assert(po.objectName === "CreateHiveTableAsSelectCommand")
    assert(po.columns === Seq("key", "value"))
    val accessType = ranger.AccessType(po, operationType, isInput = false)
    assert(accessType === AccessType.CREATE)
  }

  test("LoadDataCommand") {
    assume(!isSparkV2)
    val dataPath = getClass.getResource("/data.txt").getPath
    val tableName = reusedDb + "." + "LoadDataToTable"
    withTable(tableName) { _ =>
      sql(
        s"""
           |CREATE TABLE $tableName
           |(key int, value string, pid string)
           |USING hive
           |""".stripMargin)
      val plan = sql(s"LOAD DATA INPATH '$dataPath' OVERWRITE INTO TABLE $tableName")
        .queryExecution.analyzed
      val operationType = OperationType(plan.nodeName)
      assert(operationType === LOAD)
      val tuple = PrivilegesBuilder.build(plan)
      assert(tuple._1.isEmpty)

      assert(tuple._2.size === 1)
      val po0 = tuple._2.head
      assert(po0.actionType === PrivilegeObjectActionType.INSERT_OVERWRITE)
      assert(po0.privilegeObjectType === PrivilegeObjectType.TABLE_OR_VIEW)
      assert(po0.dbname equalsIgnoreCase reusedDb)
      assert(po0.objectName equalsIgnoreCase tableName.split("\\.").last)
      assert(po0.columns.isEmpty)
      val accessType0 = ranger.AccessType(po0, operationType, isInput = false)
      assert(accessType0 === AccessType.UPDATE)
    }
  }

  test("InsertIntoDatasourceDirCommand") {
    assume(!isSparkV2)
    val tableDirectory = getClass.getResource("/").getPath + "table_directory"
    val directory = File(tableDirectory).createDirectory()
    val plan = sql(
      s"""
         |INSERT OVERWRITE DIRECTORY '$directory.path'
         |USING parquet
         |SELECT * FROM $reusedPartTable""".stripMargin)
      .queryExecution.analyzed
    val operationType = OperationType(plan.nodeName)
    assert(operationType === QUERY)
    val tuple = PrivilegesBuilder.build(plan)
    assert(tuple._1.size === 1)
    val po0 = tuple._1.head
    assert(po0.actionType === PrivilegeObjectActionType.OTHER)
    assert(po0.privilegeObjectType === PrivilegeObjectType.TABLE_OR_VIEW)
    assert(po0.dbname equalsIgnoreCase reusedDb)
    assert(po0.objectName equalsIgnoreCase reusedPartTable.split("\\.").last)
    assert(po0.columns === Seq("key", "value", "pid"))
    val accessType0 = ranger.AccessType(po0, operationType, isInput = true)
    assert(accessType0 === AccessType.SELECT)

    assert(tuple._2.isEmpty)
  }

  test("InsertIntoDataSourceCommand") {
    assume(!isSparkV2)
    val tableName = "InsertIntoDataSourceTable"
    withTable(tableName) { _ =>
      // sql(s"CREATE TABLE $tableName (a int, b string) USING parquet")
      val schema = new StructType()
        .add("key", IntegerType, nullable = true)
        .add("value", StringType, nullable = true)
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
      val sqlStr =
        s"""
           |INSERT INTO TABLE $tableName
           |SELECT key, value FROM $reusedTable
           |""".stripMargin
      val plan = sql(sqlStr).queryExecution.analyzed
      val operationType = OperationType(plan.nodeName)
      assert(operationType === QUERY)
      val (inputs, outputs) = PrivilegesBuilder.build(plan)

      assert(inputs.size == 1)
      inputs.foreach { po =>
        assert(po.actionType === PrivilegeObjectActionType.OTHER)
        assert(po.privilegeObjectType === PrivilegeObjectType.TABLE_OR_VIEW)
        assert(po.dbname equalsIgnoreCase reusedDb)
        assert(po.objectName equalsIgnoreCase reusedTable.split("\\.").last)
        assert(po.columns === Seq("key", "value"))
        val accessType = ranger.AccessType(po, operationType, isInput = true)
        assert(accessType === AccessType.SELECT)
      }

      assert(outputs.size === 1)
      outputs.foreach { po =>
        assert(po.actionType === PrivilegeObjectActionType.INSERT)
        assert(po.privilegeObjectType === PrivilegeObjectType.TABLE_OR_VIEW)
        assert(po.dbname equalsIgnoreCase "default")
        assert(po.objectName equalsIgnoreCase tableName)
        assert(po.columns.isEmpty)
        val accessType = ranger.AccessType(po, operationType, isInput = false)
        assert(accessType === AccessType.UPDATE)

      }
    }
  }

  test("InsertIntoHadoopFsRelationCommand") {
    assume(!isSparkV2)
    val tableName = "InsertIntoHadoopFsRelationTable"
    withTable(tableName) { _ =>
      sql(s"CREATE TABLE $tableName (a int, b string) USING parquet")
      val sqlStr =
        s"""
           |INSERT INTO TABLE $tableName
           |SELECT key, value FROM $reusedTable
           |""".stripMargin
      val plan = sql(sqlStr).queryExecution.analyzed
      val operationType = OperationType(plan.nodeName)
      assert(operationType === QUERY)
      val (inputs, outputs) = PrivilegesBuilder.build(plan)

      assert(inputs.size == 1)
      inputs.foreach { po =>
        assert(po.actionType === PrivilegeObjectActionType.OTHER)
        assert(po.privilegeObjectType === PrivilegeObjectType.TABLE_OR_VIEW)
        assert(po.dbname equalsIgnoreCase reusedDb)
        assert(po.objectName equalsIgnoreCase reusedTable.split("\\.").last)
        assert(po.columns === Seq("key", "value"))
        val accessType = ranger.AccessType(po, operationType, isInput = false)
        assert(accessType === AccessType.SELECT)
      }

      assert(outputs.isEmpty)
    }
  }

  test("InsertIntoHiveDirCommand") {
    assume(!isSparkV2)
    val tableDirectory = getClass.getResource("/").getPath + "table_directory"
    val directory = File(tableDirectory).createDirectory()
    val plan = sql(
      s"""
         |INSERT OVERWRITE DIRECTORY '$directory.path'
         |USING parquet
         |SELECT * FROM $reusedPartTable""".stripMargin)
      .queryExecution.analyzed
    val operationType = OperationType(plan.nodeName)
    assert(operationType === QUERY)
    val tuple = PrivilegesBuilder.build(plan)
    assert(tuple._1.size === 1)
    val po0 = tuple._1.head
    assert(po0.actionType === PrivilegeObjectActionType.OTHER)
    assert(po0.privilegeObjectType === PrivilegeObjectType.TABLE_OR_VIEW)
    assert(po0.dbname equalsIgnoreCase reusedDb)
    assert(po0.objectName equalsIgnoreCase reusedPartTable.split("\\.").last)
    assert(po0.columns === Seq("key", "value", "pid"))
    val accessType0 = ranger.AccessType(po0, operationType, isInput = true)
    assert(accessType0 === AccessType.SELECT)

    assert(tuple._2.isEmpty)
  }

  test("InsertIntoHiveTableCommand") {
    assume(!isSparkV2)
    val tableName = "InsertIntoHiveTable"
    withTable(tableName) { _ =>
      sql(s"CREATE TABLE $tableName (a int, b string) USING hive")
      val sqlStr =
        s"""
           |INSERT INTO $tableName VALUES (1, "KYUUBI")
           |""".stripMargin
      val plan = sql(sqlStr).queryExecution.analyzed
      val operationType = OperationType(plan.nodeName)
      assert(operationType === QUERY)
      val (inputs, outputs) = PrivilegesBuilder.build(plan)

      assert(inputs.isEmpty)

      assert(outputs.size === 1)
      outputs.foreach { po =>
        assert(po.actionType === PrivilegeObjectActionType.INSERT)
        assert(po.privilegeObjectType === PrivilegeObjectType.TABLE_OR_VIEW)
        assert(po.dbname equalsIgnoreCase "default")
        assert(po.objectName equalsIgnoreCase tableName)
        assert(po.columns.isEmpty)
        val accessType = ranger.AccessType(po, operationType, isInput = false)
        assert(accessType === AccessType.UPDATE)
      }
    }
  }

  test("ShowCreateTableAsSerdeCommand") {
    assume(!isSparkV2)
    withTable("ShowCreateTableAsSerdeCommand") { t =>
      sql(s"CREATE TABLE $t (key int, pid int) USING hive PARTITIONED BY (pid)")
      val plan = sql(s"SHOW CREATE TABLE $t AS SERDE").queryExecution.analyzed
      val operationType = OperationType(plan.nodeName)
      assert(operationType === SHOW_CREATETABLE)
      val tuple = PrivilegesBuilder.build(plan)
      assert(tuple._1.size === 1)
      val po0 = tuple._1.head
      assert(po0.actionType === PrivilegeObjectActionType.OTHER)
      assert(po0.privilegeObjectType === PrivilegeObjectType.TABLE_OR_VIEW)
      assert(po0.dbname === "default")
      assert(po0.objectName === t)
      assert(po0.columns.isEmpty)
      val accessType0 = ranger.AccessType(po0, operationType, isInput = true)
      assert(accessType0 === AccessType.SELECT)

      assert(tuple._2.size === 0)
    }
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
