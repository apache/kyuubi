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

import org.apache.spark.sql.{DataFrame, SparkSession}

import org.apache.kyuubi.{KyuubiFunSuite, Utils}
import org.apache.kyuubi.plugin.spark.authz.OperationType._

abstract class PrivilegesBuilderSuite extends KyuubiFunSuite {

  protected val catalogImpl: String

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

  test("AlterTableSetPropertiesCommand") {
    val plan = sql(s"ALTER TABLE $reusedTable" +
      s" SET TBLPROPERTIES (key='AlterTableSetPropertiesCommand')")
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

class HiveCatalogPrivilegeBuilderSuite extends PrivilegesBuilderSuite {
  override protected val catalogImpl: String = "hive"

  test("AlterTableSerDePropertiesCommand") {
    assume(!org.apache.spark.SPARK_VERSION.startsWith("2"))
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
}

class InMemoryPrivilegeBuilderSuite extends PrivilegesBuilderSuite {
  override protected val catalogImpl: String = "in-memory"

  // some hive version does not support set database location
  test("AlterDatabaseSetLocationCommand") {
    assume(!org.apache.spark.SPARK_VERSION.startsWith("2"))
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
