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

import scala.util.Try

import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.sql.execution.QueryExecution

import org.apache.kyuubi.plugin.spark.authz.OperationType._
import org.apache.kyuubi.plugin.spark.authz.RangerTestNamespace._
import org.apache.kyuubi.plugin.spark.authz.ranger.AccessType
import org.apache.kyuubi.plugin.spark.authz.serde.{Database, DB_COMMAND_SPECS}
import org.apache.kyuubi.plugin.spark.authz.util.AuthZUtils._
import org.apache.kyuubi.util.AssertionUtils._

abstract class V2CommandsPrivilegesSuite extends PrivilegesBuilderSuite {

  protected val supportsUpdateTable: Boolean
  protected val supportsMergeIntoTable: Boolean
  protected val supportsDelete: Boolean
  protected val supportsPartitionGrammar: Boolean
  protected val supportsPartitionManagement: Boolean

  val catalogV2 = "local"
  val namespace = "catalog_ns"
  val catalogTable = s"$catalogV2.$namespace.catalog_table"
  val catalogTableShort = catalogTable.split("\\.").last
  val catalogPartTable = s"$catalogV2.$namespace.catalog_part_table"
  val catalogPartTableShort = catalogPartTable.split("\\.").last
  val defaultV2TableOwner = UserGroupInformation.getCurrentUser.getShortUserName
  final val sparkSessionCatalogName: String = "spark_catalog"

  protected def withV2Table(table: String)(f: String => Unit): Unit = {
    val tableId = s"$catalogV2.$namespace.$table"
    try {
      f(tableId)
    } finally {
      sql(s"DROP TABLE IF EXISTS $tableId")
    }
  }

  protected def checkV2TableOwner(po: PrivilegeObject): Unit = {
    checkTableOwner(po, defaultV2TableOwner)
  }

  protected def withV2Table(table1: String, table2: String)(f: (String, String) => Unit): Unit = {
    val tableId1 = s"$catalogV2.$namespace.$table1"
    val tableId2 = s"$catalogV2.$namespace.$table2"
    try {
      f(tableId1, tableId2)
    } finally {
      sql(s"DROP TABLE IF EXISTS $tableId1")
      sql(s"DROP TABLE IF EXISTS $tableId2")
    }
  }

  protected def executePlan(sql: String): QueryExecution = {
    val parsed = spark.sessionState.sqlParser.parsePlan(sql)
    spark.sessionState.executePlan(parsed)
  }

  override def beforeAll(): Unit = {
    if (spark.conf.getOption(s"spark.sql.catalog.$catalogV2").isDefined) {
      sql(s"CREATE DATABASE IF NOT EXISTS $catalogV2.$namespace")
      sql(
        s"CREATE TABLE IF NOT EXISTS $catalogTable (key int, value String)")
      if (supportsPartitionGrammar) {
        sql(
          s"CREATE TABLE IF NOT EXISTS $catalogPartTable (key int, value String, dt String)" +
            s" PARTITIONED BY (dt)")
      }
    }

    super.beforeAll()
  }

  test("CreateTable") {
    val table = "CreateTable"
    withV2Table(table) { tableId =>
      val plan = executePlan(
        s"CREATE TABLE IF NOT EXISTS $tableId (i int)").analyzed
      val (inputs, outputs, operationType) = PrivilegesBuilder.build(plan, spark)
      assert(operationType === CREATETABLE)
      assert(inputs.isEmpty)
      assert(outputs.size === 1)
      val po = outputs.head
      assert(po.actionType === PrivilegeObjectActionType.OTHER)
      assert(po.privilegeObjectType === PrivilegeObjectType.TABLE_OR_VIEW)
      assertEqualsIgnoreCase(Some(catalogV2))(po.catalog)
      assertEqualsIgnoreCase(namespace)(po.dbname)
      assertEqualsIgnoreCase(table)(po.objectName)
      assert(po.columns.isEmpty)
      assert(po.owner.isEmpty)
      val accessType = AccessType(po, operationType, isInput = false)
      assert(accessType === AccessType.CREATE)
    }
  }

  test("CreateTableAsSelect") {
    val table = "CreateTableAsSelect"
    withV2Table(table) { tableId =>
      val plan = executePlan(
        s"CREATE TABLE IF NOT EXISTS $tableId AS " +
          s"SELECT * FROM $reusedTable").analyzed
      val (inputs, outputs, operationType) = PrivilegesBuilder.build(plan, spark)
      assert(operationType === CREATETABLE_AS_SELECT)
      assert(inputs.size === 1)
      val po0 = inputs.head
      assert(po0.actionType === PrivilegeObjectActionType.OTHER)
      assert(po0.privilegeObjectType === PrivilegeObjectType.TABLE_OR_VIEW)
      assert(po0.catalog.isEmpty)
      assertEqualsIgnoreCase(reusedDb)(po0.dbname)
      assertEqualsIgnoreCase(reusedTableShort)(po0.objectName)
      assert(po0.columns === Seq("a", "key", "value"))
      checkTableOwner(po0)

      assert(outputs.size === 1)
      val po = outputs.head
      assert(po.actionType === PrivilegeObjectActionType.OTHER)
      assert(po.privilegeObjectType === PrivilegeObjectType.TABLE_OR_VIEW)
      assertEqualsIgnoreCase(Some(catalogV2))(po.catalog)
      assertEqualsIgnoreCase(namespace)(po.dbname)
      assertEqualsIgnoreCase(table)(po.objectName)
      assert(po.columns.isEmpty)
      assert(po.owner.isEmpty)
      val accessType = AccessType(po, operationType, isInput = false)
      assert(accessType === AccessType.CREATE)
    }
  }

  test("ReplaceTable") {
    val table = "ReplaceTable"
    withV2Table(table) { tableId =>
      sql(s"CREATE TABLE IF NOT EXISTS $tableId (i int)")
      val plan = executePlan(s"REPLACE TABLE $tableId (j int)").analyzed

      val (inputs, outputs, operationType) = PrivilegesBuilder.build(plan, spark)
      assert(operationType === CREATETABLE)
      assert(inputs.size === 0)
      assert(outputs.size === 1)
      val po = outputs.head
      assert(po.actionType === PrivilegeObjectActionType.OTHER)
      assert(po.privilegeObjectType === PrivilegeObjectType.TABLE_OR_VIEW)
      assertEqualsIgnoreCase(Some(catalogV2))(po.catalog)
      assertEqualsIgnoreCase(namespace)(po.dbname)
      assertEqualsIgnoreCase(table)(po.objectName)
      assert(po.columns.isEmpty)
      if (isSparkV34OrGreater) {
        checkV2TableOwner(po)
      } else {
        assert(po.owner.isEmpty)
      }
      val accessType = AccessType(po, operationType, isInput = false)
      assert(accessType === AccessType.CREATE)
    }
  }

  test("ReplaceTableAsSelect") {
    val table = "ReplaceTableAsSelect"
    withV2Table(table) { tableId =>
      sql(s"CREATE TABLE IF NOT EXISTS $tableId (i int)")
      val plan =
        executePlan(s"REPLACE TABLE $tableId AS SELECT * FROM $reusedTable").analyzed
      val (inputs, outputs, operationType) = PrivilegesBuilder.build(plan, spark)
      assert(operationType === CREATETABLE_AS_SELECT)
      assert(inputs.size === 1)
      val po0 = inputs.head
      assert(po0.actionType === PrivilegeObjectActionType.OTHER)
      assert(po0.privilegeObjectType === PrivilegeObjectType.TABLE_OR_VIEW)
      assert(po0.catalog.isEmpty)
      assertEqualsIgnoreCase(reusedDb)(po0.dbname)
      assertEqualsIgnoreCase(reusedTableShort)(po0.objectName)
      assert(po0.columns === Seq("a", "key", "value"))
      checkTableOwner(po0)

      assert(outputs.size === 1)
      val po = outputs.head
      assert(po.actionType === PrivilegeObjectActionType.OTHER)
      assert(po.privilegeObjectType === PrivilegeObjectType.TABLE_OR_VIEW)
      assertEqualsIgnoreCase(Some(catalogV2))(po.catalog)
      assertEqualsIgnoreCase(namespace)(po.dbname)
      assertEqualsIgnoreCase(table)(po.objectName)
      assert(po.columns.isEmpty)
      if (isSparkV34OrGreater) {
        checkV2TableOwner(po)
      } else {
        assert(po.owner.isEmpty)
      }
      val accessType = AccessType(po, operationType, isInput = false)
      assert(accessType === AccessType.CREATE)
    }
  }

  // with V2WriteCommand

  test("AppendData") {
    val plan = executePlan(s"INSERT INTO $catalogTable VALUES (0, 'a')").analyzed
    val (inputs, outputs, operationType) = PrivilegesBuilder.build(plan, spark)
    assert(operationType === QUERY)
    assert(inputs.size === 0)
    assert(outputs.size === 1)
    val po = outputs.head
    assert(po.actionType === PrivilegeObjectActionType.INSERT)
    assert(po.privilegeObjectType === PrivilegeObjectType.TABLE_OR_VIEW)
    assertEqualsIgnoreCase(Some(catalogV2))(po.catalog)
    assertEqualsIgnoreCase(namespace)(po.dbname)
    assertEqualsIgnoreCase(catalogTableShort)(po.objectName)
    assert(po.columns.isEmpty)
    checkV2TableOwner(po)
    val accessType = AccessType(po, operationType, isInput = false)
    assert(accessType === AccessType.UPDATE)
  }

  test("UpdateTable") {
    assume(supportsUpdateTable)

    val plan = executePlan(s"UPDATE $catalogTable SET value = 'a' WHERE key = 0").analyzed

    val (inputs, outputs, operationType) = PrivilegesBuilder.build(plan, spark)
    assert(operationType === QUERY)
    assert(inputs.size === 0)
    assert(outputs.size === 1)
    val po = outputs.head
    assert(po.actionType === PrivilegeObjectActionType.UPDATE)
    assert(po.privilegeObjectType === PrivilegeObjectType.TABLE_OR_VIEW)
    assertEqualsIgnoreCase(Some(catalogV2))(po.catalog)
    assertEqualsIgnoreCase(namespace)(po.dbname)
    assertEqualsIgnoreCase(catalogTableShort)(po.objectName)
    assert(po.columns.isEmpty)
    checkV2TableOwner(po)
    val accessType = AccessType(po, operationType, isInput = false)
    assert(accessType === AccessType.UPDATE)
  }

  test("DeleteFromTable") {
    assume(supportsDelete)

    val plan = executePlan(s"DELETE FROM $catalogTable WHERE key = 0").analyzed
    val (inputs, outputs, operationType) = PrivilegesBuilder.build(plan, spark)
    assert(operationType === QUERY)
    assert(inputs.size === 0)
    assert(outputs.size === 1)
    val po = outputs.head
    assert(po.actionType === PrivilegeObjectActionType.UPDATE)
    assert(po.privilegeObjectType === PrivilegeObjectType.TABLE_OR_VIEW)
    assertEqualsIgnoreCase(Some(catalogV2))(po.catalog)
    assertEqualsIgnoreCase(namespace)(po.dbname)
    assertEqualsIgnoreCase(catalogTableShort)(po.objectName)
    assert(po.columns.isEmpty)
    checkV2TableOwner(po)
    val accessType = AccessType(po, operationType, isInput = false)
    assert(accessType === AccessType.UPDATE)
  }

  test("OverwriteByExpression") {
    val plan = executePlan(s"INSERT OVERWRITE TABLE $catalogTable VALUES (0, 1)").analyzed
    val (inputs, outputs, operationType) = PrivilegesBuilder.build(plan, spark)
    assert(operationType === QUERY)
    assert(inputs.size === 0)
    assert(outputs.size === 1)
    val po = outputs.head
    assert(po.actionType === PrivilegeObjectActionType.INSERT_OVERWRITE)
    assert(po.privilegeObjectType === PrivilegeObjectType.TABLE_OR_VIEW)
    assertEqualsIgnoreCase(Some(catalogV2))(po.catalog)
    assertEqualsIgnoreCase(namespace)(po.dbname)
    assertEqualsIgnoreCase(catalogTableShort)(po.objectName)
    assert(po.columns.isEmpty)
    checkV2TableOwner(po)
    val accessType = AccessType(po, operationType, isInput = false)
    assert(accessType === AccessType.UPDATE)
  }

  test("OverwritePartitionsDynamic") {
    assume(supportsPartitionGrammar)

    try {
      sql("SET spark.sql.sources.partitionOverwriteMode=dynamic")
      val plan = executePlan(s"INSERT OVERWRITE TABLE $catalogPartTable PARTITION (dt)" +
        s"VALUES (0, 1, '2022-01-01')").analyzed
      val (inputs, outputs, operationType) = PrivilegesBuilder.build(plan, spark)
      assert(operationType === QUERY)
      assert(inputs.size === 0)
      assert(outputs.size === 1)
      val po = outputs.head
      assert(po.actionType === PrivilegeObjectActionType.INSERT_OVERWRITE)
      assert(po.privilegeObjectType === PrivilegeObjectType.TABLE_OR_VIEW)
      assertEqualsIgnoreCase(Some(catalogV2))(po.catalog)
      assertEqualsIgnoreCase(namespace)(po.dbname)
      assertEqualsIgnoreCase(catalogPartTableShort)(po.objectName)
      assert(po.columns.isEmpty)
      checkV2TableOwner(po)
      val accessType = AccessType(po, operationType, isInput = false)
      assert(accessType === AccessType.UPDATE)
    } finally {
      sql("SET spark.sql.sources.partitionOverwriteMode=static")
    }
  }

  test("AddPartitions") {
    assume(supportsPartitionManagement)

    val plan = executePlan(s"ALTER TABLE $catalogPartTable " +
      s"ADD PARTITION (dt='2022-01-01')").analyzed
    val (inputs, outputs, operationType) = PrivilegesBuilder.build(plan, spark)
    assert(operationType === ALTERTABLE_ADDPARTS)
    assert(inputs.size === 0)
    assert(outputs.size === 1)
    val po = outputs.head
    assert(po.actionType === PrivilegeObjectActionType.OTHER)
    assert(po.privilegeObjectType === PrivilegeObjectType.TABLE_OR_VIEW)
    assertEqualsIgnoreCase(Some(catalogV2))(po.catalog)
    assertEqualsIgnoreCase(namespace)(po.dbname)
    assertEqualsIgnoreCase(catalogPartTableShort)(po.objectName)
    assert(po.columns.isEmpty)
    checkV2TableOwner(po)
    val accessType = AccessType(po, operationType, isInput = false)
    assert(accessType === AccessType.ALTER)
  }

  test("DropPartitions") {
    assume(supportsPartitionManagement)

    val plan = executePlan(s"ALTER TABLE $catalogPartTable " +
      s"DROP PARTITION (dt='2022-01-01')").analyzed
    val (inputs, outputs, operationType) = PrivilegesBuilder.build(plan, spark)
    assert(operationType === ALTERTABLE_DROPPARTS)
    assert(inputs.size === 0)
    assert(outputs.size === 1)
    val po = outputs.head
    assert(po.actionType === PrivilegeObjectActionType.OTHER)
    assert(po.privilegeObjectType === PrivilegeObjectType.TABLE_OR_VIEW)
    assertEqualsIgnoreCase(Some(catalogV2))(po.catalog)
    assertEqualsIgnoreCase(namespace)(po.dbname)
    assertEqualsIgnoreCase(catalogPartTableShort)(po.objectName)
    assert(po.columns.isEmpty)
    checkV2TableOwner(po)
    val accessType = AccessType(po, operationType, isInput = false)
    assert(accessType === AccessType.ALTER)
  }

  test("RenamePartitions") {
    assume(supportsPartitionManagement)

    val plan = executePlan(s"ALTER TABLE $catalogPartTable " +
      s"PARTITION (dt='2022-01-01') RENAME TO PARTITION (dt='2022-01-02')").analyzed
    val (inputs, outputs, operationType) = PrivilegesBuilder.build(plan, spark)
    assert(operationType === ALTERTABLE_ADDPARTS)
    assert(inputs.size === 0)
    assert(outputs.size === 1)
    val po = outputs.head
    assert(po.actionType === PrivilegeObjectActionType.OTHER)
    assert(po.privilegeObjectType === PrivilegeObjectType.TABLE_OR_VIEW)
    assertEqualsIgnoreCase(Some(catalogV2))(po.catalog)
    assertEqualsIgnoreCase(namespace)(po.dbname)
    assertEqualsIgnoreCase(catalogPartTableShort)(po.objectName)
    assert(po.columns.isEmpty)
    checkV2TableOwner(po)
    val accessType = AccessType(po, operationType, isInput = false)
    assert(accessType === AccessType.ALTER)
  }

  test("TruncatePartition") {
    assume(supportsPartitionManagement)

    val plan = executePlan(s"ALTER TABLE $catalogPartTable " +
      s"PARTITION (dt='2022-01-01') RENAME TO PARTITION (dt='2022-01-02')").analyzed

    val (inputs, outputs, operationType) = PrivilegesBuilder.build(plan, spark)
    assert(operationType === ALTERTABLE_DROPPARTS)
    assert(inputs.size === 0)
    assert(outputs.size === 1)
    val po = outputs.head
    assert(po.actionType === PrivilegeObjectActionType.OTHER)
    assert(po.privilegeObjectType === PrivilegeObjectType.TABLE_OR_VIEW)
    assertEqualsIgnoreCase(Some(catalogV2))(po.catalog)
    assertEqualsIgnoreCase(namespace)(po.dbname)
    assertEqualsIgnoreCase(catalogPartTableShort)(po.objectName)
    assert(po.columns.isEmpty)
    checkV2TableOwner(po)
    val accessType = AccessType(po, operationType, isInput = false)
    assert(accessType === AccessType.ALTER)
  }

  // other table commands

  test("CommentOnTable") {
    val plan = executePlan(s"COMMENT ON TABLE $catalogTable IS 'text'").analyzed

    val (inputs, outputs, operationType) = PrivilegesBuilder.build(plan, spark)
    assert(operationType === ALTERTABLE_PROPERTIES)
    assert(inputs.isEmpty)
    assert(outputs.size === 1)
    val po = outputs.head
    assert(po.actionType === PrivilegeObjectActionType.OTHER)
    assert(po.privilegeObjectType === PrivilegeObjectType.TABLE_OR_VIEW)
    assertEqualsIgnoreCase(Some(catalogV2))(po.catalog)
    assertEqualsIgnoreCase(namespace)(po.dbname)
    assertEqualsIgnoreCase(catalogTableShort)(po.objectName)
    assert(po.columns.isEmpty)
    checkV2TableOwner(po)
    val accessType = AccessType(po, operationType, isInput = false)
    assert(accessType === AccessType.ALTER)
  }

  test("DropTable") {
    val table = "DropTable"
    withV2Table(table) { tableId =>
      sql(s"CREATE TABLE $tableId (i int)")
      val plan = executePlan(s"DROP TABLE $tableId").analyzed

      val (inputs, outputs, operationType) = PrivilegesBuilder.build(plan, spark)
      assert(operationType === DROPTABLE)
      assert(inputs.isEmpty)
      assert(outputs.size === 1)
      val po = outputs.head
      assert(po.actionType === PrivilegeObjectActionType.OTHER)
      assert(po.privilegeObjectType === PrivilegeObjectType.TABLE_OR_VIEW)
      assertEqualsIgnoreCase(Some(catalogV2))(po.catalog)
      assertEqualsIgnoreCase(namespace)(po.dbname)
      assertEqualsIgnoreCase(table)(po.objectName)
      assert(po.columns.isEmpty)
      checkV2TableOwner(po)
      val accessType = AccessType(po, operationType, isInput = false)
      assert(accessType === AccessType.DROP)
    }
  }

  test("MergeIntoTable") {
    assume(supportsMergeIntoTable)

    val table = "MergeIntoTable"
    withV2Table(table) { tableId =>
      sql(s"CREATE TABLE $tableId (key int, value String)")
      val plan = executePlan(s"MERGE INTO $tableId t " +
        s"USING (SELECT * FROM $catalogTable) s " +
        s"ON t.key = s.key " +
        s"WHEN MATCHED THEN UPDATE SET t.value = s.value " +
        s"WHEN NOT MATCHED THEN INSERT *").analyzed
      val (inputs, outputs, operationType) = PrivilegesBuilder.build(plan, spark)
      assert(operationType === QUERY)
      assert(inputs.size == 1)
      val po0 = inputs.head
      assert(po0.actionType === PrivilegeObjectActionType.OTHER)
      assert(po0.privilegeObjectType === PrivilegeObjectType.TABLE_OR_VIEW)
      assertEqualsIgnoreCase(Some(catalogV2))(po0.catalog)
      assertEqualsIgnoreCase(namespace)(po0.dbname)
      assertEqualsIgnoreCase(catalogTableShort)(po0.objectName)
      assert(po0.columns === Seq("key", "value"))
      checkV2TableOwner(po0)

      assert(outputs.size === 1)
      val po = outputs.head
      assert(po.actionType === PrivilegeObjectActionType.UPDATE)
      assert(po.privilegeObjectType === PrivilegeObjectType.TABLE_OR_VIEW)
      assertEqualsIgnoreCase(Some(catalogV2))(po.catalog)
      assertEqualsIgnoreCase(namespace)(po.dbname)
      assertEqualsIgnoreCase(table)(po.objectName)
      assert(po.columns.isEmpty)
      checkV2TableOwner(po)
      val accessType = AccessType(po, operationType, isInput = false)
      assert(accessType === AccessType.UPDATE)
    }
  }

  test("RepairTable") {
    assume(supportsPartitionGrammar)

    val plan = executePlan(s"MSCK REPAIR TABLE $catalogPartTable").analyzed

    val (inputs, outputs, operationType) = PrivilegesBuilder.build(plan, spark)
    assert(operationType === MSCK)
    assert(inputs.isEmpty)
    assert(outputs.size === 1)
    val po = outputs.head
    assert(po.actionType === PrivilegeObjectActionType.OTHER)
    assert(po.privilegeObjectType === PrivilegeObjectType.TABLE_OR_VIEW)
    assertEqualsIgnoreCase(Some(catalogV2))(po.catalog)
    assertEqualsIgnoreCase(namespace)(po.dbname)
    assertEqualsIgnoreCase(catalogPartTableShort)(po.objectName)
    assert(po.columns.isEmpty)
    checkV2TableOwner(po)
    val accessType = AccessType(po, operationType, isInput = false)
    assert(accessType === AccessType.ALTER)
  }

  test("TruncateTable") {

    val plan = executePlan(s"TRUNCATE TABLE $catalogTable").analyzed

    val (inputs, outputs, operationType) = PrivilegesBuilder.build(plan, spark)
    assert(operationType === TRUNCATETABLE)
    assert(inputs.isEmpty)
    assert(outputs.size === 1)
    val po = outputs.head
    assert(po.actionType === PrivilegeObjectActionType.OTHER)
    assert(po.privilegeObjectType === PrivilegeObjectType.TABLE_OR_VIEW)
    assertEqualsIgnoreCase(Some(catalogV2))(po.catalog)
    assertEqualsIgnoreCase(namespace)(po.dbname)
    assertEqualsIgnoreCase(catalogTableShort)(po.objectName)
    assert(po.columns.isEmpty)
    checkV2TableOwner(po)
    val accessType = AccessType(po, operationType, isInput = false)
    assert(accessType === AccessType.UPDATE)
  }

  test("DescribeTable") {
    val plan = executePlan(s"DESCRIBE TABLE $catalogTable").analyzed
    val (inputs, outputs, operationType) = PrivilegesBuilder.build(plan, spark)
    assert(operationType === DESCTABLE)
    assert(inputs.size === 1)
    val po = inputs.head
    assert(po.actionType === PrivilegeObjectActionType.OTHER)
    assert(po.privilegeObjectType === PrivilegeObjectType.TABLE_OR_VIEW)
    assertEqualsIgnoreCase(Some(catalogV2))(po.catalog)
    assertEqualsIgnoreCase(namespace)(po.dbname)
    assertEqualsIgnoreCase(catalogTableShort)(po.objectName)
    assert(po.columns.isEmpty)
    checkV2TableOwner(po)
    val accessType = AccessType(po, operationType, isInput = true)
    assert(accessType === AccessType.SELECT)
    assert(outputs.size === 0)
  }

  // with V2AlterTableCommand

  test("AddColumns") {

    val table = "AddColumns"
    withV2Table(table) { tableId =>
      sql(s"CREATE TABLE $tableId (i int)")
      val plan = executePlan(s"ALTER TABLE $tableId ADD COLUMNS (j int)").analyzed

      val (inputs, outputs, operationType) = PrivilegesBuilder.build(plan, spark)
      assert(operationType === ALTERTABLE_ADDCOLS)
      assert(inputs.isEmpty)
      assert(outputs.size === 1)
      val po = outputs.head
      assert(po.actionType === PrivilegeObjectActionType.OTHER)
      assert(po.privilegeObjectType === PrivilegeObjectType.TABLE_OR_VIEW)
      assertEqualsIgnoreCase(Some(catalogV2))(po.catalog)
      assertEqualsIgnoreCase(namespace)(po.dbname)
      assertEqualsIgnoreCase(table)(po.objectName)
      assert(po.columns.isEmpty)
      checkV2TableOwner(po)
      val accessType = AccessType(po, operationType, isInput = false)
      assert(accessType === AccessType.ALTER)
    }
  }

  test("AlterColumn") {
    val table = "AlterColumn"
    withV2Table(table) { tableId =>
      sql(s"CREATE TABLE $tableId (i int)")
      val plan = executePlan(s"ALTER TABLE $tableId ALTER COLUMN i TYPE int").analyzed

      val (inputs, outputs, operationType) = PrivilegesBuilder.build(plan, spark)
      assert(operationType === ALTERTABLE_ADDCOLS)
      assert(inputs.isEmpty)
      assert(outputs.size === 1)
      val po = outputs.head
      assert(po.actionType === PrivilegeObjectActionType.OTHER)
      assert(po.privilegeObjectType === PrivilegeObjectType.TABLE_OR_VIEW)
      assertEqualsIgnoreCase(Some(catalogV2))(po.catalog)
      assertEqualsIgnoreCase(namespace)(po.dbname)
      assertEqualsIgnoreCase(table)(po.objectName)
      assert(po.columns.isEmpty)
      checkV2TableOwner(po)
      val accessType = AccessType(po, operationType, isInput = false)
      assert(accessType === AccessType.ALTER)
    }
  }

  test("DropColumns") {
    val table = "DropColumns"
    withV2Table(table) { tableId =>
      sql(s"CREATE TABLE $tableId (i int, j int)")
      val plan = executePlan(s"ALTER TABLE $tableId DROP COLUMN i").analyzed

      val (inputs, outputs, operationType) = PrivilegesBuilder.build(plan, spark)
      assert(operationType === ALTERTABLE_ADDCOLS)
      assert(inputs.isEmpty)
      assert(outputs.size === 1)
      val po = outputs.head
      assert(po.actionType === PrivilegeObjectActionType.OTHER)
      assert(po.privilegeObjectType === PrivilegeObjectType.TABLE_OR_VIEW)
      assertEqualsIgnoreCase(Some(catalogV2))(po.catalog)
      assertEqualsIgnoreCase(namespace)(po.dbname)
      assertEqualsIgnoreCase(table)(po.objectName)
      assert(po.columns.isEmpty)
      checkV2TableOwner(po)
      val accessType = AccessType(po, operationType, isInput = false)
      assert(accessType === AccessType.ALTER)
    }
  }

  test("ReplaceColumns") {
    val table = "ReplaceColumns"
    withV2Table(table) { tableId =>
      sql(s"CREATE TABLE $tableId (i int, j int)")
      val plan = executePlan(s"ALTER TABLE $tableId REPLACE COLUMNS (i String)").analyzed

      val (inputs, outputs, operationType) = PrivilegesBuilder.build(plan, spark)
      assert(operationType === ALTERTABLE_REPLACECOLS)
      assert(inputs.isEmpty)
      assert(outputs.size === 1)
      val po = outputs.head
      assert(po.actionType === PrivilegeObjectActionType.OTHER)
      assert(po.privilegeObjectType === PrivilegeObjectType.TABLE_OR_VIEW)
      assertEqualsIgnoreCase(Some(catalogV2))(po.catalog)
      assertEqualsIgnoreCase(namespace)(po.dbname)
      assertEqualsIgnoreCase(table)(po.objectName)
      assert(po.columns.isEmpty)
      checkV2TableOwner(po)
      val accessType = AccessType(po, operationType, isInput = false)
      assert(accessType === AccessType.ALTER)
    }
  }

  test("RenameColumn") {
    val table = "RenameColumn"
    withV2Table(table) { tableId =>
      sql(s"CREATE TABLE $tableId (i int, j int)")
      val plan = executePlan(s"ALTER TABLE $tableId RENAME COLUMN i TO k").analyzed

      val (inputs, outputs, operationType) = PrivilegesBuilder.build(plan, spark)
      assert(operationType === ALTERTABLE_RENAMECOL)
      assert(inputs.isEmpty)
      assert(outputs.size === 1)
      val po = outputs.head
      assert(po.actionType === PrivilegeObjectActionType.OTHER)
      assert(po.privilegeObjectType === PrivilegeObjectType.TABLE_OR_VIEW)
      assertEqualsIgnoreCase(Some(catalogV2))(po.catalog)
      assertEqualsIgnoreCase(namespace)(po.dbname)
      assertEqualsIgnoreCase(table)(po.objectName)
      assert(po.columns.isEmpty)
      checkV2TableOwner(po)
      val accessType = AccessType(po, operationType, isInput = false)
      assert(accessType === AccessType.ALTER)
    }
  }

  test("SetNamespaceCommand") {
    val ns1 = "testns1"
    val sql1 = s"USE NAMESPACE $ns1"
    val plan1 = executePlan(sql1).analyzed
    val spec = DB_COMMAND_SPECS(plan1.getClass.getName)
    var db: Database = null
    spec.databaseDescs.find { d =>
      Try { db = d.extract(plan1) }.isSuccess
    }
    withClue(sql1) {
      assert(db.catalog === None)
      assert(db.database === ns1)
    }
  }

  test("SetNamespaceProperties") {
    val plan = sql("ALTER DATABASE default SET DBPROPERTIES (abc = '123')").queryExecution.analyzed
    val (in, out, operationType) = PrivilegesBuilder.build(plan, spark)
    assertResult(plan.getClass.getName)(
      "org.apache.spark.sql.catalyst.plans.logical.SetNamespaceProperties")
    assert(operationType === ALTERDATABASE)
    assert(in.isEmpty)
    assert(out.size === 1)
    val po = out.head
    assert(po.actionType === PrivilegeObjectActionType.OTHER)
    assert(po.privilegeObjectType === PrivilegeObjectType.DATABASE)
    assert(po.catalog.get === sparkSessionCatalogName)
    assertEqualsIgnoreCase(defaultDb)(po.dbname)
    assertEqualsIgnoreCase(defaultDb)(po.objectName)
    assert(po.columns.isEmpty)
  }

  test("CreateNamespace") {
    withDatabase("CreateNamespace") { db =>
      val plan = sql(s"CREATE DATABASE $db").queryExecution.analyzed
      val (in, out, operationType) = PrivilegesBuilder.build(plan, spark)
      assertResult(plan.getClass.getName)(
        "org.apache.spark.sql.catalyst.plans.logical.CreateNamespace")
      assert(operationType === CREATEDATABASE)
      assert(in.isEmpty)
      assert(out.size === 1)
      val po = out.head
      assert(po.actionType === PrivilegeObjectActionType.OTHER)
      assert(po.privilegeObjectType === PrivilegeObjectType.DATABASE)
      assert(po.catalog.get === sparkSessionCatalogName)
      assertEqualsIgnoreCase("CreateNamespace")(po.dbname)
      assertEqualsIgnoreCase("CreateNamespace")(po.objectName)
      assert(po.columns.isEmpty)
      val accessType = ranger.AccessType(po, operationType, isInput = false)
      assert(accessType === AccessType.CREATE)
    }
  }

  test("SetNamespaceLocation") {
    // hive does not support altering database location
    assume(catalogImpl !== "hive")
    val newLoc = spark.conf.get("spark.sql.warehouse.dir") + "/new_db_location"
    val plan = sql(s"ALTER DATABASE default SET LOCATION '$newLoc'")
      .queryExecution.analyzed
    val (in, out, operationType) = PrivilegesBuilder.build(plan, spark)
    assertResult(plan.getClass.getName)(
      "org.apache.spark.sql.catalyst.plans.logical.SetNamespaceLocation")
    assert(operationType === ALTERDATABASE_LOCATION)
    assert(in.isEmpty)
    assert(out.size === 2)
    val po0 = out.head
    assert(po0.actionType === PrivilegeObjectActionType.OTHER)
    assert(po0.privilegeObjectType === PrivilegeObjectType.DATABASE)
    assert(po0.catalog.get === sparkSessionCatalogName)
    assertEqualsIgnoreCase(defaultDb)(po0.dbname)
    assertEqualsIgnoreCase(defaultDb)(po0.objectName)
    assert(po0.columns.isEmpty)
    val accessType0 = ranger.AccessType(po0, operationType, isInput = false)
    assert(accessType0 === AccessType.ALTER)

    val po1 = out.last
    assert(po1.actionType === PrivilegeObjectActionType.OTHER)
    assert(po1.catalog.isEmpty)
    assertEqualsIgnoreCase(defaultDb)(po0.dbname)
    assertEqualsIgnoreCase(defaultDb)(po0.objectName)
    assert(po1.columns.isEmpty)
    val accessType1 = ranger.AccessType(po1, operationType, isInput = false)
    assert(accessType1 === AccessType.WRITE)
  }

  test("DescribeNamespace") {
    val plan = sql(s"DESC DATABASE $reusedDb").queryExecution.analyzed
    val (in, out, operationType) = PrivilegesBuilder.build(plan, spark)
    assertResult(plan.getClass.getName)(
      "org.apache.spark.sql.catalyst.plans.logical.DescribeNamespace")
    assert(operationType === DESCDATABASE)
    assert(in.size === 1)
    val po = in.head
    assert(po.actionType === PrivilegeObjectActionType.OTHER)
    assert(po.privilegeObjectType === PrivilegeObjectType.DATABASE)
    assert(po.catalog.get === sparkSessionCatalogName)
    assertEqualsIgnoreCase(reusedDb)(po.dbname)
    assertEqualsIgnoreCase(reusedDb)(po.objectName)
    assert(po.columns.isEmpty)
    val accessType = ranger.AccessType(po, operationType, isInput = false)
    assert(accessType === AccessType.USE)

    assert(out.size === 0)
  }

  test("DropNamespace") {
    withDatabase("DropNameSpace") { db =>
      sql(s"CREATE DATABASE $db")
      val plan = sql(s"DROP DATABASE DropNameSpace").queryExecution.analyzed
      val (in, out, operationType) = PrivilegesBuilder.build(plan, spark)
      assertResult(plan.getClass.getName)(
        "org.apache.spark.sql.catalyst.plans.logical.DropNamespace")
      assert(operationType === DROPDATABASE)
      assert(in.isEmpty)
      assert(out.size === 1)
      val po = out.head
      assert(po.actionType === PrivilegeObjectActionType.OTHER)
      assert(po.privilegeObjectType === PrivilegeObjectType.DATABASE)
      assert(po.catalog.get === sparkSessionCatalogName)
      assertEqualsIgnoreCase(db)(po.dbname)
      assertEqualsIgnoreCase(db)(po.objectName)
      assert(po.columns.isEmpty)
      val accessType = ranger.AccessType(po, operationType, isInput = false)
      assert(accessType === AccessType.DROP)
    }
  }
}
