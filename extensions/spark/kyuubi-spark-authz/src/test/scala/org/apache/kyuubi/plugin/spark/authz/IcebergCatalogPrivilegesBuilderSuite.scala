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

import org.scalatest.Outcome

import org.apache.kyuubi.Utils
import org.apache.kyuubi.plugin.spark.authz.OperationType._
import org.apache.kyuubi.plugin.spark.authz.ranger.AccessType

class IcebergCatalogPrivilegesBuilderSuite extends V2CommandsPrivilegesSuite {
  override protected val catalogImpl: String = "hive"
  override protected val sqlExtensions: String =
    if (isSparkV32OrGreater) {
      "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    } else ""
  override protected def format = "iceberg"

  override protected val supportsUpdateTable = false
  override protected val supportsMergeIntoTable = false
  override protected val supportsDelete = false
  override protected val supportsPartitionGrammar = true
  override protected val supportsPartitionManagement = false

  override def beforeAll(): Unit = {
    if (isSparkV32OrGreater) {
      spark.conf.set(
        s"spark.sql.catalog.$catalogV2",
        "org.apache.iceberg.spark.SparkCatalog")
      spark.conf.set(s"spark.sql.catalog.$catalogV2.type", "hadoop")
      spark.conf.set(
        s"spark.sql.catalog.$catalogV2.warehouse",
        Utils.createTempDir("iceberg-hadoop").toString)
    }
    super.beforeAll()
  }

  override def withFixture(test: NoArgTest): Outcome = {
    assume(isSparkV32OrGreater)
    test()
  }

  test("DeleteFromIcebergTable") {
    val plan = sql(s"DELETE FROM $catalogTable WHERE key = 1 ").queryExecution.analyzed
    assert(IcebergCommands.accept(plan.nodeName))
    val (inputs, outputs, operationType) = PrivilegesBuilder.build(plan, spark)
    assert(operationType === QUERY)
    assert(inputs.isEmpty)
    assert(outputs.size === 1)
    val po = outputs.head
    assert(po.actionType === PrivilegeObjectActionType.UPDATE)
    assert(po.privilegeObjectType === PrivilegeObjectType.TABLE_OR_VIEW)
    assert(po.dbname === namespace)
    assert(po.objectName === catalogTableShort)
    assert(po.columns.isEmpty)
    checkV2TableOwner(po)
    val accessType = AccessType(po, operationType, isInput = false)
    assert(accessType === AccessType.UPDATE)
  }

  test("UpdateIcebergTable") {
    val plan = sql(s"UPDATE $catalogTable SET value = 'b' WHERE key = 1 ").queryExecution.analyzed
    assert(IcebergCommands.accept(plan.nodeName))
    val (inputs, outputs, operationType) = PrivilegesBuilder.build(plan, spark)
    assert(operationType === QUERY)
    assert(inputs.isEmpty)
    assert(outputs.size === 1)
    val po = outputs.head
    assert(po.actionType === PrivilegeObjectActionType.UPDATE)
    assert(po.privilegeObjectType === PrivilegeObjectType.TABLE_OR_VIEW)
    assert(po.dbname === namespace)
    assert(po.objectName === catalogTableShort)
    assert(po.columns.isEmpty)
    checkV2TableOwner(po)
    val accessType = AccessType(po, operationType, isInput = false)
    assert(accessType === AccessType.UPDATE)
  }

  test("MergeIntoIcebergTable") {
    val table = "MergeIntoIcebergTable"
    withV2Table(table) { tableId =>
      sql(s"CREATE TABLE $tableId (key int, value String) USING iceberg")
      val plan = sql(s"MERGE INTO $tableId t " +
        s"USING (SELECT * FROM $catalogTable) s " +
        s"ON t.key = s.key " +
        s"WHEN MATCHED THEN UPDATE SET t.value = s.value " +
        s"WHEN NOT MATCHED THEN INSERT *").queryExecution.analyzed
      val (inputs, outputs, operationType) = PrivilegesBuilder.build(plan, spark)
      assert(operationType === QUERY)
      assert(inputs.size === 1)
      val po0 = inputs.head
      assert(po0.actionType === PrivilegeObjectActionType.OTHER)
      assert(po0.privilegeObjectType === PrivilegeObjectType.TABLE_OR_VIEW)
      assert(po0.dbname === namespace)
      assert(po0.objectName === catalogTableShort)
      assert(po0.columns === Seq("key", "value"))
      checkV2TableOwner(po0)

      assert(outputs.size === 1)
      val po = outputs.head
      assert(po.actionType === PrivilegeObjectActionType.UPDATE)
      assert(po.privilegeObjectType === PrivilegeObjectType.TABLE_OR_VIEW)
      assert(po.dbname === namespace)
      assert(po.objectName === table)
      assert(po.columns.isEmpty)
      checkV2TableOwner(po)
      val accessType = AccessType(po, operationType, isInput = false)
      assert(accessType === AccessType.UPDATE)
    }
  }
}
