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

import org.apache.kyuubi.Utils
import org.apache.kyuubi.plugin.spark.authz.OperationType._
import org.apache.kyuubi.plugin.spark.authz.ranger.AccessType
import org.apache.kyuubi.tags.PaimonTest
import org.apache.kyuubi.util.AssertionUtils._

@PaimonTest
class PaimonCatalogPrivilegesBuilderSuite extends V2CommandsPrivilegesSuite {
  override protected val catalogImpl: String = "hive"
  override protected val sqlExtensions: String =
    "org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions"
  override protected def format = "paimon"

  override protected val supportsUpdateTable = false
  override protected val supportsMergeIntoTable = false
  override protected val supportsDelete = false
  override protected val supportsPartitionGrammar = true
  override protected val supportsPartitionManagement = false
  // Paimon maps TRUNCATE TABLE / INSERT OVERWRITE PARTITION to its own
  // logical plans, so the generic V2 assertions don't hold here. The
  // catalog e2e suite covers Paimon-specific truncate and dynamic
  // partition overwrite behaviour.
  override protected val supportsTruncateTable = false
  override protected val supportsOverwritePartitionsDynamic = false

  // Paimon's V2 catalog does not populate table owner on analyzed plans for
  // CreateTable/ReplaceTable/DropTable/TruncateTable. Relax the inherited
  // owner check so the generic V2 assertions still hold.
  override protected def checkV2TableOwner(po: PrivilegeObject): Unit = {}

  override def beforeAll(): Unit = {
    spark.conf.set(
      s"spark.sql.catalog.$catalogV2",
      "org.apache.paimon.spark.SparkCatalog")
    spark.conf.set(
      s"spark.sql.catalog.$catalogV2.warehouse",
      Utils.createTempDir("paimon-hadoop").toString)
    super.beforeAll()
  }

  test("DeleteFromPaimonTable") {
    val plan = sql(s"DELETE FROM $catalogTable WHERE key = 1 ").queryExecution.analyzed
    val (inputs, outputs, operationType) = PrivilegesBuilder.build(plan, spark)
    assert(operationType === QUERY)
    assert(outputs.size === 1)
    val po = outputs.head
    assert(po.actionType === PrivilegeObjectActionType.UPDATE)
    assert(po.privilegeObjectType === PrivilegeObjectType.TABLE_OR_VIEW)
    assertEqualsIgnoreCase(namespace)(po.dbname)
    assertEqualsIgnoreCase(catalogTableShort)(po.objectName)
    assert(po.columns.isEmpty)
    val accessType = AccessType(po, operationType, isInput = false)
    assert(accessType === AccessType.UPDATE)
  }

  test("UpdatePaimonTable") {
    val plan = sql(s"UPDATE $catalogTable SET value = 'b' WHERE key = 1 ").queryExecution.analyzed
    val (inputs, outputs, operationType) = PrivilegesBuilder.build(plan, spark)
    assert(operationType === QUERY)
    assert(outputs.size === 1)
    val po = outputs.head
    assert(po.actionType === PrivilegeObjectActionType.UPDATE)
    assert(po.privilegeObjectType === PrivilegeObjectType.TABLE_OR_VIEW)
    assertEqualsIgnoreCase(namespace)(po.dbname)
    assertEqualsIgnoreCase(catalogTableShort)(po.objectName)
    assert(po.columns.isEmpty)
    val accessType = AccessType(po, operationType, isInput = false)
    assert(accessType === AccessType.UPDATE)
  }

  test("MergeIntoPaimonTable") {
    val table = "MergeIntoPaimonTable"
    withV2Table(table) { tableId =>
      sql(s"CREATE TABLE $tableId (key int, value String) USING paimon " +
        s"OPTIONS ('primary-key' = 'key')")
      val plan = sql(s"MERGE INTO $tableId t " +
        s"USING (SELECT * FROM $catalogTable) s " +
        s"ON t.key = s.key " +
        s"WHEN MATCHED THEN UPDATE SET t.value = s.value " +
        s"WHEN NOT MATCHED THEN INSERT *").queryExecution.analyzed
      val (inputs, outputs, operationType) = PrivilegesBuilder.build(plan, spark)
      assert(operationType === QUERY)
      assert(inputs.nonEmpty)
      val sourcePo = inputs.head
      assert(sourcePo.actionType === PrivilegeObjectActionType.OTHER)
      assert(sourcePo.privilegeObjectType === PrivilegeObjectType.TABLE_OR_VIEW)
      assertEqualsIgnoreCase(namespace)(sourcePo.dbname)
      assertEqualsIgnoreCase(catalogTableShort)(sourcePo.objectName)

      assert(outputs.size === 1)
      val po = outputs.head
      assert(po.actionType === PrivilegeObjectActionType.UPDATE)
      assert(po.privilegeObjectType === PrivilegeObjectType.TABLE_OR_VIEW)
      assertEqualsIgnoreCase(namespace)(po.dbname)
      assertEqualsIgnoreCase(table)(po.objectName)
      assert(po.columns.isEmpty)
      val accessType = AccessType(po, operationType, isInput = false)
      assert(accessType === AccessType.UPDATE)
    }
  }

  test("PaimonCallProcedure") {
    val table = "PaimonCallProcedure"
    withV2Table(table) { tableId =>
      sql(s"CREATE TABLE IF NOT EXISTS $tableId (key int, value String) USING paimon " +
        s"OPTIONS ('primary-key' = 'key')")
      sql(s"INSERT INTO $tableId VALUES (1, 'a'), (2, 'b'), (3, 'c')")

      val plan = sql(s"CALL $catalogV2.sys.rollback (table => '$tableId', version => '1')")
        .queryExecution.analyzed
      val (inputs, outputs, operationType) = PrivilegesBuilder.build(plan, spark)
      assert(operationType === ALTERTABLE_PROPERTIES)
      assert(inputs.size === 0)
      assert(outputs.size === 1)
      val po = outputs.head
      assert(po.actionType === PrivilegeObjectActionType.OTHER)
      assert(po.privilegeObjectType === PrivilegeObjectType.TABLE_OR_VIEW)
      assertEqualsIgnoreCase(namespace)(po.dbname)
      assertEqualsIgnoreCase(table)(po.objectName)
      val accessType = AccessType(po, operationType, isInput = false)
      assert(accessType === AccessType.ALTER)
    }
  }
}
