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

import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
// scalastyle:off
import org.scalatest.funsuite.AnyFunSuite

import org.apache.kyuubi.plugin.spark.authz.OperationType.QUERY
import org.apache.kyuubi.plugin.spark.authz.ranger.AccessType
import org.apache.kyuubi.util.AssertionUtils.assertEqualsIgnoreCase

abstract class FunctionPrivilegesBuilderSuite extends AnyFunSuite
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
    val (in, out, _) = PrivilegesBuilder.build(plan, spark)
    assert(out.isEmpty, "Queries shall not check output privileges")
    val po = in.head
    assert(po.actionType === PrivilegeObjectActionType.OTHER)
    assert(po.privilegeObjectType === PrivilegeObjectType.TABLE_OR_VIEW)
    assert(po.columns === cols)
  }

  protected def checkColumns(query: String, cols: Seq[String]): Unit = {
    checkColumns(sql(query).queryExecution.optimizedPlan, cols)
  }

  protected val HIVE_MASK_HASH_CLASS = "org.apache.hadoop.hive.ql.udf.generic.GenericUDFMaskHash"
  protected val HIVE_FUNCTION_NAME_BASE = "hive_func"
  protected val OBJECT_NAME_SEPARATOR = "_"

  protected val reusedDb: String = getClass.getSimpleName
  protected val reusedDb2: String = getClass.getSimpleName + s"${OBJECT_NAME_SEPARATOR}2"
  protected val reusedTable: String = reusedDb + "." + getClass.getSimpleName
  protected val reusedTableShort: String = reusedTable.split("\\.").last
  protected val reusedPartTable: String = reusedTable + "_part"
  protected val reusedPartTableShort: String = reusedPartTable.split("\\.").last
  protected val functionCount = 3
  protected val functionNamePrefix = "kyuubi_fun_"
  protected val tempFunNamePrefix = "kyuubi_temp_fun_"

  override def beforeAll(): Unit = {
    sql(s"CREATE DATABASE IF NOT EXISTS $reusedDb")
    sql(s"CREATE DATABASE IF NOT EXISTS $reusedDb2")
    sql(s"CREATE TABLE IF NOT EXISTS $reusedTable" +
      s" (key int, value string) USING parquet")
    sql(s"CREATE TABLE IF NOT EXISTS $reusedPartTable" +
      s" (key int, value string, pid string) USING parquet" +
      s"  PARTITIONED BY(pid)")
    // scalastyle:off
    (0 until functionCount).foreach { index =>
      {
        sql(s"CREATE FUNCTION ${reusedDb}.${functionNamePrefix}${index} AS '${HIVE_MASK_HASH_CLASS}'")
        sql(s"CREATE FUNCTION ${reusedDb2}.${functionNamePrefix}${index} AS '${HIVE_MASK_HASH_CLASS}'")
        sql(s"CREATE TEMPORARY FUNCTION ${tempFunNamePrefix}${index} AS '${HIVE_MASK_HASH_CLASS}'")
      }
    }
    sql(s"USE ${reusedDb2}")
    // scalastyle:on
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    Seq(reusedTable, reusedPartTable).foreach { t =>
      sql(s"DROP TABLE IF EXISTS $t")
    }

    Seq(reusedDb, reusedDb2).foreach { db =>
      (0 until functionCount).foreach { index =>
        sql(s"DROP FUNCTION ${db}.${functionNamePrefix}${index}")
      }
      sql(s"DROP DATABASE IF EXISTS ${db}")
    }

    spark.stop()
    super.afterAll()
  }

  protected def checkFunctions(sql: String, count: Int, spark: SparkSession): Unit = {

    val funcNames = {
      0.until(count).map(index => {
        s"${if (index % 2 == 0) reusedDb else reusedDb2}" +
          s".${HIVE_FUNCTION_NAME_BASE}${OBJECT_NAME_SEPARATOR}$index"
      })
    }

    withCleanTmpResources(funcNames.map(funcName =>
      (s"$funcName", "function"))) {
      funcNames.foreach(func =>
        spark.sql(s"CREATE FUNCTION $func AS '$HIVE_MASK_HASH_CLASS'"))
      val plan = spark.sql(sql.format(funcNames: _*)).queryExecution.analyzed
      val (inputs, _, operationType) = PrivilegesBuilder.buildFunctions(plan, spark)
      assert(inputs.size === count)
      inputs.foreach { po =>
        assert(po.actionType === PrivilegeObjectActionType.OTHER)
        assert(po.privilegeObjectType === PrivilegeObjectType.FUNCTION)
        assert(po.catalog.isEmpty)
        assertEqualsIgnoreCase(reusedDb)(getObjectBaseName(po.dbname))
        assertEqualsIgnoreCase(HIVE_FUNCTION_NAME_BASE)(getObjectBaseName(po.objectName))
        assert(po.columns.isEmpty)
        val accessType = ranger.AccessType(po, operationType, isInput = true)
        assert(accessType === AccessType.SELECT)
      }
    }
  }

  private def getObjectBaseName(name: String): String = {
    val nameParts: Array[String] = name.split(OBJECT_NAME_SEPARATOR)
    val parts: Array[String] = if (nameParts.length > 1) {
      nameParts.dropRight(1)
    } else {
      nameParts
    }
    parts.mkString(OBJECT_NAME_SEPARATOR)
  }

}

class HiveFunctionPrivilegesBuilderSuite extends FunctionPrivilegesBuilderSuite {

  override protected val catalogImpl: String = "hive"

  test("built-in function") {
    val plan = sql(s"SELECT max(value) FROM $reusedTable").queryExecution.analyzed
    val (inputs, _, _) = PrivilegesBuilder.buildFunctions(plan, spark)
    assert(inputs.size === 0)
  }

  test("temporary function") {
    val funcName = "temp_fun"
    sql(s"CREATE TEMPORARY FUNCTION $funcName AS '$HIVE_MASK_HASH_CLASS'")
    val plan = sql(s"SELECT $funcName(value) FROM $reusedTable").queryExecution.analyzed
    val (inputs, _, _) = PrivilegesBuilder.buildFunctions(plan, spark)
    assert(inputs.size === 0)
  }

  test("hive permanent function: projection") {
    checkFunctions(s"SELECT %s(value), %s(value), %s(value) FROM $reusedTable", 3, spark)
  }

  test("hive permanent function: alias") {
    checkFunctions(s"SELECT %s(value) AS c1, %s('mask') AS c2 FROM $reusedTable", 2, spark)
  }

  test("hive permanent function: literal") {
    checkFunctions(s"SELECT %s('1') AS c1, %s('2') AS c2", 2, spark)
  }

  test("hive permanent function: where") {
    checkFunctions(
      s"SELECT %s('1') AS c1, %s('2') AS c2, key" +
        s" FROM $reusedTable WHERE %s(key) = 1",
      3,
      spark)

  }

  test("hive permanent function: CTE") {
    checkFunctions(
      s"WITH table(mask_1, mask_2) AS" +
        s" (SELECT %s(value) AS c1, %s(value) AS c2 FROM $reusedTable)" +
        s" SELECT * FROM table",
      2,
      spark)
  }

  test("hive permanent function: command") {
    val tableName = "create_table"
    withCleanTmpResources(Seq((tableName, "table"))) {
      checkFunctions(
        s"CREATE TABLE $tableName AS" +
          s" SELECT %s(value) AS c1, %s(value) AS c2 FROM $reusedTable",
        2,
        spark)
    }
  }

  test("non-exists function") {
    val funcName = "non_exists_fun"
    val e1 = intercept[AnalysisException](
      sql(s"SELECT $reusedDb.$funcName(value) FROM $reusedTable").queryExecution.analyzed)
    e1.getMessage().contains("This function is neither a built-in/temporary function")

  }

  test("unresolved function") {
    val funcName = "hive_func"
    withCleanTmpResources(Seq((funcName, "function"))) {
      sql(s"CREATE FUNCTION $funcName AS '$HIVE_MASK_HASH_CLASS'")
      val plan: LogicalPlan = sql(s"SELECT $funcName(key), value FROM $reusedTable")
        .queryExecution.logical
      val (inputs, _, _) = PrivilegesBuilder.buildFunctions(plan, spark)
      assert(inputs.isEmpty)
    }
  }

  test("Function Call Query") {
    val plan = sql(s"SELECT kyuubi_fun_1('data'), " +
      s"kyuubi_fun_2(value), " +
      s"${reusedDb}.kyuubi_fun_0(value), " +
      s"kyuubi_temp_fun_1('data2')," +
      s"kyuubi_temp_fun_2(key) " +
      s"FROM $reusedTable").queryExecution.analyzed
    val (inputs, _, _) = PrivilegesBuilder.buildFunctions(plan, spark)
    assert(inputs.size === 3)
    inputs.foreach { po =>
      assert(po.actionType === PrivilegeObjectActionType.OTHER)
      assert(po.privilegeObjectType === PrivilegeObjectType.FUNCTION)
      assert(po.dbname startsWith reusedDb.toLowerCase)
      assert(po.objectName startsWith functionNamePrefix.toLowerCase)
      val accessType = ranger.AccessType(po, QUERY, isInput = true)
      assert(accessType === AccessType.SELECT)
    }
  }

  test("Function Call Query with Quoted Name") {
    val plan = sql(s"SELECT `kyuubi_fun_1`('data'), " +
      s"`kyuubi_fun_2`(value), " +
      s"`${reusedDb}`.`kyuubi_fun_0`(value), " +
      s"`kyuubi_temp_fun_1`('data2')," +
      s"`kyuubi_temp_fun_2`(key) " +
      s"FROM $reusedTable").queryExecution.analyzed
    val (inputs, _, _) = PrivilegesBuilder.buildFunctions(plan, spark)
    assert(inputs.size === 3)
    inputs.foreach { po =>
      assert(po.actionType === PrivilegeObjectActionType.OTHER)
      assert(po.privilegeObjectType === PrivilegeObjectType.FUNCTION)
      assert(po.dbname startsWith reusedDb.toLowerCase)
      assert(po.objectName startsWith functionNamePrefix.toLowerCase)
      val accessType = ranger.AccessType(po, QUERY, isInput = true)
      assert(accessType === AccessType.SELECT)
    }
  }

  test("Simple Function Call Query") {
    val plan = sql(s"SELECT kyuubi_fun_1('data'), " +
      s"kyuubi_fun_0('value'), " +
      s"${reusedDb}.kyuubi_fun_0('value'), " +
      s"${reusedDb}.kyuubi_fun_2('value'), " +
      s"kyuubi_temp_fun_1('data2')," +
      s"kyuubi_temp_fun_2('key') ").queryExecution.analyzed
    val (inputs, _, _) = PrivilegesBuilder.buildFunctions(plan, spark)
    assert(inputs.size === 4)
    inputs.foreach { po =>
      assert(po.actionType === PrivilegeObjectActionType.OTHER)
      assert(po.privilegeObjectType === PrivilegeObjectType.FUNCTION)
      assert(po.dbname startsWith reusedDb.toLowerCase)
      assert(po.objectName startsWith functionNamePrefix.toLowerCase)
      val accessType = ranger.AccessType(po, QUERY, isInput = true)
      assert(accessType === AccessType.SELECT)
    }
  }

  test("Function Call In CAST Command") {
    val table = "castTable"
    withTable(table) { table =>
      val plan = sql(s"CREATE TABLE ${table} " +
        s"SELECT kyuubi_fun_1('data') col1, " +
        s"${reusedDb2}.kyuubi_fun_2(value) col2, " +
        s"kyuubi_fun_0(value) col3, " +
        s"kyuubi_fun_2('value') col4, " +
        s"${reusedDb}.kyuubi_fun_2('value') col5, " +
        s"${reusedDb}.kyuubi_fun_1('value') col6, " +
        s"kyuubi_temp_fun_1('data2') col7, " +
        s"kyuubi_temp_fun_2(key) col8 " +
        s"FROM ${reusedTable} WHERE ${reusedDb2}.kyuubi_fun_1(key)='123'").queryExecution.analyzed
      val (inputs, _, _) = PrivilegesBuilder.buildFunctions(plan, spark)
      assert(inputs.size === 7)
      inputs.foreach { po =>
        assert(po.actionType === PrivilegeObjectActionType.OTHER)
        assert(po.privilegeObjectType === PrivilegeObjectType.FUNCTION)
        assert(po.dbname startsWith reusedDb.toLowerCase)
        assert(po.objectName startsWith functionNamePrefix.toLowerCase)
        val accessType = ranger.AccessType(po, QUERY, isInput = true)
        assert(accessType === AccessType.SELECT)
      }
    }
  }

}
