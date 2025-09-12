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

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
// scalastyle:off
import org.scalatest.funsuite.AnyFunSuite

import org.apache.kyuubi.plugin.spark.authz.OperationType.QUERY
import org.apache.kyuubi.plugin.spark.authz.ranger.AccessType

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

  protected val reusedDb: String = getClass.getSimpleName
  protected val reusedDb2: String = getClass.getSimpleName + "2"
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
        sql(s"CREATE FUNCTION ${reusedDb}.${functionNamePrefix}${index} AS 'org.apache.hadoop.hive.ql.udf.generic.GenericUDFMaskHash'")
        sql(s"CREATE FUNCTION ${reusedDb2}.${functionNamePrefix}${index} AS 'org.apache.hadoop.hive.ql.udf.generic.GenericUDFMaskHash'")
        sql(s"CREATE TEMPORARY FUNCTION ${tempFunNamePrefix}${index} AS 'org.apache.hadoop.hive.ql.udf.generic.GenericUDFMaskHash'")
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
}

class HiveFunctionPrivilegesBuilderSuite extends FunctionPrivilegesBuilderSuite {

  override protected val catalogImpl: String = "hive"

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

  test("Built in and UDF Function Call Query") {
    val plan = sql(
      s"""
         |SELECT
         |  kyuubi_fun_0('TESTSTRING') AS col1,
         |  kyuubi_fun_0(value) AS col2,
         |  abs(key) AS col3, abs(-100) AS col4,
         |  lower(value) AS col5,lower('TESTSTRING') AS col6
         |FROM $reusedTable
         |""".stripMargin).queryExecution.analyzed
    val (inputs, _, _) = PrivilegesBuilder.buildFunctions(plan, spark)
    assert(inputs.size === 2)
    inputs.foreach { po =>
      assert(po.actionType === PrivilegeObjectActionType.OTHER)
      assert(po.privilegeObjectType === PrivilegeObjectType.FUNCTION)
      assert(po.dbname startsWith reusedDb.toLowerCase)
      assert(po.objectName startsWith functionNamePrefix.toLowerCase)
      val accessType = ranger.AccessType(po, QUERY, isInput = true)
      assert(accessType === AccessType.SELECT)
    }
  }

  test("Function Call in Create Table/View") {
    val plan1 = sql(
      s"""
         |CREATE TABLE table1 AS
         |SELECT
         |  kyuubi_fun_0('KYUUBI_TESTSTRING'),
         |  kyuubi_fun_0(value)
         |FROM $reusedTable
         |""".stripMargin).queryExecution.analyzed
    val (inputs1, _, _) = PrivilegesBuilder.buildFunctions(plan1, spark)
    assert(inputs1.size === 2)
    inputs1.foreach { po =>
      assert(po.actionType === PrivilegeObjectActionType.OTHER)
      assert(po.privilegeObjectType === PrivilegeObjectType.FUNCTION)
      assert(po.dbname startsWith reusedDb.toLowerCase)
      assert(po.objectName startsWith functionNamePrefix.toLowerCase)
      val accessType = ranger.AccessType(po, QUERY, isInput = true)
      assert(accessType === AccessType.SELECT)
    }
    val plan2 = sql("DROP TABLE IF EXISTS table1").queryExecution.analyzed
    val (inputs2, _, _) = PrivilegesBuilder.buildFunctions(plan2, spark)
    assert(inputs2.size === 0)

    val plan3 = sql(
      s"""
         |CREATE VIEW view1 AS SELECT
         |  kyuubi_fun_0('KYUUBI_TESTSTRING') AS fun1,
         |  kyuubi_fun_0(value) AS fun2
         |FROM $reusedTable
         |""".stripMargin).queryExecution.analyzed
    val (inputs3, _, _) = PrivilegesBuilder.buildFunctions(plan3, spark)
    assert(inputs3.size === 2)
    inputs3.foreach { po =>
      assert(po.actionType === PrivilegeObjectActionType.OTHER)
      assert(po.privilegeObjectType === PrivilegeObjectType.FUNCTION)
      assert(po.dbname startsWith reusedDb.toLowerCase)
      assert(po.objectName startsWith functionNamePrefix.toLowerCase)
      val accessType = ranger.AccessType(po, QUERY, isInput = true)
      assert(accessType === AccessType.SELECT)
    }
    val plan4 = sql("DROP VIEW IF EXISTS view1").queryExecution.analyzed
    val (inputs4, _, _) = PrivilegesBuilder.buildFunctions(plan4, spark)
    assert(inputs4.size === 0)
  }

  test("Function Call in INSERT OVERWRITE") {
    val plan = sql(
      s"""
         |INSERT OVERWRITE TABLE $reusedTable
         |SELECT key, kyuubi_fun_0(value)
         |FROM $reusedPartTable
         |""".stripMargin).queryExecution.analyzed
    val (inputsUpdate, _, _) = PrivilegesBuilder.buildFunctions(plan, spark)
    assert(inputsUpdate.size === 1)
    inputsUpdate.foreach { po =>
      assert(po.actionType === PrivilegeObjectActionType.OTHER)
      assert(po.privilegeObjectType === PrivilegeObjectType.FUNCTION)
      assert(po.dbname startsWith reusedDb.toLowerCase)
      assert(po.objectName startsWith functionNamePrefix.toLowerCase)
      val accessType = ranger.AccessType(po, QUERY, isInput = true)
      assert(accessType === AccessType.SELECT)
    }
  }
}
