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

import org.apache.spark.sql.SparkSession

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.plugin.spark.authz.OperationType._

class PrivilegesBuilderSuite extends KyuubiFunSuite {

  private val spark = SparkSession.builder()
    .master("local")
    .config("spark.ui.enabled", "false")
    .getOrCreate()
  private val sql = spark.sql _

  override def afterAll(): Unit = {
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
  }

  test("AlterDatabaseSetLocationCommand") {
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
  }
}
