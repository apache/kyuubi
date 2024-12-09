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

package org.apache.kyuubi.spark.connector.hive.command

import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.types.{StringType, StructType}

import org.apache.kyuubi.spark.connector.common.SparkUtils.SPARK_RUNTIME_VERSION
import org.apache.kyuubi.spark.connector.hive.command.DDLCommandTestUtils.{V1_COMMAND_VERSION, V2_COMMAND_VERSION}
import org.apache.kyuubi.util.AssertionUtils.interceptContains

trait DropNamespaceSuiteBase extends DDLCommandTestUtils {
  override protected def command: String = "DROP NAMESPACE"

  private def namespace: String = "fakens"

  protected def notFoundMsgPrefix: String =
    if (commandVersion == V1_COMMAND_VERSION) "Database" else "Namespace"

  protected def checkNamespace(expected: Seq[String]) = {
    val df = sql(s"SHOW NAMESPACES IN $catalogName")
    assert(df.schema === new StructType().add("namespace", StringType, false))
    checkAnswer(df, expected.map(Row(_)))
  }

  override def afterEach(): Unit = {
    sql(s"DROP NAMESPACE IF EXISTS $catalogName.$namespace CASCADE")
    super.afterEach()
  }

  test("basic tests") {
    sql(s"CREATE NAMESPACE $catalogName.$namespace")
    checkNamespace(Seq(namespace) ++ builtinNamespace)

    sql(s"DROP NAMESPACE $catalogName.$namespace")
    checkNamespace(builtinNamespace)
  }

  test("test handling of 'IF EXISTS'") {
    // It must not throw any exceptions
    sql(s"DROP NAMESPACE IF EXISTS $catalogName.unknown")
    checkNamespace(builtinNamespace)
  }

  test("namespace does not exist") {
    // Namespace $catalog.unknown does not exist.
    val message = intercept[AnalysisException] {
      sql(s"DROP NAMESPACE $catalogName.unknown")
    }.getMessage
    assert(message.contains(s"'unknown' not found") ||
      message.contains(s"The schema `unknown` cannot be found") ||
      message.contains("SCHEMA_NOT_FOUND"))
  }

  test("drop non-empty namespace with a non-cascading mode") {
    sql(s"CREATE NAMESPACE $catalogName.$namespace")
    sql(s"CREATE TABLE $catalogName.$namespace.table (id bigint) USING parquet")
    checkNamespace(Seq(namespace) ++ builtinNamespace)

    // $catalog.ns.table is present, thus $catalog.ns cannot be dropped.
    interceptContains[AnalysisException] {
      sql(s"DROP NAMESPACE $catalogName.$namespace")
    }(if (SPARK_RUNTIME_VERSION >= "3.4") {
      s"[SCHEMA_NOT_EMPTY] Cannot drop a schema `$namespace` because it contains objects"
    } else {
      "Use CASCADE option to drop a non-empty database"
    })

    sql(s"DROP TABLE $catalogName.$namespace.table")

    // Now that $catalog.ns is empty, it can be dropped.
    sql(s"DROP NAMESPACE $catalogName.$namespace")
    checkNamespace(builtinNamespace)
  }

  test("drop non-empty namespace with a cascade mode") {
    sql(s"CREATE NAMESPACE $catalogName.$namespace")
    sql(s"CREATE TABLE $catalogName.$namespace.table (id bigint) USING parquet")
    checkNamespace(Seq(namespace) ++ builtinNamespace)

    sql(s"DROP NAMESPACE $catalogName.$namespace CASCADE")
    checkNamespace(builtinNamespace)
  }

  test("drop current namespace") {
    sql(s"CREATE NAMESPACE $catalogName.$namespace")
    sql(s"USE $catalogName.$namespace")
    sql(s"DROP NAMESPACE $catalogName.$namespace")
    checkNamespace(builtinNamespace)
  }
}

class DropNamespaceV2Suite extends DropNamespaceSuiteBase {

  override protected def catalogVersion: String = "Hive V2"

  override protected def commandVersion: String = V2_COMMAND_VERSION
}

class DropNamespaceV1Suite extends DropNamespaceSuiteBase {

  val SESSION_CATALOG_NAME: String = "spark_catalog"

  override protected val catalogName: String = SESSION_CATALOG_NAME

  override protected def catalogVersion: String = "V1"

  override protected def commandVersion: String = V1_COMMAND_VERSION
}
