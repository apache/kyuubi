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

package org.apache.kyuubi.spark.connector.yarn

import java.util.{HashMap => JHashMap}

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.spark.connector.common.LocalSparkSession.withSparkSession

class YarnCatalogSuite extends KyuubiFunSuite {

  test("table operations") {
    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .set("spark.ui.enabled", "false")
      .set("spark.sql.catalogImplementation", "in-memory")
      .set("spark.sql.catalog.yarn", classOf[YarnCatalog].getName)
    withSparkSession(SparkSession.builder.config(sparkConf).getOrCreate()) { _ =>
      val catalog = new YarnCatalog
      val catalogName = "yarn"
      catalog.initialize(catalogName, CaseInsensitiveStringMap.empty())
      assert(catalog.name == catalogName)
      assert(catalog.listTables(Array.empty) === Array(Identifier.of(Array.empty, "app_logs")))
      assert(catalog.listTables(Array("foo")) === Array.empty)
      assert(catalog.loadTable(Identifier.of(Array.empty, "app_logs")).isInstanceOf[YarnLogTable])
      intercept[NoSuchTableException] {
        catalog.loadTable(Identifier.of(Array.empty, "foo"))
      }
      intercept[NoSuchTableException] {
        catalog.loadTable(Identifier.of(Array("foo"), "app_logs"))
      }
      intercept[UnsupportedOperationException] {
        catalog.createTable(
          Identifier.of(Array.empty, "new_table"),
          new StructType,
          Array.empty[Transform],
          new JHashMap[String, String])
      }
      intercept[UnsupportedOperationException] {
        catalog.alterTable(Identifier.of(Array.empty, "app_logs"))
      }
      intercept[UnsupportedOperationException] {
        catalog.dropTable(Identifier.of(Array.empty, "app_logs"))
      }
      intercept[UnsupportedOperationException] {
        catalog.renameTable(
          Identifier.of(Array.empty, "app_logs"),
          Identifier.of(Array.empty, "new_app_logs"))
      }
    }
  }
}
