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

package org.apache.kyuubi.plugin.lineage.helper

import org.apache.spark.SparkConf
import org.apache.spark.kyuubi.lineage.LineageConf.COLLECT_INPUT_TABLES_BY_PLAN_ENABLED

import org.apache.kyuubi.plugin.lineage.Lineage

class CollectInputTablesByPlanSuite extends SparkSQLLineageParserHelperSuite {

  override def catalogName: String = {
    "org.apache.spark.sql.connector.catalog.InMemoryTableCatalog"
  }

  override def sparkConf(): SparkConf = {
    super.sparkConf()
      .set(COLLECT_INPUT_TABLES_BY_PLAN_ENABLED.key, "true")
  }

  test("columns lineage extract - MergeIntoTable") {
    val ddls =
      """
        |create table v2_catalog.db.target_t(id int, name string, price float)
        |create table v2_catalog.db.source_t(id int, name string, price float)
        |create table v2_catalog.db.pivot_t(id int, price float)
        |""".stripMargin
    ddls.split("\n").filter(_.nonEmpty).foreach(spark.sql(_).collect())
    withTable("v2_catalog.db.target_t", "v2_catalog.db.source_t", "v2_catalog.db.pivot_t") { _ =>
      val ret0 = extractLineageWithoutExecuting("MERGE INTO v2_catalog.db.target_t AS target " +
        "USING v2_catalog.db.source_t AS source " +
        "ON target.id = source.id " +
        "WHEN MATCHED THEN " +
        "  UPDATE SET target.name = source.name, target.price = source.price " +
        "WHEN NOT MATCHED THEN " +
        "  INSERT (id, name, price) VALUES (cast(source.id as int), source.name, source.price)")
      assert(ret0 == Lineage(
        List("v2_catalog.db.source_t", "v2_catalog.db.target_t"),
        List("v2_catalog.db.target_t"),
        List(
          ("v2_catalog.db.target_t.id", Set("v2_catalog.db.source_t.id")),
          ("v2_catalog.db.target_t.name", Set("v2_catalog.db.source_t.name")),
          ("v2_catalog.db.target_t.price", Set("v2_catalog.db.source_t.price")))))
    }

  }
}
