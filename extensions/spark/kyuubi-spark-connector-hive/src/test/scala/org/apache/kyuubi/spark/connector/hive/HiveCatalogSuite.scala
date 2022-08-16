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

package org.apache.kyuubi.spark.connector.hive

import org.apache.spark.SparkConf
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.spark.connector.common.LocalSparkSession.withSparkSession

class HiveCatalogSuite extends KyuubiFunSuite {

  test("get catalog name") {
    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .set("spark.ui.enabled", "false")
      .set("spark.sql.catalogImplementation", "hive")
      .set("spark.sql.catalog.v2hive", classOf[HiveTableCatalog].getName)
    withSparkSession(SparkSession.builder.config(sparkConf).getOrCreate()) { spark =>
      val catalog = new HiveTableCatalog
      val catalogName = "v2hive"
      catalog.initialize(catalogName, CaseInsensitiveStringMap.empty())
      assert(catalog.name() == catalogName)
    }
  }

  test("supports namespaces") {
    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .set("spark.ui.enabled", "false")
      .set("spark.sql.catalogImplementation", "hive")
      .set("spark.sql.catalog.v2hive_namespaces", classOf[HiveTableCatalog].getName)
    withSparkSession(SparkSession.builder.config(sparkConf).getOrCreate()) { spark =>
      try {
        spark.sql("USE v2hive_namespaces")
        spark.sql("CREATE NAMESPACE IF NOT EXISTS ns1")
        assert(spark.sql(s"SHOW NAMESPACES").collect().length == 2)
      } finally {
        spark.sql("DROP NAMESPACE IF EXISTS ns1")
      }
    }
  }

  test("nonexistent table") {
    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .set("spark.ui.enabled", "false")
      .set("spark.sql.catalogImplementation", "hive")
      .set("spark.sql.catalog.v2hive", classOf[HiveTableCatalog].getName)
    withSparkSession(SparkSession.builder.config(sparkConf).getOrCreate()) { spark =>
      val exception = intercept[AnalysisException] {
        spark.table("v2hive.ns1.nonexistent_table")
      }
      assert(exception.message === "Table or view not found: v2hive.ns1.nonexistent_table")
    }
  }
}
