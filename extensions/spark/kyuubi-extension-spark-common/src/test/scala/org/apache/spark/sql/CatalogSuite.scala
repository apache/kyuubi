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

package org.apache.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalog.ShowCatalogsCommand
import org.apache.spark.sql.internal.StaticSQLConf

class CatalogSuite extends KyuubiSparkSQLExtensionTest {

  override def sparkConf(): SparkConf = {
    super.sparkConf()
      .set(
        StaticSQLConf.SPARK_SESSION_EXTENSIONS.key,
        "org.apache.kyuubi.sql.KyuubiSparkSQLCommonExtension")
      .set("spark.sql.catalog.clickhouse", "ClickHouseCatalogImpl")
      .set("spark.sql.catalog.kudu", "KuduClickHouseImpl")
  }

  test("Analyze: SHOW CATALOGS") {
    comparePlans(
      sql("SHOW CATALOGS").queryExecution.analyzed,
      ShowCatalogsCommand(None))
    comparePlans(
      sql("SHOW CATALOGS LIKE 'defau*'").queryExecution.analyzed,
      ShowCatalogsCommand(Some("defau*")))
  }

  test("Execute: SHOW CATALOGS") {
    checkAnswer(
      sql("SHOW CATALOGS"),
      Array(Row("clickhouse"), Row("kudu"), Row("spark_catalog")))

    checkAnswer(
      sql("SHOW CATALOGS LIKE 'click*'"),
      Array(Row("clickhouse")))
  }
}
