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

package org.apache.kyuubi.jdbc

import java.util.Properties

import org.apache.kyuubi.IcebergSuiteMixin
import org.apache.kyuubi.engine.spark.WithSparkSQLEngine
import org.apache.kyuubi.engine.spark.shim.SparkCatalogShim
import org.apache.kyuubi.tags.IcebergTest

@IcebergTest
class KyuubiDriverSuite extends WithSparkSQLEngine with IcebergSuiteMixin {

  override def withKyuubiConf: Map[String, String] = extraConfigs -
    "spark.sql.defaultCatalog" -
    "spark.sql.catalog.spark_catalog"

  override def afterAll(): Unit = {
    super.afterAll()
    for ((k, _) <- extraConfigs) {
      System.clearProperty(k)
    }
  }

  test("get tables with kyuubi driver") {
    val kyuubiDriver = new KyuubiDriver()
    val connection = kyuubiDriver.connect(getJdbcUrl, new Properties())
    assert(connection.isInstanceOf[KyuubiConnection])
    val metaData = connection.getMetaData
    assert(metaData.isInstanceOf[KyuubiDatabaseMetaData])
    val statement = connection.createStatement()
    val table1 = s"${SparkCatalogShim.SESSION_CATALOG}.default.kyuubi_hive_jdbc"
    val table2 = s"$catalog.default.hdp_cat_tbl"
    try {
      statement.execute(s"CREATE TABLE $table1(key int) using parquet")
      statement.execute(s"CREATE TABLE $table2(key int) using $format")

      val resultSet1 = metaData.getTables(SparkCatalogShim.SESSION_CATALOG, "default", "%", null)
      assert(resultSet1.next())
      assert(resultSet1.getString(1) === SparkCatalogShim.SESSION_CATALOG)
      assert(resultSet1.getString(2) === "default")
      assert(resultSet1.getString(3) === "kyuubi_hive_jdbc")

      val resultSet2 = metaData.getTables(catalog, "default", "%", null)
      assert(resultSet2.next())
      assert(resultSet2.getString(1) === catalog)
      assert(resultSet2.getString(2) === "default")
      assert(resultSet2.getString(3) === "hdp_cat_tbl")
    } finally {
      statement.execute(s"DROP TABLE $table1")
      statement.execute(s"DROP TABLE $table2")
      statement.close()
      connection.close()
    }
    connection
  }
}
