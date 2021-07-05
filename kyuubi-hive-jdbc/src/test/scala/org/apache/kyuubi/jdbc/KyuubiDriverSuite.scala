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

import org.apache.kyuubi.engine.spark.WithSparkSQLEngine
import org.apache.kyuubi.engine.spark.shim.SparkCatalogShim

class KyuubiDriverSuite extends WithSparkSQLEngine {

  test("get tables with kyuubi driver") {
    val kyuubiDriver = new KyuubiDriver()
    val connection = kyuubiDriver.connect(getJdbcUrl, new Properties())
    val statement = connection.createStatement()
    val table = s"${SparkCatalogShim.SESSION_CATALOG}.default.kyuubi_hive_jdbc"
    try {
      statement.execute(s"CREATE TABLE ${table}(key int) using parquet")
      assert(connection.isInstanceOf[KyuubiConnection])
      val metaData = connection.getMetaData
      assert(metaData.isInstanceOf[KyuubiDatabaseMetaData])
      val resultSet = metaData.getTables(SparkCatalogShim.SESSION_CATALOG, "default", "%", null)
      assert(resultSet.next())
      assert(resultSet.getString(1) === SparkCatalogShim.SESSION_CATALOG)
      assert(resultSet.getString(2) === "default")
      assert(resultSet.getString(3) === "kyuubi_hive_jdbc")
    } finally {
      statement.execute(s"DROP TABLE ${table}")
      statement.close()
      connection.close()
    }
    connection
  }

  override def withKyuubiConf: Map[String, String] = Map.empty
}
