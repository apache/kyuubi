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

package org.apache.kyuubi.it.flink.operation

import java.io.File
import java.nio.file.Paths

import org.apache.kyuubi.{HADOOP_COMPILE_VERSION, SCALA_COMPILE_VERSION, Utils}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.{ENGINE_TYPE, KYUUBI_ENGINE_ENV_PREFIX}
import org.apache.kyuubi.it.flink.WithKyuubiServerAndFlinkMiniCluster
import org.apache.kyuubi.operation.HiveJDBCTestHelper
import org.apache.kyuubi.operation.meta.ResultSetSchemaConstant.TABLE_CAT

class FlinkOperationSuite extends WithKyuubiServerAndFlinkMiniCluster with HiveJDBCTestHelper {
  val kyuubiHome: String = Utils.getCodeSourceLocation(getClass).split("integration-tests")(0)
  val hadoopClasspath: String = Paths.get(
    kyuubiHome,
    "externals",
    "kyuubi-flink-sql-engine",
    "target",
    s"scala-$SCALA_COMPILE_VERSION",
    "jars").toAbsolutePath.toString
  override val conf: KyuubiConf = KyuubiConf()
    .set(ENGINE_TYPE, "FLINK_SQL")
    .set("flink.parallelism.default", "6")
    .set(
      s"$KYUUBI_ENGINE_ENV_PREFIX.FLINK_HADOOP_CLASSPATH",
      s"$hadoopClasspath${File.separator}" +
        s"hadoop-client-api-$HADOOP_COMPILE_VERSION.jar${File.pathSeparator}" +
        s"$hadoopClasspath${File.separator}hadoop-client-runtime-$HADOOP_COMPILE_VERSION.jar")

  override protected def jdbcUrl: String = getJdbcUrl

  test("get catalogs for flink sql") {
    withJdbcStatement() { statement =>
      val meta = statement.getConnection.getMetaData
      val catalogs = meta.getCatalogs
      val expected = Set("default_catalog").toIterator
      while (catalogs.next()) {
        assert(catalogs.getString(TABLE_CAT) === expected.next())
      }
      assert(!expected.hasNext)
      assert(!catalogs.next())
    }
  }

  test("execute statement - select column name with dots") {
    withJdbcStatement() { statement =>
      val resultSet = statement.executeQuery("select 'tmp.hello'")
      assert(resultSet.next())
      assert(resultSet.getString(1) === "tmp.hello")
    }
  }

  test("set kyuubi conf into flink conf") {
    withJdbcStatement() { statement =>
      val resultSet = statement.executeQuery("SET")
      // Flink does not support set key without value currently,
      // thus read all rows to find the desired one
      var success = false
      while (resultSet.next() && !success) {
        if (resultSet.getString(1) == "parallelism.default" &&
          resultSet.getString(2) == "6") {
          success = true
        }
      }
      assert(success)
    }
  }
}
