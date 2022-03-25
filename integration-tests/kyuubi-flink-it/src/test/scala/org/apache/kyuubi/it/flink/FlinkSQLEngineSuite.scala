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

package org.apache.kyuubi.it.flink

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.{ENGINE_TYPE, FRONTEND_THRIFT_BINARY_BIND_PORT}
import org.apache.kyuubi.operation.HiveJDBCTestHelper

class FlinkSQLEngineSuite extends WithKyuubiServerAndFlinkMiniCluster with HiveJDBCTestHelper {

  override val conf: KyuubiConf = KyuubiConf()
    .set(ENGINE_TYPE, "FLINK_SQL")
    .set(FRONTEND_THRIFT_BINARY_BIND_PORT, 10029)
    .set("flink.parallelism.default", "6")

  override protected def jdbcUrl: String = getJdbcUrl

  test("set kyuubi conf into flink conf") {
    withJdbcStatement() { statement =>
      val resultSet = statement.executeQuery("SET")
      // Flink does not support set key without value currently,
      // thus read all rows to find the desired one
      var success = false
      while (resultSet.next() && success == false) {
        if (resultSet.getString(1) == "parallelism.default" &&
          resultSet.getString(2) == "6") {
          success = true
        }
      }
      assert(success)
    }
  }
}
