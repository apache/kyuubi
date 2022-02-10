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

package org.apache.kyuubi.engine.trino.session

import org.apache.kyuubi.config.KyuubiConf.ENGINE_SHARE_LEVEL
import org.apache.kyuubi.config.KyuubiConf.ENGINE_TRINO_CONNECTION_CATALOG
import org.apache.kyuubi.engine.trino.WithTrinoEngine
import org.apache.kyuubi.operation.HiveJDBCTestHelper

class SessionSuite extends WithTrinoEngine with HiveJDBCTestHelper {
  override def withKyuubiConf: Map[String, String] = Map(
    ENGINE_TRINO_CONNECTION_CATALOG.key -> "memory",
    ENGINE_SHARE_LEVEL.key -> "SERVER")

  override protected val schema = "default"

  override protected def jdbcUrl: String = getJdbcUrl

  test("test session") {
    withJdbcStatement() { statement =>
      statement.executeQuery("create or replace view temp_view as select 1 as id")
      val resultSet = statement.executeQuery("select * from temp_view")
      assert(resultSet.next())
    }
  }

}
