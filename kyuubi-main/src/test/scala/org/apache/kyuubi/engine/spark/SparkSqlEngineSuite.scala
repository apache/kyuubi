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

package org.apache.kyuubi.engine.spark

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.ENGINE_INITIALIZE_SQL
import org.apache.kyuubi.operation.{JDBCTestUtils, WithKyuubiServer}

class SparkSqlEngineSuite extends WithKyuubiServer with JDBCTestUtils  {
  override protected val conf: KyuubiConf = {
    KyuubiConf().set(ENGINE_INITIALIZE_SQL.key,
      "CREATE DATABASE IF NOT EXISTS INIT_DB;" +
        "CREATE TABLE IF NOT EXISTS INIT_DB.test(a int);" +
        "INSERT OVERWRITE TABLE INIT_DB.test SELECT 1;")
  }

  override def afterAll(): Unit = {
    withJdbcStatement() { statement =>
      statement.executeQuery("DROP TABLE IF EXISTS INIT_DB.test")
      statement.executeQuery("DROP DATABASE IF EXISTS INIT_DB")
    }
    super.afterAll()
  }

  test("KYUUBI-457: Support configurable initialize sql statement for engine startup") {
    withJdbcStatement() { statement =>
      val result = statement.executeQuery("SELECT * FROM INIT_DB.test")
      assert(result.next())
      assert(result.getInt(1) == 1)
      assert(!result.next())
    }
  }

  override protected def jdbcUrl: String = getJdbcUrl
}
