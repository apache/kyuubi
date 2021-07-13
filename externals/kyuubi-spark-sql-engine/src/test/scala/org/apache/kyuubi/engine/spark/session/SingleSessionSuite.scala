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

package org.apache.kyuubi.engine.spark.session

import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.engine.spark.WithSparkSQLEngine
import org.apache.kyuubi.operation.JDBCTestUtils

class SingleSessionSuite extends WithSparkSQLEngine with JDBCTestUtils {

  override def withKyuubiConf: Map[String, String] = {
    Map(ENGINE_SHARE_LEVEL.key -> "SERVER",
      ENGINE_SINGLE_SPARK_SESSION.key -> "true")
  }

  override protected def jdbcUrl: String = s"jdbc:hive2://${engine.connectionUrl}/;#" +
    s"spark.ui.enabled=false"

  test("test single session") {
    withJdbcStatement() { statement =>
      statement.execute("create or replace temporary view temp_view as select 1")
    }
    withJdbcStatement() { statement =>
      val resultSet = statement.executeQuery("select * from temp_view")
      assert(resultSet.next())
    }
  }

}
