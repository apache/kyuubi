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

package org.apache.kyuubi.engine.hive.operation

import org.apache.commons.lang3.{JavaVersion, SystemUtils}

import org.apache.kyuubi.{HiveEngineTests, Utils}
import org.apache.kyuubi.engine.hive.HiveSQLEngine
import org.apache.kyuubi.jdbc.hive.KyuubiStatement

class HiveOperationSuite extends HiveEngineTests {

  override def beforeAll(): Unit = {
    val metastore = Utils.createTempDir(prefix = getClass.getSimpleName)
    metastore.toFile.delete()
    val args = Array(
      "--conf",
      s"javax.jdo.option.ConnectionURL=jdbc:derby:;databaseName=$metastore;create=true")
    HiveSQLEngine.main(args)
    super.beforeAll()
  }

  override protected def jdbcUrl: String = {
    "jdbc:hive2://" + HiveSQLEngine.currentEngine.get.frontendServices.head.connectionUrl + "/;"
  }

  test("test get query id") {
    assume(SystemUtils.isJavaVersionAtMost(JavaVersion.JAVA_1_8))
    withJdbcStatement("hive_engine_test") { statement =>
      statement.execute("CREATE TABLE hive_engine_test(id int, value string) stored as orc")
      statement.execute("INSERT INTO hive_engine_test SELECT 1, '2'")
      statement.executeQuery("SELECT ID, VALUE FROM hive_engine_test")
      val kyuubiStatement = statement.asInstanceOf[KyuubiStatement]
      assert(kyuubiStatement.getQueryId != null)
    }
  }
}
