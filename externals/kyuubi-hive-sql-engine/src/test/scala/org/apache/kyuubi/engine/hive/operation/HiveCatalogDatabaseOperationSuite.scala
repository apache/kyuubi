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

import org.apache.kyuubi.Utils
import org.apache.kyuubi.config.KyuubiConf.ENGINE_OPERATION_CONVERT_CATALOG_DATABASE_ENABLED
import org.apache.kyuubi.config.KyuubiReservedKeys
import org.apache.kyuubi.engine.hive.HiveSQLEngine
import org.apache.kyuubi.operation.HiveJDBCTestHelper
import org.apache.kyuubi.util.command.CommandLineUtils._

class HiveCatalogDatabaseOperationSuite extends HiveJDBCTestHelper {

  override def beforeAll(): Unit = {
    val metastore = Utils.createTempDir(prefix = getClass.getSimpleName)
    metastore.toFile.delete()
    val args = Array(
      CONF,
      s"javax.jdo.option.ConnectionURL=jdbc:derby:;databaseName=$metastore;create=true",
      CONF,
      s"${ENGINE_OPERATION_CONVERT_CATALOG_DATABASE_ENABLED.key}=true",
      CONF,
      s"${KyuubiReservedKeys.KYUUBI_SESSION_USER_KEY}=kyuubi")
    HiveSQLEngine.main(args)
    super.beforeAll()
  }

  override protected def jdbcUrl: String = {
    "jdbc:hive2://" + HiveSQLEngine.currentEngine.get.frontendServices.head.connectionUrl + "/;"
  }

  test("test set/get catalog") {
    assume(SystemUtils.isJavaVersionAtMost(JavaVersion.JAVA_1_8))
    withJdbcStatement()({ statement =>
      val catalog = statement.getConnection.getCatalog
      assert(catalog == "")
      statement.getConnection.setCatalog("hive_not_support_catalog")
      val changedCatalog = statement.getConnection.getCatalog
      assert(changedCatalog == "")
    })
  }

  test("test set/get database") {
    assume(SystemUtils.isJavaVersionAtMost(JavaVersion.JAVA_1_8))
    withJdbcStatement()({ statement =>
      statement.execute("create database test_hive_db")
      val schema = statement.getConnection.getSchema
      assert(schema == "default")
      statement.getConnection.setSchema("test_hive_db")
      val changedSchema = statement.getConnection.getSchema
      assert(changedSchema == "test_hive_db")
      statement.execute("drop database test_hive_db")
    })
  }
}
