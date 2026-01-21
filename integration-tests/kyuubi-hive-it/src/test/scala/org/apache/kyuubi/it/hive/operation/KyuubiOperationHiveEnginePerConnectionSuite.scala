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

package org.apache.kyuubi.it.hive.operation

import org.apache.commons.lang3.{JavaVersion, SystemUtils}

import org.apache.kyuubi.{Utils, WithKyuubiServer}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.operation.HiveJDBCTestHelper
import org.apache.kyuubi.util.JavaUtils

class KyuubiOperationHiveEnginePerConnectionSuite extends WithKyuubiServer with HiveJDBCTestHelper {

  val kyuubiHome: String = JavaUtils.getCodeSourceLocation(getClass).split("integration-tests").head

  override protected val conf: KyuubiConf = {
    val metastore = Utils.createTempDir(prefix = getClass.getSimpleName)
    metastore.toFile.delete()
    val currentUser = Utils.currentUser
    KyuubiConf()
      .set(s"$KYUUBI_ENGINE_ENV_PREFIX.$KYUUBI_HOME_ENV_VAR_NAME", kyuubiHome)
      .set(ENGINE_TYPE, "HIVE_SQL")
      .set(ENGINE_SHARE_LEVEL, "connection")
      // increase this to 30s as hive session state and metastore client is slow initializing
      .setIfMissing(ENGINE_IDLE_TIMEOUT, 30000L)
      .set("javax.jdo.option.ConnectionURL", s"jdbc:derby:;databaseName=$metastore;create=true")
      .set(s"hadoop.proxyuser.$currentUser.groups", "*")
      .set(s"hadoop.proxyuser.$currentUser.hosts", "*")
  }

  test("KYUUBI #2604: multi tenancy support") {
    assume(SystemUtils.isJavaVersionAtMost(JavaVersion.JAVA_1_8))
    val currentUser = Utils.currentUser
    val proxyUser = currentUser + "proxy"
    withSessionConf(Map("hive.server2.proxy.user" -> proxyUser))(
      Map("kyuubi.engine.share.level" -> "connection"))() {
      withJdbcStatement() { statement =>
        val rs = statement.executeQuery("SELECT current_user() as col")
        assert(rs.next())
        assert(rs.getString("col") === proxyUser)
      }
    }
  }

  override protected def jdbcUrl: String = getJdbcUrl
}
