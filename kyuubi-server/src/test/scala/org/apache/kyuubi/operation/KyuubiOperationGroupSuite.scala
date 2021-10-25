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

package org.apache.kyuubi.operation

import org.apache.hadoop.security.UserGroupInformation

import org.apache.kyuubi.WithKyuubiServer
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.util.KyuubiHadoopUtils

class KyuubiOperationGroupSuite extends WithKyuubiServer with BasicQueryTests {

  override protected def jdbcUrl: String = getJdbcUrl

  override protected val conf: KyuubiConf = {
    val c = KyuubiConf().set(KyuubiConf.ENGINE_SHARE_LEVEL, "group")
      .set("hadoop.user.group.static.mapping.overrides",
        s"user1=testGG,group_tt;user2=testGG")
    UserGroupInformation.setConfiguration(KyuubiHadoopUtils.newHadoopConf(c))
    c.set(s"hadoop.proxyuser.$user.groups", "*")
      .set(s"hadoop.proxyuser.$user.hosts", "*")
  }

  test("ensure two connections in group mode share the same engine started by primary group") {
    var r1: String = null
    var r2: String = null
    withSessionConf(Map("hive.server2.proxy.user" -> "user1"))(Map.empty)(Map.empty) {
      withJdbcStatement() { statement =>
        val res = statement.executeQuery("set spark.app.name")
        assert(res.next())
        r1 = res.getString("value")
      }
    }

    withSessionConf(Map("hive.server2.proxy.user" -> "user2"))(Map.empty)(Map.empty) {
      withJdbcStatement() { statement =>
        val res = statement.executeQuery("set spark.app.name")
        assert(res.next())
        r2 = res.getString("value")
      }
    }
    assert(r1 != null && r2 != null)
    assert(r1 === r2)
    assert(r1.startsWith(s"kyuubi_GROUP_testGG"))
  }


  test("kyuubi defined function - system_user/session_user") {
    withSessionConf(Map("hive.server2.proxy.user" -> "user1"))(Map.empty)(Map.empty) {
      withJdbcStatement() { statement =>
        val res = statement.executeQuery("select system_user() as c1, session_user() as c2")
        assert(res.next())
        assert(res.getString("c1") === "testGG")
        assert(res.getString("c2") === "user1")
      }
    }
  }
}
