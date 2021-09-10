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

import org.apache.kyuubi.WithKyuubiServer
import org.apache.kyuubi.config.KyuubiConf

class PlanOnlyOperationSuite extends WithKyuubiServer with JDBCTestUtils {

  override protected val conf: KyuubiConf = {
    KyuubiConf()
      .set(KyuubiConf.ENGINE_SHARE_LEVEL, "user")
      .set(KyuubiConf.OPERATION_PLAN_ONLY, "optimize")
      .set(KyuubiConf.ENGINE_SHARE_LEVEL_SUBDOMAIN.key, "plan-only")
  }

  override protected def jdbcUrl: String = getJdbcUrl

  test("KYUUBI #1059: Plan only operation with system defaults") {
    withJdbcStatement() { statement =>
      val set = statement.executeQuery("select 1 where true")
      assert(set.next())
      val res = set.getString(1)
      assert(res.startsWith("Project") && !res.contains("Filter"))
    }
  }

  test("KYUUBI #1059: Plan only operation with with session conf") {
    withSessionConf()(Map(KyuubiConf.OPERATION_PLAN_ONLY.key -> "analyze"))(Map.empty) {
      withJdbcStatement() { statement =>
        val set = statement.executeQuery("select 1 where true")
        assert(set.next())
        val res = set.getString(1)
        assert(res.startsWith("Project") && res.contains("Filter"))
      }
    }
  }

  test("KYUUBI #1059: Plan only operation with with set command") {
    withSessionConf()(Map(KyuubiConf.OPERATION_PLAN_ONLY.key -> "analyze"))(Map.empty) {
      withJdbcStatement() { statement =>
        statement.execute(s"set ${KyuubiConf.OPERATION_PLAN_ONLY.key}=parse")
        val set = statement.executeQuery("select 1 where true")
        assert(set.next())
        val res = set.getString(1)
        assert(res.startsWith("'Project"))
      }
    }
  }
}
