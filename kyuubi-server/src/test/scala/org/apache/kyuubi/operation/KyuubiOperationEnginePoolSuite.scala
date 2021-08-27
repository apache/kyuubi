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

import org.scalatest.time.SpanSugar._

import org.apache.kyuubi.WithKyuubiServer
import org.apache.kyuubi.config.KyuubiConf

class KyuubiOperationEnginePoolSuite extends WithKyuubiServer with JDBCTestUtils {

  override protected def jdbcUrl: String = getJdbcUrl

  override protected val conf: KyuubiConf = {
    KyuubiConf()
  }

  test("ensure app name contains engine-pool when engine pool is enabled.") {
    withSessionConf()(
      Map(
        KyuubiConf.ENGINE_SHARE_LEVEL.key -> "user",
        KyuubiConf.ENGINE_POOL_SIZE.key -> "2"
      ))(Map.empty) {

      var r1: String = null
      withJdbcStatement() { statement =>
        val res = statement.executeQuery("set spark.app.name")
        assert(res.next())
        r1 = res.getString("value")
      }

      eventually(timeout(120.seconds), interval(100.milliseconds)) {
        assert(r1 != null)
      }

      assert(r1.contains("engine-pool-"))
    }
  }

  test("ensure the sub-domain doesn't work with the CONNECTION share level.") {
    withSessionConf()(
      Map(
        KyuubiConf.ENGINE_SHARE_LEVEL.key -> "connection",
        KyuubiConf.ENGINE_POOL_SIZE.key -> "2"
      ))(Map.empty) {

      var r1: String = null
      withJdbcStatement() { statement =>
        val res = statement.executeQuery("set spark.app.name")
        assert(res.next())
        r1 = res.getString("value")
      }

      eventually(timeout(120.seconds), interval(100.milliseconds)) {
        assert(r1 != null)
      }

      assert(r1.contains("engine-pool-") === false)
    }
  }
}
