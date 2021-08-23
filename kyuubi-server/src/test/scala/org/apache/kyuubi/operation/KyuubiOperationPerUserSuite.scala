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
import org.apache.kyuubi.ha.HighAvailabilityConf

class KyuubiOperationPerUserSuite extends WithKyuubiServer with JDBCTests {

  override protected def jdbcUrl: String = getJdbcUrl

  override protected val conf: KyuubiConf = {
    KyuubiConf().set(KyuubiConf.ENGINE_SHARE_LEVEL, "user")
  }

  test("ensure two connections share the same engine when engine pool size is 1.") {
    withSessionConf()(
      Map(HighAvailabilityConf.HA_ZK_ENGINE_POOL_SIZE.key -> "1",
        KyuubiConf.ENGINE_SHARE_LEVEL_SUB_DOMAIN.key -> "aaa"
      ))(Map.empty) {

      var r1: String = null
      var r2: String = null
      new Thread {
        override def run(): Unit = withJdbcStatement() { statement =>
          val res = statement.executeQuery("set spark.app.name")
          assert(res.next())
          r1 = res.getString("value")
        }
      }.start()

      new Thread {
        override def run(): Unit = withJdbcStatement() { statement =>
          val res = statement.executeQuery("set spark.app.name")
          assert(res.next())
          r2 = res.getString("value")
        }
      }.start()

      eventually(timeout(120.seconds), interval(100.milliseconds)) {
        assert(r1 != null && r2 != null)
      }

      assert(r1 === r2)
    }
  }

  test("ensure two connections don't share the same engine when engine pool is 2.") {
    withSessionConf()(
      Map(HighAvailabilityConf.HA_ZK_ENGINE_POOL_SIZE.key -> "2",
        KyuubiConf.ENGINE_SHARE_LEVEL_SUB_DOMAIN.key -> "bbb"
      ))(Map.empty) {

      var r1: String = null
      var r2: String = null
      new Thread {
        override def run(): Unit = withJdbcStatement() { statement =>
          val res = statement.executeQuery("set spark.app.name")
          assert(res.next())
          r1 = res.getString("value")
        }
      }.start()

      new Thread {
        override def run(): Unit = withJdbcStatement() { statement =>
          val res = statement.executeQuery("set spark.app.name")
          assert(res.next())
          r2 = res.getString("value")
        }
      }.start()

      eventually(timeout(120.seconds), interval(100.milliseconds)) {
        assert(r1 != null && r2 != null)
      }

      assert(r1 != r2)
    }
  }

  test("ensure two of three connections share the same engine when engine pool size is 2.") {
    withSessionConf()(
      Map(HighAvailabilityConf.HA_ZK_ENGINE_POOL_SIZE.key -> "2",
        KyuubiConf.ENGINE_SHARE_LEVEL_SUB_DOMAIN.key -> "ccc"
      ))(Map.empty) {

      var r1: String = null
      var r2: String = null
      var r3: String = null
      new Thread {
        override def run(): Unit = withJdbcStatement() { statement =>
          val res = statement.executeQuery("set spark.app.name")
          assert(res.next())
          r1 = res.getString("value")
        }
      }.start()

      new Thread {
        override def run(): Unit = withJdbcStatement() { statement =>
          val res = statement.executeQuery("set spark.app.name")
          assert(res.next())
          r2 = res.getString("value")
        }
      }.start()

      new Thread {
        override def run(): Unit = withJdbcStatement() { statement =>
          val res = statement.executeQuery("set spark.app.name")
          assert(res.next())
          r3 = res.getString("value")
        }
      }.start()

      eventually(timeout(180.seconds), interval(100.milliseconds)) {
        assert(r1 != null && r2 != null && r3 != null)
      }

      assert(r1 != r2)
      assert(r3 === r1 || r3 === r2)
    }
  }

}
