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

import java.util.UUID
import java.util.concurrent.Executors

import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime

import org.apache.kyuubi.operation.HiveJDBCTestHelper
import org.apache.kyuubi.service.ServiceState

class ExposeSparkEngineSessionsSuite
  extends WithDiscoverySparkSQLEngine with HiveJDBCTestHelper with WithEmbeddedZookeeper {

  override protected def jdbcUrl: String = getJdbcUrl

  override val namespace: String = {
    s"/kyuubi/tom/${UUID.randomUUID().toString}"
  }

  override def withKyuubiConf: Map[String, String] = {
    super.withKyuubiConf ++ zookeeperConf
  }

  test("engine session info") {
    withDiscoveryClient { discoveryClient =>
      assert(engine.getServiceState == ServiceState.STARTED)
      assert(discoveryClient.pathExists(namespace))

      val sessionsPath = namespace.split("/", 3).drop(1).mkString("/", "/sessions/", "")
      assert(discoveryClient.getChildrenCount(sessionsPath) == 0)

      val threads = Executors.newFixedThreadPool(3)
      threads.execute(() => {
        withJdbcStatement() { statement =>
          statement.execute("SELECT java_method('java.lang.Thread', 'sleep', 5000l)")
        }
      })
      eventually(Timeout(2.seconds)) {
        assert(discoveryClient.getChildrenCount(sessionsPath) == 1)
      }

      threads.execute(() => {
        withJdbcStatement() { statement =>
          statement.execute("SELECT java_method('java.lang.Thread', 'sleep', 5000l)")
        }
      })
      threads.execute(() => {
        withJdbcStatement() { statement =>
          statement.execute("SELECT java_method('java.lang.Thread', 'sleep', 5000l)")
        }
      })

      eventually(Timeout(2.seconds)) {
        assert(discoveryClient.getChildrenCount(sessionsPath) == 3)
      }

      threads.shutdown()
      eventually(Timeout(20.seconds)) {
        assert(discoveryClient.getChildrenCount(sessionsPath) == 0)
      }
    }
  }
}
