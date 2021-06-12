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

import org.apache.kyuubi.config.KyuubiConf.ENGINE_SHARE_LEVEL
import org.apache.kyuubi.engine.ShareLevel
import org.apache.kyuubi.engine.ShareLevel.ShareLevel
import org.apache.kyuubi.operation.JDBCTestUtils
import org.apache.kyuubi.service.ServiceState

/**
 * This suite is to test some behavior with spark engine in different share level.
 * e.g. cleanup discovery service before stop.
 */
abstract class ShareLevelSparkEngineSuite
  extends WithDiscoverySparkSQLEngine with JDBCTestUtils {
  def shareLevel: ShareLevel
  override def withKyuubiConf: Map[String, String] = {
    super.withKyuubiConf ++ Map(ENGINE_SHARE_LEVEL.key -> shareLevel.toString)
  }
  override protected def jdbcUrl: String = getJdbcUrl
  override val namespace: String = {
    // for test, we always use uuid as namespace
    s"/kyuubi/${shareLevel.toString}/${UUID.randomUUID().toString}"
  }

  test("check discovery service is clean up with different share level") {
    withZkClient { zkClient =>
      assert(engine.getServiceState == ServiceState.STARTED)
      assert(zkClient.checkExists().forPath(namespace) != null)
      withJdbcStatement() {_ => }
      shareLevel match {
        // Connection level, we will cleanup namespace since it's always a global unique value.
        case ShareLevel.CONNECTION =>
          assert(engine.getServiceState == ServiceState.STOPPED)
          assert(zkClient.checkExists().forPath(namespace) == null)
        case _ =>
          assert(engine.getServiceState == ServiceState.STARTED)
          assert(zkClient.checkExists().forPath(namespace) != null)
      }
    }
  }
}

class ConnectionLevelSparkEngineSuite extends ShareLevelSparkEngineSuite {
  override def shareLevel: ShareLevel = ShareLevel.CONNECTION
}

class UserLevelSparkEngineSuite extends ShareLevelSparkEngineSuite {
  override def shareLevel: ShareLevel = ShareLevel.USER
}
