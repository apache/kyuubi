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

package org.apache.kyuubi.ha.client.zookeeper

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.ha.HighAvailabilityConf.HA_ZK_CONN_BASE_RETRY_WAIT
import org.apache.kyuubi.ha.HighAvailabilityConf.HA_ZK_CONN_MAX_RETRIES
import org.apache.kyuubi.ha.HighAvailabilityConf.HA_ZK_CONN_MAX_RETRY_WAIT
import org.apache.kyuubi.ha.HighAvailabilityConf.HA_ZK_CONN_RETRY_POLICY

class ZookeeperClientProviderSuite extends KyuubiFunSuite {

  test("get graceful stop thread start delay") {
    val conf = KyuubiConf()

    val baseSleepTime = conf.get(HA_ZK_CONN_BASE_RETRY_WAIT)
    val maxSleepTime = conf.get(HA_ZK_CONN_MAX_RETRY_WAIT)
    val maxRetries = conf.get(HA_ZK_CONN_MAX_RETRIES)
    val delay1 = ZookeeperClientProvider.getGracefulStopThreadDelay(conf)
    assert(delay1 >= baseSleepTime * maxRetries)

    conf.set(HA_ZK_CONN_RETRY_POLICY, "ONE_TIME")
    val delay2 = ZookeeperClientProvider.getGracefulStopThreadDelay(conf)
    assert(delay2 === baseSleepTime)

    conf.set(HA_ZK_CONN_RETRY_POLICY, "N_TIME")
    val delay3 = ZookeeperClientProvider.getGracefulStopThreadDelay(conf)
    assert(delay3 === baseSleepTime * maxRetries)

    conf.set(HA_ZK_CONN_RETRY_POLICY, "UNTIL_ELAPSED")
    val delay4 = ZookeeperClientProvider.getGracefulStopThreadDelay(conf)
    assert(delay4 === maxSleepTime)

    conf.set(HA_ZK_CONN_RETRY_POLICY, "BOUNDED_EXPONENTIAL_BACKOFF")
    val delay5 = ZookeeperClientProvider.getGracefulStopThreadDelay(conf)
    assert(delay5 >= baseSleepTime * maxRetries)
  }
}
