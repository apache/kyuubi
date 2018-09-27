/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package yaooqinn.kyuubi.ha

import com.google.common.io.Files
import org.apache.curator.test.TestingServer
import org.apache.spark.{KyuubiConf, KyuubiSparkUtil, SparkConf, SparkFunSuite}
import org.apache.spark.KyuubiConf._

trait ZookeeperFunSuite extends SparkFunSuite{

  var zkServer: TestingServer = _
  var connectString: String = _
  val conf = new SparkConf(loadDefaults = true)
  KyuubiSparkUtil.setupCommonConfig(conf)
  conf.set(KyuubiConf.FRONTEND_BIND_PORT.key, "0")

  override def beforeAll(): Unit = {
    zkServer = new TestingServer(2181, Files.createTempDir(), true)
    connectString = zkServer.getConnectString
    conf.set(HA_ZOOKEEPER_QUORUM.key, connectString)
    conf.set(HA_ZOOKEEPER_CONNECTION_BASESLEEPTIME.key, "100ms")
    conf.set(HA_ZOOKEEPER_SESSION_TIMEOUT.key, "15s")
    conf.set(HA_ZOOKEEPER_CONNECTION_MAX_RETRIES.key, "0")
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    zkServer.stop()
    System.clearProperty(HA_ZOOKEEPER_QUORUM.key)
    System.clearProperty(HA_ENABLED.key)
    super.afterAll()
  }
}
