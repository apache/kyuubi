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

package org.apache.kyuubi.ha.client.etcd

import java.nio.charset.StandardCharsets

import scala.collection.JavaConverters._

import io.etcd.jetcd.launcher.{Etcd, EtcdCluster}

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.ha.HighAvailabilityConf.{HA_ADDRESSES, HA_CLIENT_CLASS}
import org.apache.kyuubi.ha.client.DiscoveryClientProvider.withDiscoveryClient
import org.apache.kyuubi.ha.client.DiscoveryClientTests
import org.apache.kyuubi.service.NoopTBinaryFrontendServer

class EtcdDiscoveryClientSuite extends DiscoveryClientTests {

  private var etcdCluster: EtcdCluster = _
  var engineServer: NoopTBinaryFrontendServer = _

  private lazy val _connectString = etcdCluster.clientEndpoints.asScala.mkString(",")

  override def getConnectString: String = _connectString

  var conf: KyuubiConf = KyuubiConf()
    .set(HA_CLIENT_CLASS, classOf[EtcdDiscoveryClient].getName)

  override def beforeAll(): Unit = {
    etcdCluster = new Etcd.Builder()
      .withNodes(2)
      .build()
    etcdCluster.start()
    conf = new KyuubiConf()
      .set(HA_CLIENT_CLASS, classOf[EtcdDiscoveryClient].getName)
      .set(HA_ADDRESSES, getConnectString)
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    if (etcdCluster != null) {
      etcdCluster.close()
      etcdCluster = null
    }
  }

  test("etcd test: set, get and delete") {
    withDiscoveryClient(conf) { discoveryClient =>
      val path = "/kyuubi"
      // set
      discoveryClient.create(path, "PERSISTENT")
      assert(discoveryClient.pathExists(path))

      // get
      assert(new String(discoveryClient.getData(path), StandardCharsets.UTF_8) == path)

      // delete
      discoveryClient.delete(path)
      assert(!discoveryClient.pathExists(path))
    }
  }
}
