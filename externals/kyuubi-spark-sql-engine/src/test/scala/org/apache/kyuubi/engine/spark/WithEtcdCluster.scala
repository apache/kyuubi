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

import scala.collection.JavaConverters._

import io.etcd.jetcd.launcher.Etcd
import io.etcd.jetcd.launcher.EtcdCluster

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.ha.HighAvailabilityConf.HA_ADDRESSES
import org.apache.kyuubi.ha.HighAvailabilityConf.HA_CLIENT_CLASS
import org.apache.kyuubi.ha.client.etcd.EtcdDiscoveryClient

trait WithEtcdCluster extends KyuubiFunSuite {

  private var etcdCluster: EtcdCluster = _

  lazy val etcdConf: Map[String, String] = {
    Map(
      HA_ADDRESSES.key -> etcdCluster.clientEndpoints().asScala.mkString(","),
      HA_CLIENT_CLASS.key -> classOf[EtcdDiscoveryClient].getName)
  }

  override def beforeAll(): Unit = {
    etcdCluster = new Etcd.Builder()
      .withNodes(1)
      .withMountedDataDirectory(false)
      .build()
    etcdCluster.start()
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    if (etcdCluster != null) {
      etcdCluster.close()
    }
    super.afterAll()
  }
}
