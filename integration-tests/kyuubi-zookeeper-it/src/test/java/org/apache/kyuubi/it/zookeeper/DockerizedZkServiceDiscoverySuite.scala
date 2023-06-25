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

package org.apache.kyuubi.it.zookeeper

import com.dimafeng.testcontainers.{GenericContainer, SingleContainer}
import org.testcontainers.containers.wait.strategy.Wait

import org.apache.kyuubi.Utils
import org.apache.kyuubi.ha.client.zookeeper.ZookeeperDiscoveryClientSuite

class DockerizedZkServiceDiscoverySuite extends ZookeeperDiscoveryClientSuite {

  private val zkClientPort = 2181
  private val zkVersion = sys.env.getOrElse("KYUUBI_IT_ZOOKEEPER_VERSION", "3.4")
  private val zkImage = sys.env.getOrElse("KYUUBI_IT_ZOOKEEPER_IMAGE", s"zookeeper:$zkVersion")

  val container: SingleContainer[_] = GenericContainer(
    dockerImage = zkImage,
    exposedPorts = Seq(zkClientPort),
    waitStrategy = Wait.forListeningPort)

  override def getConnectString: String = s"${container.host}:${container.mappedPort(zkClientPort)}"

  override def startZk(): Unit = synchronized {
    container.start()
  }

  override def stopZk(): Unit = synchronized {
    Utils.tryLogNonFatalError { container.stop() }
  }
}
