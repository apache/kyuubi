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

package org.apache.kyuubi

import java.time.Duration

import com.dimafeng.testcontainers.{ContainerDef, GenericContainer}
import com.dimafeng.testcontainers.scalatest.TestContainerForAll
import org.testcontainers.containers.wait.strategy.HostPortWaitStrategy

trait WithSimpleHMSContainer extends KyuubiFunSuite with TestContainerForAll {

  final val DOCKER_IMAGE_NAME = SimpleHMSContainer.DOCKER_IMAGE_NAME

  private val exposedHmsPort = SimpleHMSContainer.EXPOSED_HMS_PORT

  private var _hmsThriftUris: String = _

  def hmsThriftUris: String = {
    require(_hmsThriftUris != null, "HMS container has not started yet")
    _hmsThriftUris
  }

  override val containerDef: SimpleHMSContainer.Def =
    SimpleHMSContainer.Def(DOCKER_IMAGE_NAME, exposedHmsPort)

  override def afterContainersStart(containers: Containers): Unit = {
    _hmsThriftUris = "thrift://localhost:" + containers.mappedPort(exposedHmsPort)
  }
}

object SimpleHMSContainer {
  final val DOCKER_IMAGE_NAME = "nekyuubi/kyuubi-hive-metastore:latest"
  final val EXPOSED_HMS_PORT = 9083
  final val STARTUP_TIMEOUT = Duration.ofSeconds(60)

  def newBaseContainer(
      dockerImage: String,
      exposedHmsPort: Int,
      exposedPorts: Seq[Int] = Seq.empty,
      env: Map[String, String] = Map.empty): GenericContainer =
    GenericContainer(
      dockerImage,
      exposedPorts = exposedPorts,
      env = env,
      waitStrategy = new HostPortWaitStrategy()
        .forPorts(exposedHmsPort)
        .withStartupTimeout(STARTUP_TIMEOUT))

  case class Def(
      dockerImage: String,
      exposedHmsPort: Int)
    extends ContainerDef {

    override type Container = GenericContainer

    override def createContainer(): Container =
      new GenericContainer(
        newBaseContainer(dockerImage, exposedHmsPort, exposedPorts = Seq(exposedHmsPort)))
  }
}
