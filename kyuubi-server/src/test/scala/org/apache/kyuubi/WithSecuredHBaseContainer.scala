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

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import com.dimafeng.testcontainers.{ContainerDef, FixedHostPortGenericContainer}
import com.dimafeng.testcontainers.scalatest.TestContainerForAll
import com.github.dockerjava.api.model.{ExposedPort, Ports}
import org.apache.hadoop.conf.Configuration
import org.testcontainers.containers.wait.strategy.HostPortWaitStrategy

trait WithSecuredHBaseContainer extends KerberizedTestHelper with TestContainerForAll {

  final val HADOOP_SECURITY_AUTHENTICATION = "kerberos"
  final val HBASE_KERBEROS_REALM = "TEST.ORG"
  final val HBASE_KERBEROS_PRINCIPAL = "hbase/localhost"
  final val HBASE_KERBEROS_KEYTAB = "/opt/hbase/conf/hbase.keytab"
  final val DOCKER_IMAGE_NAME = "z1wu97/kyuubi-hbase-cluster:latest"

  private val tempDir = Utils.createTempDir(prefix = "kyuubi-server-hbase")
  private val exposedKdcPort = 88
  private val exposedZkPort = 2181
  private val exposedHbaseMasterPort = 16000
  private val exposedHbaseRegionServerPort = 16020
  private val testPrincipalOverride =
    HBASE_KERBEROS_PRINCIPAL + "@" + HBASE_KERBEROS_REALM
  private val krb5ConfPathOverride = new File(tempDir.toFile, "krb5.conf").getAbsolutePath
  val hbaseConf: Configuration = new Configuration(false)

  override val testKeytab: String = new File(tempDir.toFile, "hbase.service.keytab").getAbsolutePath
  override val containerDef: HBaseContainer.Def = HBaseContainer.Def(
    DOCKER_IMAGE_NAME,
    exposedKdcPort,
    exposedHbaseMasterPort,
    exposedHbaseRegionServerPort,
    exposedZkPort)

  override def beforeAll(): Unit = {
    super.beforeAll()
    testPrincipal = testPrincipalOverride
    krb5ConfPath = krb5ConfPathOverride
  }

  override def afterContainersStart(containers: Containers): Unit = {
    containers.copyFileFromContainer(HBASE_KERBEROS_KEYTAB, testKeytab)
    hbaseConf.set("hbase.zookeeper.quorum", "localhost:" + containers.mappedPort(exposedZkPort))
    hbaseConf.set("hbase.security.authentication", "kerberos")
    hbaseConf.set("hadoop.security.authentication", "kerberos")
    hbaseConf.set(
      "hbase.master.kerberos.principal",
      HBASE_KERBEROS_PRINCIPAL + "@" + HBASE_KERBEROS_REALM)
    hbaseConf.set(
      "hbase.regionserver.kerberos.principal",
      HBASE_KERBEROS_PRINCIPAL + "@" + HBASE_KERBEROS_REALM)

    val krb5ConfContent =
      s"""[libdefaults]
         |  default_realm = $HBASE_KERBEROS_REALM
         |  dns_lookup_realm = false
         |  dns_lookup_kdc = false
         |  forwardable = true
         |
         |[realms]
         |  ${HBASE_KERBEROS_REALM} = {
         |    kdc = localhost:${containers.getKdcUdpPort}
         |  }
         |
         |""".stripMargin

    val writer = Files.newBufferedWriter(Paths.get(krb5ConfPathOverride), StandardCharsets.UTF_8)
    writer.write(krb5ConfContent)
    writer.close()
  }
}

class HBaseContainer(exposedKdcPort: Int, dockerImage: String)
  extends FixedHostPortGenericContainer(dockerImage) {

  def getKdcUdpPort: Int = {
    container.getContainerInfo.getNetworkSettings.getPorts.getBindings
      .get(ExposedPort.udp(exposedKdcPort)).head.getHostPortSpec.toInt
  }
}

object HBaseContainer {
  case class Def(
      dockerImage: String,
      exposedKdcPort: Int,
      exposedHbaseMasterPort: Int,
      exposedHbaseRegionServerPort: Int,
      exposedZkPort: Int,
      env: Map[String, String] = Map())
    extends ContainerDef {

    override type Container = HBaseContainer

    override def createContainer(): Container = {
      val container = new HBaseContainer(
        exposedKdcPort,
        dockerImage)

      container.container.withExposedPorts(
        exposedKdcPort,
        exposedZkPort)
      container.container.withFixedExposedPort(
        exposedHbaseRegionServerPort,
        exposedHbaseRegionServerPort)
      container.container.withFixedExposedPort(exposedHbaseMasterPort, exposedHbaseMasterPort)
      container.container.setWaitStrategy(new HostPortWaitStrategy()
        .forPorts(exposedHbaseMasterPort, exposedHbaseRegionServerPort, exposedZkPort))

      container.container.withCreateContainerCmdModifier(cmd => {
        val udpExposedPort = ExposedPort.udp(exposedKdcPort)
        val exposedPorts = new java.util.LinkedList[ExposedPort]()
        for (p <- cmd.getExposedPorts) {
          exposedPorts.add(p)
        }
        exposedPorts.add(udpExposedPort)
        cmd.withExposedPorts(exposedPorts)

        // Add previous port bindings and UDP port binding
        val ports = cmd.getHostConfig.getPortBindings
        ports.bind(udpExposedPort, Ports.Binding.empty)
        cmd.getHostConfig.withPortBindings(ports)
      })
      container
    }
  }
}
