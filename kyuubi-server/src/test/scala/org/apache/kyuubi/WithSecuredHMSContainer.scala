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

import com.dimafeng.testcontainers.{ContainerDef, GenericContainer}
import com.dimafeng.testcontainers.scalatest.TestContainerForAll
import com.github.dockerjava.api.model.{ExposedPort, Ports}
import org.apache.hadoop.conf.Configuration
import org.testcontainers.containers.wait.strategy.HostPortWaitStrategy

import org.apache.kyuubi.shaded.hive.metastore.conf.MetastoreConf
import org.apache.kyuubi.shaded.hive.metastore.conf.MetastoreConf.ConfVars._

trait WithSecuredHMSContainer extends KerberizedTestHelper with TestContainerForAll {

  final val HADOOP_SECURITY_AUTHENTICATION = "kerberos"
  final val HIVE_METASTORE_KERBEROS_REALM = "TEST.ORG"
  final val HIVE_METASTORE_KERBEROS_PRINCIPAL = "hive/localhost"
  final val HIVE_METASTORE_KERBEROS_KEYTAB = "/hive.service.keytab"
  final val DOCKER_IMAGE_NAME = "nekyuubi/kyuubi-hive-metastore:latest"

  private val tempDir = Utils.createTempDir(prefix = "kyuubi-server-hms")
  private val exposedKdcPort = 88
  private val exposedHmsPort = 9083
  private val testPrincipalOverride =
    HIVE_METASTORE_KERBEROS_PRINCIPAL + "@" + HIVE_METASTORE_KERBEROS_REALM
  private val krb5ConfPathOverride = new File(tempDir.toFile, "krb5.conf").getAbsolutePath

  val hiveConf: Configuration = MetastoreConf.newMetastoreConf()

  override val testKeytab: String = new File(tempDir.toFile, "hive.service.keytab").getAbsolutePath
  override val containerDef: HMSContainer.Def = HMSContainer.Def(
    DOCKER_IMAGE_NAME,
    exposedKdcPort,
    exposedHmsPort,
    env = Map(
      "HADOOP_SECURITY_AUTHENTICATION" -> HADOOP_SECURITY_AUTHENTICATION,
      "HIVE_METASTORE_KERBEROS_PRINCIPAL" -> HIVE_METASTORE_KERBEROS_PRINCIPAL,
      "HIVE_METASTORE_KERBEROS_KEYTAB" -> HIVE_METASTORE_KERBEROS_KEYTAB))

  override def beforeAll(): Unit = {
    super.beforeAll()
    testPrincipal = testPrincipalOverride
    krb5ConfPath = krb5ConfPathOverride
  }

  override def afterContainersStart(containers: Containers): Unit = {
    containers.copyFileFromContainer(HIVE_METASTORE_KERBEROS_KEYTAB, testKeytab)

    MetastoreConf.setVar(
      hiveConf,
      THRIFT_URIS,
      "thrift://localhost:" + containers.mappedPort(exposedHmsPort))
    MetastoreConf.setBoolVar(hiveConf, USE_THRIFT_SASL, true)
    MetastoreConf.setVar(
      hiveConf,
      KERBEROS_PRINCIPAL,
      HIVE_METASTORE_KERBEROS_PRINCIPAL + "@" + HIVE_METASTORE_KERBEROS_REALM)
    hiveConf.set("hadoop.security.authentication", "kerberos")

    val krb5ConfContent =
      s"""[libdefaults]
         |  default_realm = $HIVE_METASTORE_KERBEROS_REALM
         |  dns_lookup_realm = false
         |  dns_lookup_kdc = false
         |  forwardable = true
         |
         |[realms]
         |  TEST.ORG = {
         |    kdc = localhost:${containers.getKdcUdpPort}
         |  }
         |
         |""".stripMargin

    val writer = Files.newBufferedWriter(Paths.get(krb5ConfPathOverride), StandardCharsets.UTF_8)
    writer.write(krb5ConfContent)
    writer.close()
  }
}

class HMSContainer(exposedKdcPort: Int, underlying: GenericContainer)
  extends GenericContainer(underlying) {

  def getKdcUdpPort: Int = {
    underlying.container.getContainerInfo.getNetworkSettings.getPorts.getBindings
      .get(ExposedPort.udp(exposedKdcPort)).head.getHostPortSpec.toInt
  }
}

object HMSContainer {
  case class Def(
      dockerImage: String,
      exposedKdcPort: Int,
      exposedHmsPort: Int,
      env: Map[String, String] = Map())
    extends ContainerDef {

    override type Container = HMSContainer

    override def createContainer(): Container = {
      val container = new HMSContainer(
        exposedKdcPort,
        GenericContainer(
          dockerImage,
          env = env,
          waitStrategy = new HostPortWaitStrategy().forPorts(exposedHmsPort)))

      container.container.withExposedPorts(exposedKdcPort, exposedHmsPort)
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
