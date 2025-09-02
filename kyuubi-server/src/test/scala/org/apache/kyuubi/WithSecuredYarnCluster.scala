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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.UserGroupInformation

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.server.MiniYarnService

trait WithSecuredYarnCluster extends KerberizedTestHelper {

  private var miniYarnService: MiniYarnService = _

  private def newSecuredConf(): Configuration = {
    val hdfsConf = new Configuration()
    hdfsConf.set("ignore.secure.ports.for.testing", "true")
    hdfsConf.set("hadoop.security.authentication", "kerberos")
    hdfsConf.set("yarn.resourcemanager.keytab", testKeytab)
    hdfsConf.set("yarn.resourcemanager.principal", testPrincipal)

    hdfsConf.set("yarn.nodemanager.keytab", testPrincipal)
    hdfsConf.set("yarn.nodemanager.principal", testKeytab)

    hdfsConf
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    tryWithSecurityEnabled {
      UserGroupInformation.loginUserFromKeytab(testPrincipal, testKeytab)
      miniYarnService = new MiniYarnService(newSecuredConf())
      miniYarnService.initialize(new KyuubiConf(false))
      miniYarnService.start()
    }
  }

  override def afterAll(): Unit = {
    miniYarnService.stop()
    super.afterAll()
  }

  def getHadoopConf: Configuration = miniYarnService.getYarnConf
  def getHadoopConfDir: String = miniYarnService.getYarnConfDir
}
