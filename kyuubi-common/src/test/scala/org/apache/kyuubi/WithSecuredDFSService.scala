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
import org.apache.kyuubi.service.MiniDFSService

trait WithSecuredDFSService extends KerberizedTestHelper {

  private val miniDFSService = new MiniDFSService(newSecuredConf())

  private def newSecuredConf(): Configuration = {
    val hdfsConf = new Configuration()
    hdfsConf.set("ignore.secure.ports.for.testing", "true")
    hdfsConf.set("hadoop.security.authentication", "kerberos")
    hdfsConf.set("dfs.block.access.token.enable", "true")
    hdfsConf.set("dfs.namenode.keytab.file", testKeytab)
    hdfsConf.set("dfs.namenode.kerberos.principal", testPrincipal)
    hdfsConf.set("dfs.namenode.kerberos.internal.spnego.principal", testPrincipal)
    hdfsConf.set("dfs.web.authentication.kerberos.principal", testPrincipal)

    hdfsConf.set("dfs.datanode.address", "0.0.0.0:1025")
    hdfsConf.set("dfs.datanode.kerberos.principal", testPrincipal)
    hdfsConf.set("dfs.datanode.keytab.file", testKeytab)

    hdfsConf
  }

  override def beforeAll(): Unit = {
    tryWithSecurityEnabled {
      UserGroupInformation.loginUserFromKeytab(testPrincipal, testKeytab)
      miniDFSService.initialize(new KyuubiConf(false))
      miniDFSService.start()
    }
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    miniDFSService.stop()
    super.afterAll()
  }

  def getHadoopConf(): Configuration = miniDFSService.getHadoopConf
}
