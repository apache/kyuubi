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

package org.apache.kyuubi.service.authentication

import scala.util.Random

import com.unboundid.ldap.listener.{InMemoryDirectoryServer, InMemoryDirectoryServerConfig}
import com.unboundid.ldif.LDIFReader

import org.apache.kyuubi.{KyuubiFunSuite, Utils}

trait WithLdapServer extends KyuubiFunSuite {
  protected var ldapServer: InMemoryDirectoryServer = _
  protected val ldapBaseDn: Array[String] = Array("ou=users")
  protected val ldapUser: String = Utils.currentUser
  protected val ldapUserPasswd: String = Random.alphanumeric.take(16).mkString

  protected def ldapUrl = s"ldap://localhost:${ldapServer.getListenPort}"

  /**
   * Apply LDIF files
   * @param resource the LDIF file under classpath
   */
  def applyLDIF(resource: String): Unit = {
    ldapServer.applyChangesFromLDIF(
      new LDIFReader(Utils.getContextOrKyuubiClassLoader.getResource(resource).openStream()))
  }

  override def beforeAll(): Unit = {
    val config = new InMemoryDirectoryServerConfig(ldapBaseDn: _*)
    // disable the schema so that we can apply LDIF which contains Microsoft's Active Directory
    // specific definitions.
    // https://myshittycode.com/2017/03/28/
    config.setSchema(null)
    config.addAdditionalBindCredentials(s"uid=$ldapUser,ou=users", ldapUserPasswd)
    ldapServer = new InMemoryDirectoryServer(config)
    ldapServer.startListening()
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    ldapServer.close()
    super.afterAll()
  }
}
