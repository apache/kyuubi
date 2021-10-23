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

import com.unboundid.ldap.listener.{InMemoryDirectoryServer, InMemoryDirectoryServerConfig}

import org.apache.kyuubi.{KyuubiFunSuite, Utils}

trait WithLdapServer extends KyuubiFunSuite {
  protected var ldapServer: InMemoryDirectoryServer = _
  protected val ldapBaseDn = "ou=users"
  protected val ldapUser = Utils.currentUser
  protected val ldapUserPasswd = "password"

  protected def ldapUrl = s"ldap://localhost:${ldapServer.getListenPort}"

  override def beforeAll(): Unit = {
    val config = new InMemoryDirectoryServerConfig(ldapBaseDn)
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
