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
  protected val ldapBaseDn = "dc=example,dc=com"
  protected val ldapUser = Utils.currentUser
  protected val ldapUserPasswd = "ldapPassword"
  protected val ldapGuidKey = "uid"
  protected val ldapBinddn = "uid=admin,cn=Directory Manager,ou=users,dc=example,dc=com"
  protected val ldapBindpw = "adminPassword"
  protected val ldapBaseUserDn = "ou=users,dc=example,dc=com"
  protected val ldapDomain = "example"
  protected val ldapAttrs = Seq("mail")

  protected def ldapUrl = s"ldap://localhost:${ldapServer.getListenPort}"

  override def beforeAll(): Unit = {
    val config = new InMemoryDirectoryServerConfig(ldapBaseDn)
    config.addAdditionalBindCredentials(ldapBinddn, ldapBindpw)
    ldapServer = new InMemoryDirectoryServer(config)
    ldapServer.startListening()
    addLdapUser(ldapServer, ldapBaseDn, ldapBaseUserDn, ldapDomain, ldapUser, ldapUserPasswd)
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    ldapServer.close()
    super.afterAll()
  }

  def addLdapUser(
      ldapServer: InMemoryDirectoryServer,
      ldapBaseDn: String,
      ldapBaseUserDn: String,
      ldapDomain: String,
      ldapUser: String,
      ldapUserPasswd: String): Unit = {
    ldapServer.add(
      s"dn: $ldapBaseDn",
      "objectClass: domain",
      "objectClass: top",
      "dc: example")
    ldapServer.add(
      s"dn: $ldapBaseUserDn",
      "objectClass: top",
      "objectClass: organizationalUnit",
      "ou: users")
    ldapServer.add(
      s"dn: cn=$ldapUser,$ldapBaseUserDn",
      s"cn: $ldapUser",
      s"sn: $ldapUser",
      s"userPassword: $ldapUserPasswd",
      "objectClass: person")
    ldapServer.add(
      s"dn: uid=$ldapUser,cn=$ldapUser,$ldapBaseUserDn",
      s"uid: $ldapUser",
      s"mail: $ldapUser@$ldapDomain",
      s"cn: $ldapUser",
      s"sn: $ldapUser",
      s"userPassword: $ldapUserPasswd",
      "objectClass: person",
      "objectClass: inetOrgPerson")
  }
}
