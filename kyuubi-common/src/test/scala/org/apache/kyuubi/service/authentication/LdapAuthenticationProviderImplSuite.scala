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

import java.io.File
import javax.naming.{CommunicationException, Context}
import javax.naming.directory.{BasicAttribute, BasicAttributes, InitialDirContext}
import javax.security.sasl.AuthenticationException

import scala.collection.mutable.ArrayBuffer

import org.apache.directory.api.ldap.model.name.Dn
import org.apache.directory.api.ldap.schemaextractor.impl.DefaultSchemaLdifExtractor
import org.apache.directory.api.ldap.schemaloader.LdifSchemaLoader
import org.apache.directory.api.ldap.schemamanager.impl.DefaultSchemaManager
import org.apache.directory.server.constants.ServerDNConstants
import org.apache.directory.server.core.DefaultDirectoryService
import org.apache.directory.server.core.api.{CacheService, InstanceLayout}
import org.apache.directory.server.core.api.partition.Partition
import org.apache.directory.server.core.api.schema.SchemaPartition
import org.apache.directory.server.core.partition.impl.btree.jdbm.JdbmPartition
import org.apache.directory.server.core.partition.ldif.LdifPartition
import org.apache.directory.server.ldap.LdapServer
import org.apache.directory.server.protocol.shared.transport.TcpTransport
import org.apache.mina.util.AvailablePortFinder

import org.apache.kyuubi.{KyuubiFunSuite, Utils}
import org.apache.kyuubi.config.KyuubiConf

class LdapAuthenticationProviderImplSuite extends KyuubiFunSuite {

  private val servicePort = AvailablePortFinder.getNextAvailable(9000)
  private val service = new DefaultDirectoryService
  private val workingDIr = Utils.createTempDir().toFile
  private val ldapServer: LdapServer = new LdapServer()

  private val partitions = ArrayBuffer[Partition]()

  private val conf = new KyuubiConf()

  override def beforeAll(): Unit = {
    service.setInstanceLayout(new InstanceLayout(workingDIr))
    val cachedService = new CacheService()
    cachedService.initialize(service.getInstanceLayout)
    service.setCacheService(cachedService)

    val schemaPartitionDir = new File(service.getInstanceLayout.getPartitionsDirectory, "schema")
    if (!schemaPartitionDir.exists()) {
      val extractor = new DefaultSchemaLdifExtractor(
        service.getInstanceLayout.getPartitionsDirectory)
      extractor.extractOrCopy()
    }

    val loader = new LdifSchemaLoader(schemaPartitionDir)
    val schemaManager = new DefaultSchemaManager(loader)
    schemaManager.loadAllEnabled()
    service.setSchemaManager(schemaManager)

    val schemaLDifPartition = new LdifPartition(schemaManager)
    schemaLDifPartition.setPartitionPath(schemaPartitionDir.toURI)
    val schemaPartition = new SchemaPartition(schemaManager)
    schemaPartition.setWrappedPartition(schemaLDifPartition)
    service.setSchemaPartition(schemaPartition)

    val jdbmPartition = new JdbmPartition(service.getSchemaManager)
    jdbmPartition.setId("kyuubi")
    jdbmPartition.setPartitionPath(
      new File(service.getInstanceLayout.getPartitionsDirectory, jdbmPartition.getId).toURI)
    jdbmPartition.setSuffixDn(new Dn(ServerDNConstants.SYSTEM_DN))
    jdbmPartition.setSchemaManager(service.getSchemaManager)
    service.setSystemPartition(jdbmPartition)

    service.getChangeLog.setEnabled(false)

    service.setDenormalizeOpAttrsEnabled(true)

    service.startup()
    ldapServer.setDirectoryService(service)
    ldapServer.setTransports(new TcpTransport(servicePort))
    ldapServer.start()

    conf.set(AUTHENTICATION_LDAP_URL, "ldap://localhost:" + ldapServer.getPort)

    addPartition("kentyao", "uid=kentyao,ou=users")
    addUser()

    super.beforeAll()
  }

  private def addUser(): Unit = {
    val env = new java.util.Hashtable[String, Any]()
    env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory")
    env.put(Context.SECURITY_AUTHENTICATION, "simple")
    env.put(Context.PROVIDER_URL, conf.get(AUTHENTICATION_LDAP_URL).get)
    env.put(Context.SECURITY_PRINCIPAL, "uid=admin,ou=system")
    env.put(Context.SECURITY_CREDENTIALS, "secret")

    val container = new BasicAttributes()
    val objClasses = new BasicAttribute("objectClass")
    objClasses.add("inetOrgPerson")
    val cn = new BasicAttribute("cn", "kentyao")
    val o = new BasicAttribute("o", "kyuubi")
    val sn = new BasicAttribute("sn", "yao")
    val uid = new BasicAttribute("uid", "kentyao")
    val password = new BasicAttribute("userpassword", "kentyao")
    container.put(objClasses)
    container.put(cn)
    container.put(uid)
    container.put(o)
    container.put(sn)
    container.put(password)
    val context = new InitialDirContext(env)
    context.createSubcontext("uid=kentyao,ou=users", container)
  }

  private def addPartition(id: String, dn: String): Partition = {
    val partition = new JdbmPartition(service.getSchemaManager)
    partition.setId(id)
    partition.setPartitionPath(
      new File(service.getInstanceLayout.getPartitionsDirectory, partition.getId).toURI)
    partition.setSuffixDn(new Dn(dn))
    service.addPartition(partition)
    partitions += partition
    partition
  }

  override def afterAll(): Unit = {
    if (ldapServer.isStarted) {
      ldapServer.stop()
    }
    super.afterAll()
  }

  test("ldap server is started") {
    assert(ldapServer.isStarted)
    assert(ldapServer.getPort > 0)
  }

  test("authenticate tests") {
    val providerImpl = new LdapAuthenticationProviderImpl(conf)
    val e1 = intercept[AuthenticationException](providerImpl.authenticate("", ""))
    assert(e1.getMessage.contains("user is null"))
    val e2 = intercept[AuthenticationException](providerImpl.authenticate("kyuubi", ""))
    assert(e2.getMessage.contains("password is null"))

    val user = "uid=kentyao,ou=users"
    providerImpl.authenticate(user, "kentyao")
    val e3 = intercept[AuthenticationException](
      providerImpl.authenticate(user, "kent"))
    assert(e3.getMessage.contains(user))
    assert(e3.getCause.isInstanceOf[javax.naming.AuthenticationException])

    val dn = "ou=users"
    conf.set(AUTHENTICATION_LDAP_BASEDN, dn)
    val providerImpl2 = new LdapAuthenticationProviderImpl(conf)
    providerImpl2.authenticate("kentyao", "kentyao")

    val e4 = intercept[AuthenticationException](
      providerImpl.authenticate("kentyao", "kent"))
    assert(e4.getMessage.contains(user))

    conf.unset(AUTHENTICATION_LDAP_URL)
    val providerImpl3 = new LdapAuthenticationProviderImpl(conf)
    val e5 = intercept[AuthenticationException](
      providerImpl3.authenticate("kentyao", "kentyao"))

    assert(e5.getMessage.contains(user))
    assert(e5.getCause.isInstanceOf[CommunicationException])

    conf.set(AUTHENTICATION_LDAP_DOMAIN, "kyuubi.com")
    val providerImpl4 = new LdapAuthenticationProviderImpl(conf)
    intercept[AuthenticationException](providerImpl4.authenticate("kentyao", "kentyao"))
  }
}
