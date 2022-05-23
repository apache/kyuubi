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

package org.apache.kyuubi.operation

import java.util.Base64
import javax.servlet.http.HttpServletResponse
import javax.ws.rs.client.Entity
import javax.ws.rs.core.MediaType

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hive.service.rpc.thrift.TProtocolVersion

import org.apache.kyuubi.{KerberizedTestHelper, RestFrontendTestHelper}
import org.apache.kyuubi.client.api.v1.dto.{SessionHandle, SessionOpenCount, SessionOpenRequest}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.server.http.authentication.AuthenticationHandler.AUTHORIZATION_HEADER
import org.apache.kyuubi.server.http.authentication.AuthSchemes
import org.apache.kyuubi.service.authentication._

class KyuubiRestAuthenticationSuite extends RestFrontendTestHelper with KerberizedTestHelper
  with WithLdapServer {

  private val customUser: String = "user"
  private val customPasswd: String = "password"
  private val currentUser = UserGroupInformation.getCurrentUser

  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    System.clearProperty("java.security.krb5.conf")
    UserGroupInformation.setLoginUser(currentUser)
    UserGroupInformation.setConfiguration(new Configuration())
    assert(!UserGroupInformation.isSecurityEnabled)
    super.afterAll()
  }

  override protected lazy val conf: KyuubiConf = {
    val config = new Configuration()
    val authType = "hadoop.security.authentication"
    config.set(authType, "KERBEROS")
    System.setProperty("java.security.krb5.conf", krb5ConfPath)
    UserGroupInformation.setConfiguration(config)
    assert(UserGroupInformation.isSecurityEnabled)

    KyuubiConf().set(KyuubiConf.AUTHENTICATION_METHOD, Seq("KERBEROS", "LDAP", "CUSTOM"))
      .set(KyuubiConf.SERVER_KEYTAB.key, testKeytab)
      .set(KyuubiConf.SERVER_PRINCIPAL, testPrincipal)
      .set(KyuubiConf.SERVER_SPNEGO_KEYTAB, testKeytab)
      .set(KyuubiConf.SERVER_SPNEGO_PRINCIPAL, testSpnegoPrincipal)
      .set(KyuubiConf.AUTHENTICATION_LDAP_URL, ldapUrl)
      .set(KyuubiConf.AUTHENTICATION_LDAP_BASEDN, ldapBaseDn)
      .set(
        KyuubiConf.AUTHENTICATION_CUSTOM_CLASS,
        classOf[UserDefineAuthenticationProviderImpl].getCanonicalName)
      .set(KyuubiConf.ENGINE_SECURITY_ENABLED, true)
      .set(
        KyuubiConf.ENGINE_SECURITY_SECRET_PROVIDER,
        classOf[UserDefinedEngineSecuritySecretProvider].getCanonicalName)
  }

  test("test with LDAP authorization") {
    val encodeAuthorization = new String(
      Base64.getEncoder.encode(
        s"$ldapUser:$ldapUserPasswd".getBytes()),
      "UTF-8")
    val response = webTarget.path("api/v1/sessions/count")
      .request()
      .header(AUTHORIZATION_HEADER, s"BASIC $encodeAuthorization")
      .get()

    assert(HttpServletResponse.SC_OK == response.getStatus)
    val openedSessionCount = response.readEntity(classOf[SessionOpenCount])
    assert(openedSessionCount.getOpenSessionCount == 0)
  }

  test("test with CUSTOM authorization") {
    val encodeAuthorization = new String(
      Base64.getEncoder.encode(
        s"$customUser:$customPasswd".getBytes()),
      "UTF-8")
    val response = webTarget.path("api/v1/sessions/count")
      .request()
      .header(AUTHORIZATION_HEADER, s"BASIC $encodeAuthorization")
      .get()

    assert(HttpServletResponse.SC_INTERNAL_SERVER_ERROR == response.getStatus)
  }

  test("test without authorization") {
    val response = webTarget.path("api/v1/sessions/count")
      .request()
      .get()

    assert(HttpServletResponse.SC_UNAUTHORIZED == response.getStatus)
  }

  test("test with valid spnego authentication") {
    UserGroupInformation.loginUserFromKeytab(testPrincipal, testKeytab)
    val token = generateToken(hostName)
    val response = webTarget.path("api/v1/sessions/count")
      .request()
      .header(AUTHORIZATION_HEADER, s"NEGOTIATE $token")
      .get()

    assert(HttpServletResponse.SC_OK == response.getStatus)
  }

  test("test with invalid spnego authorization") {
    val encodeAuthorization = new String(
      Base64.getEncoder.encode(
        s"invalidKerberosToken".getBytes()),
      "UTF-8")
    val response = webTarget.path("api/v1/sessions/count")
      .request()
      .header(AUTHORIZATION_HEADER, s"NEGOTIATE $encodeAuthorization")
      .get()

    assert(HttpServletResponse.SC_INTERNAL_SERVER_ERROR == response.getStatus)
  }

  test("test with not supported auth scheme") {
    val encodeAuthorization = new String(
      Base64.getEncoder.encode(
        s"$ldapUser:$ldapUserPasswd".getBytes()),
      "UTF-8")
    val response = webTarget.path("api/v1/sessions/count")
      .request()
      .header(AUTHORIZATION_HEADER, s"OTHER_SCHEME $encodeAuthorization")
      .get()

    assert(HttpServletResponse.SC_UNAUTHORIZED == response.getStatus)
  }

  test("test with non-authentication path") {
    val response = webTarget.path("swagger").request().get()
    assert(HttpServletResponse.SC_OK == response.getStatus)

    val openApiResponse = webTarget.path("api/openapi.json").request().get()
    assert(HttpServletResponse.SC_OK == openApiResponse.getStatus)
  }

  test("test with ugi wrapped open session") {
    UserGroupInformation.loginUserFromKeytab(testPrincipal, testKeytab)
    var token = generateToken(hostName)
    val sessionOpenRequest = new SessionOpenRequest(
      TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V11.getValue,
      "kyuubi",
      "pass",
      "localhost",
      Map(KyuubiConf.ENGINE_SHARE_LEVEL.key -> "CONNECTION").asJava)

    var response = webTarget.path("api/v1/sessions")
      .request()
      .header(AUTHORIZATION_HEADER, s"NEGOTIATE $token")
      .post(Entity.entity(sessionOpenRequest, MediaType.APPLICATION_JSON_TYPE))

    assert(HttpServletResponse.SC_OK == response.getStatus)
    val sessionHandle = response.readEntity(classOf[SessionHandle])

    token = generateToken(hostName)
    val serializedSessionHandle = s"${sessionHandle.getPublicId}|" +
      s"${sessionHandle.getSecretId}|${sessionHandle.getProtocolVersion}"

    response = webTarget.path(s"api/v1/sessions/$serializedSessionHandle")
      .request()
      .header(AUTHORIZATION_HEADER, s"NEGOTIATE $token")
      .delete()
    assert(HttpServletResponse.SC_OK == response.getStatus)
  }

  test("test with internal authorization") {
    val internalSecurityAccessor = InternalSecurityAccessor.get()
    var encodeAuthorization = new String(
      Base64.getEncoder.encode(
        s"$ldapUser:${internalSecurityAccessor.issueToken()}".getBytes()),
      "UTF-8")
    var response = webTarget.path("api/v1/sessions/count")
      .request()
      .header(AUTHORIZATION_HEADER, s"${AuthSchemes.KYUUBI_INTERNAL.toString} $encodeAuthorization")
      .get()

    assert(HttpServletResponse.SC_OK == response.getStatus)
    val openedSessionCount = response.readEntity(classOf[SessionOpenCount])
    assert(openedSessionCount.getOpenSessionCount == 0)

    val badAuthorization = new String(
      Base64.getEncoder.encode(
        s"$ldapUser:".getBytes()),
      "UTF-8")
    response = webTarget.path("api/v1/sessions/count")
      .request()
      .header(AUTHORIZATION_HEADER, s"${AuthSchemes.KYUUBI_INTERNAL.toString} $badAuthorization")
      .get()

    assert(HttpServletResponse.SC_UNAUTHORIZED == response.getStatus)
  }
}
