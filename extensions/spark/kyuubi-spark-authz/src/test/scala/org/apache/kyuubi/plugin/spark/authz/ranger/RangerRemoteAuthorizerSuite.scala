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

package org.apache.kyuubi.plugin.spark.authz.ranger

import java.util.Properties

import scala.collection.JavaConverters._

import org.apache.hadoop.security.UserGroupInformation
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.funsuite.AnyFunSuite

import org.apache.kyuubi.plugin.spark.authz.{AccessControlException, OperationType}
import org.apache.kyuubi.plugin.spark.authz.ObjectType._
import org.apache.kyuubi.plugin.spark.authz.RangerTestUsers._
import org.apache.kyuubi.plugin.spark.authz.ranger.AccessType._

/**
 * The tests exercise the Ranger PDP remote authorizer (the default), which sends
 * authorization requests to a Ranger PDP server via REST APIs.
 *
 * By default, the requests go to [[MockPdpServer]] backed by the same policies used
 * by the other suites. To validate against a real Ranger PDP server, start one
 * configured with the same service (hive_jenkins) and policies, then run this suite
 * with the Ranger PDP server url and the Ranger PDP client settings, e.g. the
 * authentication settings, specified as system properties, e.g.
 * {{{
 * mvn test -pl extensions/spark/kyuubi-spark-authz \
 *   -DforkMode=never \
 *   -DwildcardSuites=org.apache.kyuubi.plugin.spark.authz.ranger.RangerRemoteAuthorizerSuite \
 *   -Dranger.authz.remote.pdp.url=https://ranger-pdp.org:8585 \
 *   -Dranger.authz.remote.authn.type=header \
 *   -Dranger.authz.remote.authn.header.X-Forwarded-User=ranger
 * }}}
 *
 * The plugin reads the security-critical Ranger configurations from the configuration
 * resources only, so the suite passes them to the plugin explicitly; the system
 * properties above are consumed by this suite.
 *
 * Note that `-DforkMode=never` is required to run the tests in the maven JVM so
 * that the system properties reach the suite.
 */
class RangerRemoteAuthorizerSuite extends AnyFunSuite with BeforeAndAfterAll
  with BeforeAndAfterEach {

  private val RemoteAuthorizer = "org.apache.ranger.authz.remote.RangerRemoteAuthorizer"

  // when a real Ranger PDP server is given via the system property,
  // the mock server is not started
  private lazy val mockPdp: Option[MockPdpServer] =
    if (System.getProperty("ranger.authz.remote.pdp.url") == null) {
      Some(new MockPdpServer)
    } else {
      None
    }

  private def pdpUrl: String =
    System.getProperty("ranger.authz.remote.pdp.url", mockPdp.map(_.url).orNull)

  private def ugiOf(user: String): UserGroupInformation =
    UserGroupInformation.createRemoteUser(user)

  private def tableReq(user: String, db: String, table: String, accessType: AccessType) =
    AccessRequest(
      AccessResource(TABLE, db, table, null),
      ugiOf(user),
      OperationType.QUERY,
      accessType)

  private def initializeRemoteAuthorizer(): Unit = {
    // ensures the Ranger configuration resources are loaded
    SparkRangerAdminPlugin.initialize()
    val properties = new Properties
    // the Ranger PDP client settings, e.g. the authentication settings,
    // can be specified as system properties of the test JVM
    System.getProperties.asScala
      .filter { case (key, _) => key.startsWith("ranger.authz.remote.") }
      .foreach { case (key, value) => properties.put(key, value) }
    properties.setProperty("ranger.authorizer.impl.class", RemoteAuthorizer)
    properties.setProperty("ranger.authz.remote.pdp.url", pdpUrl)
    SparkRangerAdminPlugin.reset()
    SparkRangerAdminPlugin.initialize(properties)
  }

  override def afterAll(): Unit = {
    SparkRangerAdminPlugin.reset()
    SparkRangerAdminPlugin.initialize()
    mockPdp.foreach(_.close())
    super.afterAll()
  }

  // the authorizer may be re-initialized by other suites running in the same JVM
  override def beforeEach(): Unit = {
    initializeRemoteAuthorizer()
    super.beforeEach()
  }

  test("verify allowed access") {
    SparkRangerAdminPlugin.verify(Seq(tableReq(bob, "default", "src", SELECT)))
    SparkRangerAdminPlugin.verify(
      Seq(
        AccessRequest(
          AccessResource(COLUMN, "default", "src", "key"),
          ugiOf(kent),
          OperationType.QUERY,
          SELECT)))
  }

  test("verify denied access") {
    val e = intercept[AccessControlException] {
      SparkRangerAdminPlugin.verify(Seq(tableReq(someone, "default", "src", SELECT)))
    }
    assert(e.getMessage.contains(s"does not have [select] privilege on [default/src]"))
  }

  test("verify multiple denied accesses in single call") {
    val requests = Seq(
      tableReq(someone, "default", "src", SELECT),
      tableReq(someone, "default", "perm_view", SELECT))
    val e = intercept[AccessControlException] {
      SparkRangerAdminPlugin.verify(requests)
    }
    assert(e.getMessage.contains("[select] privilege on [default/src,default/perm_view]"))
  }

  test("get filter expr") {
    val filterExpr =
      SparkRangerAdminPlugin.getFilterExpr(tableReq(bob, "default", "src", SELECT))
    assert(filterExpr.nonEmpty && filterExpr.get == "key<20")
  }

  test("get mask expr") {
    val maskedExpr = SparkRangerAdminPlugin.getMaskingExpr(
      AccessRequest(
        AccessResource(COLUMN, "default", "src", "value1"),
        ugiOf(bob),
        OperationType.QUERY,
        SELECT))
    assert(maskedExpr.nonEmpty && maskedExpr.get == "md5(cast(value1 as string))")

    val showFirst4Expr = SparkRangerAdminPlugin.getMaskingExpr(
      AccessRequest(
        AccessResource(COLUMN, "default", "src", "value3"),
        ugiOf(bob),
        OperationType.QUERY,
        SELECT))
    assert(showFirst4Expr.nonEmpty && showFirst4Expr.get.contains("regexp_replace"))
  }

  test("access allowed for any access type") {
    assert(SparkRangerAdminPlugin.isAccessAllowed(
      tableReq(bob, "default_bob", "table_use1", USE)))
    assert(!SparkRangerAdminPlugin.isAccessAllowed(
      tableReq(someone, "default_bob", "table_use1", USE)))
  }

  test("access allowed for uri") {
    val uri = AccessRequest(
      AccessResource(URI, "/tmp/data", null, null),
      ugiOf(admin),
      OperationType.LOAD,
      READ)
    assert(SparkRangerAdminPlugin.isAccessAllowed(uri))
    val denied = AccessRequest(
      AccessResource(URI, "/tmp/data", null, null),
      ugiOf(someone),
      OperationType.LOAD,
      READ)
    assert(!SparkRangerAdminPlugin.isAccessAllowed(denied))
  }
}
