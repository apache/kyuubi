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

import org.apache.hadoop.security.UserGroupInformation
import org.apache.ranger.authz.api.RangerAuthorizer
import org.apache.ranger.authz.model.{RangerAuthzRequest, RangerAuthzResult, RangerMultiAuthzRequest, RangerMultiAuthzResult, RangerResourcePermissions, RangerResourcePermissionsRequest}

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.plugin.spark.authz.{AccessControlException, ObjectType, OperationType}
import org.apache.kyuubi.plugin.spark.authz.RangerTestNamespace._
import org.apache.kyuubi.plugin.spark.authz.RangerTestUsers._
import org.apache.kyuubi.plugin.spark.authz.ranger.SparkRangerAdminPlugin._

/**
 * An authorizer that returns no result for any authorization request.
 */
class NoResultAuthorizer(properties: Properties) extends RangerAuthorizer(properties) {
  override def init(): Unit = ()
  override def close(): Unit = {}
  override def authorize(request: RangerAuthzRequest): RangerAuthzResult = null
  override def authorize(request: RangerMultiAuthzRequest): RangerMultiAuthzResult = null
  override def getResourcePermissions(
      request: RangerResourcePermissionsRequest): RangerResourcePermissions = null
}

class SparkRangerAdminPluginSuite extends KyuubiFunSuite {

  private val RemoteAuthorizer = "org.apache.ranger.authz.remote.RangerRemoteAuthorizer"

  private def tableSelectRequest(user: String, database: String, table: String): AccessRequest =
    AccessRequest(
      AccessResource(ObjectType.TABLE, database, table, null),
      UserGroupInformation.createRemoteUser(user),
      OperationType.QUERY,
      AccessType.SELECT)

  /**
   * initializes the plugin with the given authorizer properties,
   * and restores the default authorizer after running the body
   */
  private def withAuthorizer(properties: Properties)(body: => Unit): Unit = {
    SparkRangerAdminPlugin.initialize()
    try {
      SparkRangerAdminPlugin.reset()
      SparkRangerAdminPlugin.initialize(properties)
      body
    } finally {
      SparkRangerAdminPlugin.reset()
      SparkRangerAdminPlugin.initialize()
    }
  }

  test("get filter expression") {
    val bob = UserGroupInformation.createRemoteUser("bob")
    val are = AccessResource(ObjectType.TABLE, defaultDb, "src", null)
    def buildAccessRequest(ugi: UserGroupInformation): AccessRequest = {
      AccessRequest(are, ugi, OperationType.QUERY, AccessType.SELECT)
    }
    val maybeString = getFilterExpr(buildAccessRequest(bob))
    assert(maybeString.get === "key<20")
    Seq(admin, alice).foreach { user =>
      val ugi = UserGroupInformation.createRemoteUser(user)
      val maybeString = getFilterExpr(buildAccessRequest(ugi))
      assert(maybeString.isEmpty)
    }
  }

  test("get data masker") {
    val bob = UserGroupInformation.createRemoteUser("bob")
    def buildAccessRequest(ugi: UserGroupInformation, column: String): AccessRequest = {
      val are = AccessResource(ObjectType.COLUMN, defaultDb, "src", column)
      AccessRequest(are, ugi, OperationType.QUERY, AccessType.SELECT)
    }
    assert(getMaskingExpr(buildAccessRequest(bob, "value1")).get === "md5(cast(value1 as string))")
    assert(getMaskingExpr(buildAccessRequest(bob, "value2")).get ===
      "regexp_replace(regexp_replace(regexp_replace(regexp_replace(value2, '[A-Z]', 'X')," +
      " '[a-z]', 'x'), '[0-9]', 'n'), '[^A-Za-z0-9]', 'U')")
    assert(getMaskingExpr(buildAccessRequest(bob, "value3")).get contains "regexp_replace")
    assert(getMaskingExpr(buildAccessRequest(bob, "value4")).get === "date_trunc('YEAR', value4)")
    assert(getMaskingExpr(buildAccessRequest(bob, "value5")).get ===
      "concat(regexp_replace(regexp_replace(regexp_replace(regexp_replace(" +
      "left(value5, length(value5) - 4), '[A-Z]', 'X'), '[a-z]', 'x')," +
      " '[0-9]', 'n'), '[^A-Za-z0-9]', 'U'), right(value5, 4))")

    Seq(admin, alice).foreach { user =>
      val ugi = UserGroupInformation.createRemoteUser(user)
      val maybeString = getMaskingExpr(buildAccessRequest(ugi, "value1"))
      assert(maybeString.isEmpty)
    }
  }

  test("deny accesses when the authorizer returns no result") {
    val properties = new Properties
    properties.setProperty("ranger.authorizer.impl.class", classOf[NoResultAuthorizer].getName)
    withAuthorizer(properties) {
      val e = intercept[AccessControlException] {
        verify(Seq(tableSelectRequest(bob, defaultDb, "src")))
      }
      assert(e.getMessage.contains(s"Permission denied: user [$bob] does not have " +
        s"[select] privilege on [$defaultDb/src]"))
    }
  }

  test("deny accesses when the authorizer returns no result in single call") {
    val properties = new Properties
    properties.setProperty("ranger.authorizer.impl.class", classOf[NoResultAuthorizer].getName)
    withAuthorizer(properties) {
      // the single call mode is read from the Ranger configuration resources
      val config = SparkRangerAdminPlugin.config
      config.setBoolean("ranger.plugin.spark.authorize.in.single.call", true)
      try {
        val e = intercept[AccessControlException] {
          verify(Seq(
            tableSelectRequest(bob, defaultDb, "src"),
            tableSelectRequest(bob, defaultDb, "perm_view")))
        }
        assert(e.getMessage.contains(s"Permission denied: user [$bob] does not have " +
          s"[select] privilege on [$defaultDb/src,$defaultDb/perm_view]"))
      } finally {
        config.unset("ranger.plugin.spark.authorize.in.single.call")
      }
    }
  }

  test("the security-critical configurations cannot be overridden by system properties") {
    System.setProperty("ranger.authorizer.impl.class", RemoteAuthorizer)
    // an unreachable Ranger PDP server, to detect an unwanted switch to the remote
    // authorizer by a failed access check
    System.setProperty("ranger.authz.remote.pdp.url", "http://localhost:1")
    try {
      SparkRangerAdminPlugin.reset()
      SparkRangerAdminPlugin.initialize()
      // the embedded authorizer pinned in the configuration resources is used,
      // so the access is evaluated against the local policies
      assert(isAccessAllowed(tableSelectRequest(admin, defaultDb, "src")))
    } finally {
      System.clearProperty("ranger.authorizer.impl.class")
      System.clearProperty("ranger.authz.remote.pdp.url")
      SparkRangerAdminPlugin.reset()
      SparkRangerAdminPlugin.initialize()
    }
  }

  test("fail to initialize when the Ranger PDP url is missing for the remote authorizer") {
    SparkRangerAdminPlugin.initialize()
    val config = SparkRangerAdminPlugin.config
    val originalImplClass = config.get("ranger.authorizer.impl.class")
    config.set("ranger.authorizer.impl.class", RemoteAuthorizer)
    try {
      SparkRangerAdminPlugin.reset()
      val e = intercept[IllegalArgumentException] {
        SparkRangerAdminPlugin.initialize()
      }
      assert(e.getMessage.contains("ranger.authz.remote.pdp.url"))
    } finally {
      config.set("ranger.authorizer.impl.class", originalImplClass)
      SparkRangerAdminPlugin.reset()
      SparkRangerAdminPlugin.initialize()
    }
  }
}
