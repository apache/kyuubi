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

import org.apache.hadoop.security.UserGroupInformation
// scalastyle:off
import org.scalatest.funsuite.AnyFunSuite

import org.apache.kyuubi.plugin.spark.authz.{ObjectType, OperationType}
import org.apache.kyuubi.plugin.spark.authz.RangerTestNamespace._
import org.apache.kyuubi.plugin.spark.authz.RangerTestUsers._
import org.apache.kyuubi.plugin.spark.authz.ranger.SparkRangerAdminPlugin._

class SparkRangerAdminPluginSuite extends AnyFunSuite {
// scalastyle:on

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
}
