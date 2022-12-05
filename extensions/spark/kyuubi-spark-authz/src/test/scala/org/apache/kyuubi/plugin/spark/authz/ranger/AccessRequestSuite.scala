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

// scalastyle:off

import org.apache.hadoop.security.UserGroupInformation
import org.scalatest.funsuite.AnyFunSuite

import org.apache.kyuubi.plugin.spark.authz.ObjectType._
import org.apache.kyuubi.plugin.spark.authz.{ObjectType, OperationType}
import org.apache.kyuubi.plugin.spark.authz.ranger.SparkRangerAdminPlugin.getFilterExpr

class AccessRequestSuite extends AnyFunSuite {
  test("[[KYUUBI #3300]] overriding userGroups with UserStore") {
    SparkRangerAdminPlugin.init()

    val resource1 = AccessResource(COLUMN, "my_db_name", "my_table_name", "my_col_1,my_col_2")

    val ugi1 = UserGroupInformation.createRemoteUser("anonymous")
    val art1 = AccessRequest(resource1, ugi1, OperationType.QUERY, AccessType.SELECT)
    assert(art1.getUserGroups.isEmpty)

    val ugi2 = UserGroupInformation.createRemoteUser("bob")
    val art2 = AccessRequest(resource1, ugi2, OperationType.QUERY, AccessType.SELECT)
    assert(!art2.getUserGroups.isEmpty)
    assert(art2.getUserGroups.contains("group_a"))
    assert(art2.getUserGroups.contains("group_b"))

    val are = AccessResource(ObjectType.TABLE, "default", "src_group_row_filter", null)
    val art3=AccessRequest(are, ugi2, OperationType.QUERY, AccessType.SELECT)
    val maybeString = getFilterExpr(art3)
    assert(maybeString.get === "key<120")
  }
}
