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
import org.apache.spark.sql.SparkSessionExtensions

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.config.KyuubiReservedKeys.KYUUBI_SESSION_USER_KEY
import org.apache.kyuubi.plugin.spark.authz.SparkSessionProvider
import org.apache.kyuubi.plugin.spark.authz.util.AuthZUtils

class AuthzSessionUserSuite extends KyuubiFunSuite with SparkSessionProvider {
  override protected val extension: SparkSessionExtensions => Unit = new RangerSparkExtension
  override protected val catalogImpl: String = "in-memory"

  override def afterAll(): Unit = {
    spark.stop()
    super.afterAll()
  }

  test("KYUUBI #7684 - a cleared session user must be null rather than empty") {
    val sc = spark.sparkContext

    // An empty user passes the null check and reaches createRemoteUser, which rejects it. This is
    // why an operation has to clear the property with null.
    sc.setLocalProperty(KYUUBI_SESSION_USER_KEY, "")
    val e = intercept[IllegalArgumentException](AuthZUtils.getAuthzUgi(sc))
    assert(e.getMessage === "Null user")

    sc.setLocalProperty(KYUUBI_SESSION_USER_KEY, null)
    assert(AuthZUtils.getAuthzUgi(sc).getUserName ===
      UserGroupInformation.getCurrentUser.getUserName)
  }
}
