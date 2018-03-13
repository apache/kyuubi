/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark

import java.security.PrivilegedExceptionAction

import org.apache.hadoop.security.UserGroupInformation

class SparkUtilsSuite extends SparkFunSuite {


  test("get current user name") {
    val user = SparkUtils.getCurrentUserName()
    assert(user === System.getProperty("user.name"))
  }

  test("get user with impersonation") {
    val currentUser = UserGroupInformation.getCurrentUser
    val user1 = SparkUtils.getCurrentUserName()
    assert(user1 === currentUser.getShortUserName)
    val remoteUser = UserGroupInformation.createRemoteUser("test")
    remoteUser.doAs(new PrivilegedExceptionAction[Unit] {
      override def run(): Unit = {
        val user2 = SparkUtils.getCurrentUserName()
        assert(user2 === remoteUser.getShortUserName)
      }
    })
  }
}
