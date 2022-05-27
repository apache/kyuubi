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

package org.apache.kyuubi.server.api.v1

import java.util.Base64

import org.apache.hadoop.security.UserGroupInformation

import org.apache.kyuubi.{KyuubiFunSuite, RestFrontendTestHelper}
import org.apache.kyuubi.server.http.authentication.AuthenticationHandler.AUTHORIZATION_HEADER

class AdminResourceSuite extends KyuubiFunSuite with RestFrontendTestHelper {
  test("refresh Hadoop configuration of the kyuubi server") {
    var response = webTarget.path("api/v1/admin/refreshServerHadoopConf")
      .request()
      .post(null)
    assert(405 == response.getStatus)

    val adminUser = UserGroupInformation.getCurrentUser.getShortUserName
    val encodeAuthorization = new String(
      Base64.getEncoder.encode(
        s"$adminUser:".getBytes()),
      "UTF-8")
    response = webTarget.path("api/v1/admin/refreshServerHadoopConf")
      .request()
      .header(AUTHORIZATION_HEADER, s"BASIC $encodeAuthorization")
      .post(null)
    assert(200 == response.getStatus)
  }
}
