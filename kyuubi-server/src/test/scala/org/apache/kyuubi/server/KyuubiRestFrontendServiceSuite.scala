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

package org.apache.kyuubi.server

import org.apache.kyuubi.RestFrontendTestHelper

class KyuubiRestFrontendServiceSuite extends RestFrontendTestHelper {

  test("kyuubi REST frontend service http basic") {
    val resp = webTarget.path("/api/v1/ping").request().get()
    assert(resp.readEntity(classOf[String]) === "pong")
  }

  test("test error and exception response") {
    // send a not exists request
    var response = webTarget.path("api/v1/pong").request().get()
    assert(404 == response.getStatus)
    assert(response.getStatusInfo.getReasonPhrase.equalsIgnoreCase("not found"))

    // send a exists request but wrong http method
    response = webTarget.path("api/v1/ping").request().post(null)
    assert(405 == response.getStatus)
    assert(response.getStatusInfo.getReasonPhrase.equalsIgnoreCase("method not allowed"))

    // send a request but throws a exception on the server side
    response = webTarget.path("api/v1/exception").request().get()
    assert(500 == response.getStatus)
    assert(response.getStatusInfo.getReasonPhrase.equalsIgnoreCase("server error"))
  }

  test("swagger ui") {
    val resp = webTarget.path("/api/v1/swagger-ui").request().get()
    assert(resp.getStatus === 200)
  }

  test("swagger ui json data") {
    val resp = webTarget.path("/openapi.json").request().get()
    assert(resp.getStatus === 200)
  }
}
