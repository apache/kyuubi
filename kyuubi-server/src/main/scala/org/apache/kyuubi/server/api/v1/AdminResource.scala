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

import javax.ws.rs.{NotAllowedException, Path, POST, Produces}
import javax.ws.rs.core.{MediaType, Response}

import io.swagger.v3.oas.annotations.media.Content
import io.swagger.v3.oas.annotations.responses.ApiResponse
import io.swagger.v3.oas.annotations.tags.Tag

import org.apache.kyuubi.{Logging, Utils}
import org.apache.kyuubi.server.KyuubiServer
import org.apache.kyuubi.server.api.ApiRequestContext
import org.apache.kyuubi.server.http.authentication.AuthenticationFilter

@Tag(name = "Admin")
@Produces(Array(MediaType.APPLICATION_JSON))
private[v1] class AdminResource extends ApiRequestContext with Logging {
  private lazy val administrator = Utils.currentUser

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON)),
    description = "refresh the Kyuubi server hadoop conf, note that, " +
      "it only takes affect for frontend services now")
  @POST
  @Path("refresh/hadoop_conf")
  def refreshFrontendHadoopConf(): Response = {
    val userName = fe.getUserName(Map.empty)
    val ipAddress = AuthenticationFilter.getUserIpAddress
    info(s"Receive refresh Kyuubi server hadoop conf request from $userName/$ipAddress")
    if (!userName.equals(administrator)) {
      throw new NotAllowedException(
        s"$userName is not allowed to refresh the Kyuubi server hadoop conf")
    }
    info(s"Reloading the Kyuubi server hadoop conf")
    KyuubiServer.reloadHadoopConf()
    Response.ok().build()
  }
}
