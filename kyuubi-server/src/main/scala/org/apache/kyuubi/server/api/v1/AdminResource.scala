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

import javax.ws.rs._
import javax.ws.rs.core.{MediaType, Response}

import io.swagger.v3.oas.annotations.media.Content
import io.swagger.v3.oas.annotations.responses.ApiResponse
import io.swagger.v3.oas.annotations.tags.Tag

import org.apache.kyuubi.{KYUUBI_VERSION, Logging, Utils}
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.ha.HighAvailabilityConf.HA_NAMESPACE
import org.apache.kyuubi.ha.client.DiscoveryClientProvider.withDiscoveryClient
import org.apache.kyuubi.ha.client.DiscoveryPaths
import org.apache.kyuubi.server.KyuubiServer
import org.apache.kyuubi.server.api.ApiRequestContext

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
    val ipAddress = fe.getIpAddress
    info(s"Receive refresh Kyuubi server hadoop conf request from $userName/$ipAddress")
    if (!userName.equals(administrator)) {
      throw new NotAllowedException(
        s"$userName is not allowed to refresh the Kyuubi server hadoop conf")
    }
    info(s"Reloading the Kyuubi server hadoop conf")
    KyuubiServer.reloadHadoopConf()
    Response.ok(s"Refresh the hadoop conf for ${fe.connectionUrl} successfully.").build()
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON)),
    description = "delete kyuubi engine")
  @DELETE
  @Path("engine")
  def deleteEngine(
      @QueryParam("user") user: String,
      @QueryParam("type") engineType: String,
      @QueryParam("subdomain") engineSubdomain: String,
      @QueryParam("sharelevel") engineShareLevel: String,
      @QueryParam("version") version: String,
      @QueryParam("namespace") namespace: String): Response = {
    val userName = fe.getUserName(Map.empty)
    val ipAddress = fe.getIpAddress
    info(s"Received delete engine request from $userName/$ipAddress")
    if (!userName.equals(administrator)) {
      throw new NotAllowedException(
        s"$userName is not allowed to delete kyuubi engine")
    }

    // validate parameters
    require(user != null && !user.isEmpty, "user is a required parameter")

    // use default value from kyuubi conf when param is not provided
    val normalizedEngineType = Some(engineType)
      .filter(_ != null).filter(_.nonEmpty)
      .getOrElse(fe.getConf.get(ENGINE_TYPE))
    val normalizedEngineSubdomain = Some(engineSubdomain)
      .filter(_ != null).filter(_.nonEmpty)
      .getOrElse(fe.getConf.get(ENGINE_SHARE_LEVEL_SUBDOMAIN).getOrElse("default"))
    val normalizedEngineShareLevel = Some(engineShareLevel)
      .filter(_ != null).filter(_.nonEmpty)
      .getOrElse(fe.getConf.get(ENGINE_SHARE_LEVEL))
    val normalizedNamespace = Some(namespace)
      .filter(_ != null).filter(_.nonEmpty)
      .getOrElse(fe.getConf.get(HA_NAMESPACE))
    val normalizedVersion = Some(version)
      .filter(_ != null).filter(_.nonEmpty)
      .getOrElse(KYUUBI_VERSION)

    val engineSpace = DiscoveryPaths.makePath(
      s"${normalizedNamespace}_" +
        s"${normalizedVersion}_" +
        s"${normalizedEngineShareLevel}_${normalizedEngineType}",
      user,
      Array(normalizedEngineSubdomain))

    withDiscoveryClient(fe.getConf) { discoveryClient =>
      val serviceNodes = discoveryClient.getServiceNodesInfo(engineSpace)
      serviceNodes.foreach { node =>
        val nodePath = s"$engineSpace/${node.nodeName}"
        info(s"Deleting zookeeper engine node:$nodePath")
        try {
          discoveryClient.delete(nodePath)
        } catch {
          case e: Exception =>
            error(s"Failed to delete zookeeper engine node:$nodePath", e)
            throw new NotFoundException(s"Failed to delete zookeeper engine node:$nodePath," +
              s"${e.getMessage}")
        }
      }
    }

    Response.ok(s"Engine ${engineSpace} is deleted successfully.").build()
  }
}
