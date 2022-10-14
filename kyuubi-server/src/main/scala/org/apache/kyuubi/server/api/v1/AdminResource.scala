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
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.ha.HighAvailabilityConf.HA_NAMESPACE
import org.apache.kyuubi.ha.client.{DiscoveryPaths, ServiceNodeInfo}
import org.apache.kyuubi.ha.client.DiscoveryClientProvider.withDiscoveryClient
import org.apache.kyuubi.server.{KyuubiRestFrontendService, KyuubiServer}
import org.apache.kyuubi.server.api.ApiRequestContext
import org.apache.kyuubi.server.api.v1.AdminResource._
import org.apache.kyuubi.service.authentication.KyuubiAuthenticationFactory

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
      @QueryParam("type") engineType: String,
      @QueryParam("sharelevel") shareLevel: String,
      @QueryParam("subdomain") subdomain: String,
      @QueryParam("hive.server2.proxy.user") hs2ProxyUser: String): Response = {
    val userName = getUserName(fe, hs2ProxyUser)
    val engineSpace =
      getEngineSpace(userName, engineType, shareLevel, subdomain, "default", fe.getConf)

    withDiscoveryClient(fe.getConf) { discoveryClient =>
      val engineNodes = discoveryClient.getChildren(engineSpace)
      engineNodes.foreach { node =>
        val nodePath = s"$engineSpace/$node"
        info(s"Deleting engine node:$nodePath")
        try {
          discoveryClient.delete(nodePath)
        } catch {
          case e: Exception =>
            error(s"Failed to delete engine node:$nodePath", e)
            throw new NotFoundException(s"Failed to delete engine node:$nodePath," +
              s"${e.getMessage}")
        }
      }
    }

    Response.ok(s"Engine $engineSpace is deleted successfully.").build()
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON)),
    description = "delete kyuubi engine")
  @GET
  @Path("engine")
  def listEngine(
      @QueryParam("type") engineType: String,
      @QueryParam("sharelevel") shareLevel: String,
      @QueryParam("subdomain") subdomain: String,
      @QueryParam("hive.server2.proxy.user") hs2ProxyUser: String): Seq[ServiceNodeInfo] = {
    val userName = getUserName(fe, hs2ProxyUser)
    val engineSpace = getEngineSpace(userName, engineType, shareLevel, subdomain, "", fe.getConf)

    var engineNodes = Seq.empty[ServiceNodeInfo]
    Option(subdomain) match {
      case Some(_) =>
        withDiscoveryClient(fe.getConf) { discoveryClient =>
          info(s"Listing engine nodes for $engineSpace")
          engineNodes = engineNodes ++
            discoveryClient.getServiceNodesInfo(engineSpace)
        }
      case None =>
        withDiscoveryClient(fe.getConf) { discoveryClient =>
          discoveryClient.getChildren(engineSpace).map { child =>
            info(s"Listing engine nodes for $engineSpace/$child")
            engineNodes = engineNodes ++ discoveryClient.getServiceNodesInfo(s"$engineSpace/$child")
          }
        }
    }
    engineNodes
  }
}

object AdminResource {

  def getUserName(fe: KyuubiRestFrontendService, hs2ProxyUser: String): String = {
    val sessionConf = Option(hs2ProxyUser).filter(_.nonEmpty).map(proxyUser =>
      Map(KyuubiAuthenticationFactory.HS2_PROXY_USER -> proxyUser)).getOrElse(Map())

    var userName: String = null
    try {
      userName = fe.getUserName(sessionConf)
    } catch {
      case t: Throwable =>
        throw new NotAllowedException(t.getMessage)
    }
    userName
  }

  def getEngineSpace(
      userName: String,
      engineType: String,
      shareLevel: String,
      subdomain: String,
      subdomainDefault: String,
      conf: KyuubiConf): String = {
    // use default value from kyuubi conf when param is not provided
    val clonedConf: KyuubiConf = conf.clone
    Option(engineType).foreach(clonedConf.set(ENGINE_TYPE, _))
    Option(subdomain).filter(_.nonEmpty)
      .foreach(_ => clonedConf.set(ENGINE_SHARE_LEVEL_SUBDOMAIN, Option(subdomain)))
    Option(shareLevel).filter(_.nonEmpty).foreach(clonedConf.set(ENGINE_SHARE_LEVEL, _))

    val normalizedEngineType = clonedConf.get(ENGINE_TYPE)
    val engineSubdomain = clonedConf.get(ENGINE_SHARE_LEVEL_SUBDOMAIN).getOrElse(subdomainDefault)
    val engineShareLevel = clonedConf.get(ENGINE_SHARE_LEVEL)

    DiscoveryPaths.makePath(
      s"${clonedConf.get(HA_NAMESPACE)}_${KYUUBI_VERSION}_" +
        s"${engineShareLevel}_$normalizedEngineType",
      userName,
      Array(engineSubdomain))
  }
}
