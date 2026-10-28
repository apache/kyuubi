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

package org.apache.kyuubi.server.api

import java.net.URI
import java.nio.charset.StandardCharsets.UTF_8
import java.util.{Base64, Locale, UUID}

import scala.util.Try
import scala.util.control.NonFatal

import org.apache.kyuubi.Utils
import org.apache.kyuubi.config.KyuubiConf.ENGINE_DO_AS_ENABLED
import org.apache.kyuubi.config.KyuubiReservedKeys.{KYUUBI_ENGINE_APP_MGR_INFO_KEY, KYUUBI_ENGINE_ID, KYUUBI_ENGINE_URL}
import org.apache.kyuubi.engine.{ApplicationManagerInfo, ApplicationState, EngineType}
import org.apache.kyuubi.ha.HighAvailabilityConf.HA_NAMESPACE
import org.apache.kyuubi.ha.client.{DiscoveryClient, DiscoveryPaths, ServiceNodeInfo}
import org.apache.kyuubi.ha.client.DiscoveryClientProvider.createDiscoveryClient
import org.apache.kyuubi.operation.OperationState
import org.apache.kyuubi.server.KyuubiRestFrontendService
import org.apache.kyuubi.session.SessionHandle

/** Resolves UI destinations from server-owned resource records, never from a client URL. */
private[api] class EngineUIProxyTargetResolver(fe: KyuubiRestFrontendService)
  extends AutoCloseable {
  import EngineUIProxyTargetResolver._

  private var client: Option[DiscoveryClient] = None
  @volatile private var closed = false

  private def discoveryClient: DiscoveryClient = synchronized {
    require(!closed, "Engine UI target resolver is closed")
    client.getOrElse {
      val created = createDiscoveryClient(fe.getConf)
      try {
        created.createClient()
        client = Some(created)
        created
      } catch {
        case NonFatal(e) =>
          Utils.tryLogNonFatalError(created.closeClient())
          throw e
      }
    }
  }

  override def close(): Unit = synchronized {
    closed = true
    try client.foreach(_.closeClient())
    finally client = None
  }

  def resolveResource(kind: String, reference: String): Option[EngineUIProxyTarget] = {
    require(!closed, "Engine UI target resolver is closed")
    kind match {
      case "engines" => resolveEngine(reference)
      case "batches" => resolveBatch(reference)
      case _ => None
    }
  }

  private def resolveEngine(reference: String): Option[EngineUIProxyTarget] = {
    decodeReference(reference).flatMap { case (namespace, refId) =>
      engineScope(namespace, fe.getConf.get(HA_NAMESPACE)).flatMap {
        case (_, owner) =>
          val nodes = discoveryClient.getServiceNodesInfo(namespace)
            .filter(_.engineRefId.contains(refId))
          // Never pick an arbitrary incarnation when discovery contains duplicate records.
          if (nodes.size == 1) engineTarget(nodes.head, refId, owner) else None
      }
    }
  }

  private def engineTarget(
      node: ServiceNodeInfo,
      refId: String,
      owner: String): Option[EngineUIProxyTarget] = {
    node.attributes.get(KYUUBI_ENGINE_URL).flatMap(parseTarget).map { uri =>
      val managerInfo = node.attributes.get(KYUUBI_ENGINE_APP_MGR_INFO_KEY)
        .map(ApplicationManagerInfo.deserialize)
      if (managerInfo.exists(_.resourceManager.exists(
          _.toLowerCase(Locale.ROOT).startsWith("yarn")))) {
        val applicationId = node.attributes.get(KYUUBI_ENGINE_ID)
        val applicationPath = applicationId.filter(_.matches("application_[0-9]+_[0-9]+"))
          .map("/proxy/" + _)
        val proxyUser =
          if (fe.getConf.get(ENGINE_DO_AS_ENABLED)) {
            owner
          } else Utils.currentUser
        EngineUIProxyTarget(
          uri,
          applicationPath,
          () => {
            // Query only when the driver requires its web proxy. A directly accessible UI
            // must not depend on the ResourceManager or its web proxy being available.
            fe.sessionManager.applicationManager
              .getApplicationInfo(managerInfo.get, refId, Some(proxyUser))
              .filter(info =>
                applicationId.contains(info.id) &&
                  !ApplicationState.isTerminated(info.state))
              .flatMap(_.url).flatMap(parseTarget)
          })
      } else EngineUIProxyTarget(uri)
    }
  }

  private def batchTarget(url: Option[String], applicationId: Option[String])
      : Option[EngineUIProxyTarget] = {
    url.flatMap(parseTarget).map { uri =>
      EngineUIProxyTarget(
        uri,
        applicationId.filter(_.matches("application_[0-9]+_[0-9]+")).map("/proxy/" + _))
    }
  }

  private def resolveBatch(reference: String): Option[EngineUIProxyTarget] = {
    Try(SessionHandle.fromUUID(reference)).toOption.flatMap { handle =>
      val manager = fe.sessionManager
      manager.getBatchSession(handle) match {
        case Some(session) =>
          val operation = session.batchJobSubmissionOp
          if (OperationState.isTerminal(operation.getStatus.state)) None
          else operation.getApplicationInfo.filterNot(info =>
            ApplicationState.isTerminated(info.state))
            .flatMap(info => batchTarget(info.url, Option(info.id)))
        case None =>
          manager.getBatchMetadata(reference).flatMap { metadata =>
            if (metadata.state == null ||
              OperationState.isTerminal(OperationState.withName(metadata.state)) ||
              metadata.appState.exists(ApplicationState.isTerminated)) {
              None
            } else {
              batchTarget(Option(metadata.engineUrl), Option(metadata.engineId))
            }
          }
      }
    }
  }

}

private[api] case class EngineUIProxyTarget(
    uri: URI,
    applicationPath: Option[String] = None,
    resolveProxy: () => Option[URI] = () => None)

private[api] object EngineUIProxyTargetResolver {
  val REFERENCE_ATTRIBUTE = "kyuubi.engine.ui.proxy.ref"
  private val shareLevels = Seq("SERVER_LOCAL", "CONNECTION", "SERVER", "GROUP", "USER")

  def reference(node: ServiceNodeInfo): Option[String] = node.engineRefId.map { refId =>
    Base64.getUrlEncoder.withoutPadding().encodeToString(
      s"${node.namespace}\n$refId".getBytes(UTF_8))
  }

  private[api] def decodeReference(reference: String): Option[(String, String)] = {
    if (reference.length > 4096 || !reference.matches("[A-Za-z0-9_-]+")) return None
    Try(new String(Base64.getUrlDecoder.decode(reference), UTF_8)).toOption.flatMap { value =>
      value.split("\n", -1) match {
        case Array(namespace, refId) if Try(UUID.fromString(refId)).isSuccess =>
          Some(namespace -> refId)
        case _ => None
      }
    }
  }

  private[api] def engineScope(
      namespace: String,
      serverNamespace: String): Option[(String, String)] = {
    val prefix = DiscoveryPaths.makePath(null, serverNamespace) + "_"
    if (!namespace.startsWith(prefix)) return None
    val parts = namespace.substring(prefix.length).split("/", -1)
    if (parts.length < 2 || parts.exists(p =>
        p.isEmpty || p == "." || p == ".." ||
          p.exists(c => c == '%' || c == '\\' || c.isControl))) {
      return None
    }
    (for {
      shareLevel <- shareLevels.iterator
      engineType <- EngineType.values.iterator
      suffix = s"_${shareLevel}_$engineType"
      if parts.head.endsWith(suffix)
      version = parts.head.stripSuffix(suffix)
      if version.matches("[A-Za-z0-9.-]+")
    } yield (shareLevel, parts(1))).take(1).toList.headOption
  }

  private[api] def parseTarget(value: String): Option[URI] = {
    val address = if (value.contains("://")) value else s"http://$value"
    Try(new URI(address)).toOption.filter { uri =>
      Set("http", "https").contains(uri.getScheme) && uri.getHost != null &&
      uri.getUserInfo == null && uri.getFragment == null && uri.getQuery == null &&
      (uri.getPort == -1 || uri.getPort > 0 && uri.getPort <= 65535) &&
      EngineUIProxyServlet.safePath(Option(uri.getRawPath).getOrElse(""))
    }
  }
}
