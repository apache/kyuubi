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

import java.net.{URI, URL}
import java.util.Locale
import java.util.regex.Pattern
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}

import scala.util.Try

import org.apache.commons.lang3.StringUtils
import org.eclipse.jetty.client.api.Request
import org.eclipse.jetty.proxy.ProxyServlet

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.server.KyuubiRestFrontendService

private[api] class EngineUIProxyServlet(
    proxyEnabled: Boolean,
    allowedHosts: Set[String]) extends ProxyServlet with Logging {

  private val allowedHostPatterns =
    allowedHosts.map(EngineUIProxyServlet.normalizeHost).filter(_.nonEmpty).map(
      EngineUIProxyServlet.wildcardHostPattern)

  override def rewriteTarget(request: HttpServletRequest): String = {
    val requestURL = request.getRequestURL
    val requestURI = request.getRequestURI
    // A null target is handled by onProxyRewriteFailed with an actionable 403 response.
    val targetURL = allowedTarget(requestURI).map { targetAddress =>
      val targetURI = targetAddress.path match {
        // for some reason, the proxy can not handle redirect well, as a workaround,
        // we simulate the Spark UI redirection behavior and forcibly rewrite the
        // empty URI to the Spark Jobs page.
        case "" | "/" => "/jobs/"
        case path => path
      }
      val targetQueryString =
        Option(
          request.getQueryString).filter(StringUtils.isNotEmpty).map(q => s"?$q").getOrElse("")
      new URL(
        "http",
        targetAddress.host,
        targetAddress.port,
        targetURI + targetQueryString).toString
    }.orNull
    debug(s"rewrite $requestURL => $targetURL")
    targetURL
  }

  override def onProxyRewriteFailed(
      request: HttpServletRequest,
      response: HttpServletResponse): Unit = {
    val (title, message) = deniedRequestMessage(request.getRequestURI)
    response.setStatus(HttpServletResponse.SC_FORBIDDEN)
    response.setCharacterEncoding("UTF-8")
    response.setContentType("text/html")
    response.getWriter.write(EngineUIProxyServlet.errorPage(
      title,
      message,
      extractTargetAddress(request.getRequestURI)))
  }

  override def addXForwardedHeaders(
      clientRequest: HttpServletRequest,
      proxyRequest: Request): Unit = {
    allowedTarget(clientRequest.getRequestURI).foreach {
      case TargetAddress(host, port, _) =>
        // SPARK-24209: Knox uses X-Forwarded-Context to notify the application the base path
        proxyRequest.header("X-Forwarded-Context", s"/engine-ui/$host:$port")
    }
    super.addXForwardedHeaders(clientRequest, proxyRequest)
  }

  private def isAllowedHost(host: String): Boolean =
    proxyEnabled && {
      val normalizedHost = EngineUIProxyServlet.normalizeHost(host)
      allowedHostPatterns.exists(_.matcher(normalizedHost).matches())
    }

  private def allowedTarget(requestURI: String): Option[TargetAddress] =
    extractTargetAddress(requestURI).filter(target => isAllowedHost(target.host))

  private[api] def deniedRequestMessage(requestURI: String): (String, String) = {
    if (!proxyEnabled) {
      val allowedHost = extractTargetAddress(requestURI)
        .map(target => s"add <code>${EngineUIProxyServlet.escapeHtml(target.host)}</code> to")
        .getOrElse("add the target host to")
      (
        "Engine UI proxy is disabled",
        s"Ask an administrator to set <code>${
            KyuubiConf.FRONTEND_REST_ENGINE_UI_PROXY_ENABLED.key
          }</code> to <code>true</code>, then $allowedHost <code>${
            KyuubiConf.FRONTEND_REST_ENGINE_UI_PROXY_HOSTS.key
          }</code>.")
    } else {
      extractTargetAddress(requestURI) match {
        case Some(target) =>
          val escapedHost = EngineUIProxyServlet.escapeHtml(target.host)
          (
            "Engine UI host is not allowed",
            s"Ask an administrator to add <code>$escapedHost</code> " +
              s"to <code>${KyuubiConf.FRONTEND_REST_ENGINE_UI_PROXY_HOSTS.key}</code>.")
        case None =>
          (
            "Invalid Engine UI address",
            "The URL must include an engine host and port, for example " +
              "<code>/engine-ui/spark.example.com:4040/</code>.")
      }
    }
  }

  private[api] def extractTargetAddress(requestURI: String): Option[TargetAddress] = {
    val target = requestURI.stripPrefix("/engine-ui/")
    if (target == requestURI) {
      None
    } else {
      Try(new URI(s"http://$target")).toOption.flatMap { uri =>
        for {
          host <- Option(uri.getHost)
          port <- Option(uri.getPort).filter(_ >= 0)
        } yield TargetAddress(
          host,
          port,
          Option(uri.getRawPath).filter(_.nonEmpty).getOrElse("/"))
      }
    }
  }

}

private[api] case class TargetAddress(host: String, port: Int, path: String)

private[api] object EngineUIProxyServlet {

  private def errorPage(
      title: String,
      message: String,
      targetAddress: Option[TargetAddress]): String = {
    val target = targetAddress.map { address =>
      s"""<div class="target">
         |      <span>Target engine</span>
         |      <code>${escapeHtml(address.host)}:${address.port}</code>
         |    </div>""".stripMargin
    }.getOrElse("")

    s"""<!DOCTYPE html>
       |<html lang="en">
       |<head>
       |  <meta charset="UTF-8">
       |  <meta name="viewport" content="width=device-width, initial-scale=1">
       |  <title>403 - $title</title>
       |  <style>
       |    * { box-sizing: border-box; }
       |    body { margin: 0; min-height: 100vh; display: grid; place-items: center;
       |      font-family: "Myriad Pro", "Helvetica Neue", Arial, Helvetica, sans-serif;
       |      background: #eef0f3; color: #242e42; }
       |    main { width: min(680px, calc(100% - 40px)); background: #fff;
       |      border: 1px solid #dcdfe6; border-top: 4px solid #242e42;
       |      box-shadow: 0 8px 24px rgba(0, 21, 41, .08); }
       |    header { height: 56px; display: flex; align-items: center; padding: 0 28px;
       |      border-bottom: 1px solid #ebeef5; color: #999; font-size: 14px;
       |      font-weight: 600; letter-spacing: .06em; }
       |    header::before { content: ""; width: 10px; height: 10px; margin-right: 10px;
       |      background: #e7211a; transform: rotate(45deg); }
       |    article { padding: 36px 40px 40px; }
       |    .status { color: #db2828; font-size: 13px; font-weight: 700; letter-spacing: .08em; }
       |    h1 { margin: 10px 0 12px; font-size: 30px; line-height: 1.25; font-weight: 600; }
       |    p { margin: 0; color: #606266; font-size: 16px; line-height: 1.7; }
       |    code { padding: 2px 6px; background: #f2f6fc; color: #242e42;
       |      font-family: "SFMono-Regular", Consolas, "Liberation Mono", monospace;
       |      font-size: 14px; overflow-wrap: anywhere; }
       |    .target { margin: 28px 0 20px; padding: 14px 16px; display: flex;
       |      align-items: center; gap: 16px; background: #f5f7fa; border-left: 3px solid #1890ff; }
       |    .target span { min-width: 96px; color: #909399; font-size: 13px; font-weight: 600;
       |      letter-spacing: .02em; text-transform: uppercase; }
       |    .target code { padding: 0; background: transparent; color: #001529; font-weight: 600; }
       |    @media (max-width: 540px) {
       |      article { padding: 28px 24px 32px; }
       |      header { padding: 0 24px; }
       |      h1 { font-size: 25px; }
       |      .target { align-items: flex-start; flex-direction: column; gap: 6px; }
       |    }
       |  </style>
       |</head>
       |<body>
       |  <main>
       |    <header>APACHE KYUUBI</header>
       |    <article>
       |      <div class="status">403 / ACCESS DENIED</div>
       |      <h1>$title</h1>
       |      $target
       |      <p>$message</p>
       |    </article>
       |  </main>
       |</body>
       |</html>""".stripMargin
  }

  private def escapeHtml(value: String): String =
    value
      .replace("&", "&amp;")
      .replace("<", "&lt;")
      .replace(">", "&gt;")
      .replace("\"", "&quot;")
      .replace("'", "&#39;")

  def apply(fe: KyuubiRestFrontendService): EngineUIProxyServlet = {
    new EngineUIProxyServlet(
      fe.getConf.get(KyuubiConf.FRONTEND_REST_ENGINE_UI_PROXY_ENABLED),
      fe.getConf.get(KyuubiConf.FRONTEND_REST_ENGINE_UI_PROXY_HOSTS).toSet)
  }

  private[api] def normalizeHost(host: String): String = host.trim.toLowerCase(Locale.ROOT)

  private[api] def wildcardHostPattern(hostPattern: String): Pattern = {
    Pattern.compile(hostPattern.map {
      case '*' => ".*"
      case c => Pattern.quote(c.toString)
    }.mkString("^", "", "$"))
  }
}
