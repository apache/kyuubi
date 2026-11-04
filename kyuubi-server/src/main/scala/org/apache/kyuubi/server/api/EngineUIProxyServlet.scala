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

import java.io.IOException
import java.net.URI
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets.ISO_8859_1
import java.util.Locale
import java.util.regex.Pattern
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}

import scala.util.Try
import scala.util.control.NonFatal

import org.eclipse.jetty.client.{HttpClient, HttpRedirector, HttpRequest, RedirectProtocolHandler}
import org.eclipse.jetty.client.api.{Request, Response, Result}
import org.eclipse.jetty.proxy.AsyncMiddleManServlet
import org.eclipse.jetty.proxy.AsyncMiddleManServlet.{ContentTransformer, GZIPContentTransformer}

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.server.KyuubiRestFrontendService

private[api] class EngineUIProxyServlet(
    proxyEnabled: Boolean,
    resolveTarget: (String, String) => Option[EngineUIProxyTarget]) extends AsyncMiddleManServlet
  with Logging {
  import EngineUIProxyServlet._

  override def rewriteTarget(request: HttpServletRequest): String = {
    if (!proxyEnabled) return null
    parseRoute(request.getRequestURI).flatMap { case (kind, reference, path) =>
      try {
        resolveTarget(kind, reference).map { resource =>
          val target = resource.uri
          val basePath = Option(target.getRawPath).getOrElse("").stripSuffix("/")
          val suffix = if (path.isEmpty || path == "/") {
            if (basePath.isEmpty) "/jobs/" else "/"
          } else path
          val query = Option(request.getQueryString).filter(_.nonEmpty).map("?" + _).getOrElse("")
          val targetURL = s"${target.getScheme}://${target.getRawAuthority}$basePath$suffix$query"
          request.setAttribute(
            CONTEXT_ATTRIBUTE,
            new ProxyContext(resource, s"/engine-ui/$kind/$reference"))
          targetURL
        }
      } catch {
        case NonFatal(e) =>
          error(s"Unable to resolve Engine UI $kind resource $reference", e)
          None
      }
    }.orNull
  }

  override def onProxyRewriteFailed(
      request: HttpServletRequest,
      response: HttpServletResponse): Unit = {
    response.setStatus(HttpServletResponse.SC_FORBIDDEN)
    response.setCharacterEncoding("UTF-8")
    response.setContentType("text/html")
    val message = if (!proxyEnabled) {
      s"Ask an administrator to enable <code>${
          KyuubiConf.FRONTEND_REST_ENGINE_UI_PROXY_ENABLED.key
        }</code>."
    } else {
      "Open the UI from an active Engine or Batch in the Kyuubi Web UI."
    }
    response.getWriter.write(errorPage("Engine UI is not accessible", message))
  }

  override protected def createHttpClient(): HttpClient = {
    val client = super.createHttpClient()
    val redirector = new ApplicationRedirector(client)
    client.getProtocolHandlers.put(new RedirectProtocolHandler(client) {
      private def context(request: Request): ProxyContext =
        request.asInstanceOf[HttpRequest].getConversation
          .getAttribute(CONTEXT_ATTRIBUTE).asInstanceOf[ProxyContext]

      override def accept(proxyRequest: Request, response: Response): Boolean = {
        val proxy = context(proxyRequest)
        proxy != null && proxy.proxyBase.isEmpty &&
        Set("GET", "HEAD").contains(proxyRequest.getMethod) &&
        Set(301, 302, 303, 307, 308).contains(response.getStatus) &&
        proxy.resource.applicationPath.nonEmpty
      }

      override def onComplete(result: Result): Unit = {
        if (result.isFailed) {
          super.onComplete(result)
          return
        }
        val proxy = context(result.getRequest)
        val resource = proxy.resource
        try {
          val location = Option(result.getResponse.getHeaders.get("Location"))
            .getOrElse(throw new IOException("Application redirect has no Location"))
          val source = result.getRequest.getURI
          val redirect = source.resolve(location)
          val destination =
            if (rewriteLocation(
                proxy.target,
                source,
                proxy.prefix,
                location).nonEmpty) {
              Some(redirect)
            } else {
              resource.resolveProxy().flatMap(approvedProxyRedirect(
                _,
                redirect)).map { approved =>
                val proxyBase = new URI(
                  s"${approved.getScheme}://${approved.getRawAuthority}" +
                    s"${resource.applicationPath.get}/")
                proxy.proxyBase = Some(proxyBase)
                approved
              }
            }
          destination match {
            case Some(approved) =>
              result.getResponse.getHeaders.put("Location", approved.toString)
              super.onComplete(result)
            case None =>
              redirector.reject(
                result,
                new IOException("Application redirect is outside its target"))
          }
        } catch {
          case NonFatal(e) => redirector.reject(result, e)
        }
      }
    })
    client
  }

  override protected def copyRequestHeaders(
      clientRequest: HttpServletRequest,
      proxyRequest: Request): Unit = {
    super.copyRequestHeaders(clientRequest, proxyRequest)
    proxyRequest.asInstanceOf[HttpRequest].getConversation
      .setAttribute(CONTEXT_ATTRIBUTE, proxyContext(clientRequest))
    // Kyuubi credentials must never reach the application being proxied.
    Seq("Authorization", "Proxy-Authorization", "Cookie", "X-Forwarded-Context")
      .foreach(proxyRequest.getHeaders.remove)
    // Keep textual responses transformable even when browsers advertise Brotli support.
    proxyRequest.header("Accept-Encoding", "identity")
  }

  override def addXForwardedHeaders(
      clientRequest: HttpServletRequest,
      proxyRequest: Request): Unit = {
    super.addXForwardedHeaders(clientRequest, proxyRequest)
    proxyRequest.header(
      "X-Forwarded-Context",
      proxyContext(clientRequest).prefix)
  }

  override protected def newServerResponseContentTransformer(
      request: HttpServletRequest,
      proxyResponse: HttpServletResponse,
      response: Response): ContentTransformer = {
    val context = proxyContext(request)
    val base = context.target
    val resource = context.resource
    val applicationBase = resource.applicationPath.map { path =>
      new URI(s"${base.getScheme}://${base.getRawAuthority}$path/")
    }.getOrElse(base)
    val basePath = Option(applicationBase.getRawPath).getOrElse("").stripSuffix("/")
    val contentType = Option(response.getHeaders.get("Content-Type"))
      .getOrElse("").takeWhile(_ != ';').trim.toLowerCase(Locale.ROOT)
    if (basePath.isEmpty || !Set(
        "text/html",
        "application/xhtml+xml",
        "text/css",
        "text/javascript",
        "application/javascript").contains(contentType)) {
      return ContentTransformer.IDENTITY
    }
    val transformer = new ApplicationPathTransformer(
      applicationBase,
      proxyContext(request).prefix)
    Option(response.getHeaders.get("Content-Encoding")).map(_.toLowerCase(Locale.ROOT)) match {
      case None | Some("identity") => transformer
      case Some("gzip") => new GZIPContentTransformer(transformer)
      case _ => ContentTransformer.IDENTITY
    }
  }

  override protected def filterServerResponseHeader(
      request: HttpServletRequest,
      response: Response,
      name: String,
      value: String): String = {
    if (name.equalsIgnoreCase("Set-Cookie")) return null
    if (name.equalsIgnoreCase("Location")) {
      val context = proxyContext(request)
      return rewriteLocation(
        context.target,
        response.getRequest.getURI,
        context.prefix,
        value).orNull
    }
    super.filterServerResponseHeader(request, response, name, value)
  }
}

private[api] object EngineUIProxyServlet {
  private val CONTEXT_ATTRIBUTE = "kyuubi.engine.ui.context"

  private class ProxyContext(val resource: EngineUIProxyTarget, val prefix: String) {
    var proxyBase: Option[URI] = None
    def target: URI = proxyBase.getOrElse(resource.uri)
  }

  private def proxyContext(request: HttpServletRequest): ProxyContext =
    request.getAttribute(CONTEXT_ATTRIBUTE).asInstanceOf[ProxyContext]

  private class ApplicationRedirector(client: HttpClient) extends HttpRedirector(client) {
    def reject(result: Result, cause: Throwable): Unit =
      fail(result.getRequest, result.getResponse, cause)
  }

  private[api] def approvedProxyRedirect(proxy: URI, redirect: URI): Option[URI] = {
    val path = Option(proxy.getRawPath).getOrElse("").stripSuffix("/")
    val redirectPath = Option(redirect.getRawPath).getOrElse("")
    val alternate = path.replaceFirst("^/proxy/", "/proxy/redirect/")
    if (!path.matches("/proxy/application_[0-9]+_[0-9]+") ||
      proxy.getScheme != redirect.getScheme || proxy.getRawAuthority != redirect.getRawAuthority ||
      !safePath(redirectPath)) {
      return None
    }
    Seq(path, alternate).find(root => redirectPath == root || redirectPath.startsWith(root + "/"))
      .map { root =>
        new URI(s"${proxy.getScheme}://${proxy.getRawAuthority}$path" +
          redirectPath.stripPrefix(root) + Option(redirect.getRawQuery).map("?" + _).getOrElse(""))
      }
  }

  /** Rebase quoted application URLs without buffering an entire UI page or decoding its text. */
  private[api] class ApplicationPathTransformer(base: URI, prefix: String)
    extends ContentTransformer {
    private val basePath = Option(base.getRawPath).getOrElse("").stripSuffix("/")
    private val roots = Seq(basePath, s"${base.getScheme}://${base.getRawAuthority}$basePath")
    private val pattern = Pattern.compile(
      "[\"'(](?:" + roots.map(Pattern.quote).mkString("|") + ")(?=[/\"')?#])")
    private val retainedBytes = roots.map(_.length).max + 2
    private var pending = ""

    override def transform(
        input: ByteBuffer,
        finished: Boolean,
        output: java.util.List[ByteBuffer]): Unit = {
      val bytes = new Array[Byte](input.remaining())
      input.get(bytes)
      // ISO-8859-1 is a reversible byte mapping: split UTF-8 characters are left intact.
      val content = pending + new String(bytes, ISO_8859_1)
      val limit = if (finished) content.length else math.max(0, content.length - retainedBytes)
      val matcher = pattern.matcher(content)
      val result = new StringBuilder
      var consumed = 0
      while (matcher.find() && matcher.start() < limit) {
        result.append(content.substring(consumed, matcher.start() + 1))
        result.append(prefix)
        consumed = matcher.end()
      }
      val end = math.max(consumed, limit)
      result.append(content.substring(consumed, end))
      pending = content.substring(end)
      if (result.nonEmpty) output.add(ByteBuffer.wrap(result.toString.getBytes(ISO_8859_1)))
    }
  }

  private[api] def safePath(path: String): Boolean = {
    val lower = path.toLowerCase(Locale.ROOT)
    !path.contains("\\") && !path.contains(";") && !path.exists(_.isControl) &&
    !path.split("/", -1).exists(p => p == "." || p == "..") &&
    !Seq("%2e", "%2f", "%5c", "%25", "%00", "%3b").exists(lower.contains)
  }

  private[api] def parseRoute(uri: String): Option[(String, String, String)] = {
    val parts = uri.split("/", 5)
    if (parts.length < 4 || parts(0).nonEmpty || parts(1) != "engine-ui" ||
      !Set("engines", "batches").contains(parts(2)) ||
      !parts(3).matches("[A-Za-z0-9_-]+")) {
      return None
    }
    val path = if (parts.length == 5) "/" + parts(4) else ""
    if (safePath(path)) Some((parts(2), parts(3), path)) else None
  }

  private[api] def rewriteLocation(
      base: URI,
      source: URI,
      prefix: String,
      location: String): Option[String] = {
    Try(source.resolve(location)).toOption.filter { target =>
      target.getScheme == base.getScheme && target.getRawAuthority == base.getRawAuthority &&
      safePath(target.getRawPath) &&
      target.getRawPath.startsWith(Option(base.getRawPath).getOrElse("").stripSuffix("/") + "/")
    }.map { target =>
      val basePath = Option(base.getRawPath).getOrElse("").stripSuffix("/")
      prefix + target.getRawPath.stripPrefix(basePath) +
        Option(target.getRawQuery).map("?" + _).getOrElse("") +
        Option(target.getRawFragment).map("#" + _).getOrElse("")
    }
  }

  private def errorPage(
      title: String,
      message: String): String = {
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
       |    @media (max-width: 540px) {
       |      article { padding: 28px 24px 32px; }
       |      header { padding: 0 24px; }
       |      h1 { font-size: 25px; }
       |    }
       |  </style>
       |</head>
       |<body>
       |  <main>
       |    <header>APACHE KYUUBI</header>
       |    <article>
       |      <div class="status">403 / ACCESS DENIED</div>
       |      <h1>$title</h1>
       |      <p>$message</p>
       |    </article>
       |  </main>
       |</body>
       |</html>""".stripMargin
  }

  def apply(fe: KyuubiRestFrontendService): EngineUIProxyServlet = {
    val resolver = new EngineUIProxyTargetResolver(fe)
    new EngineUIProxyServlet(
      fe.getConf.get(KyuubiConf.FRONTEND_REST_ENGINE_UI_PROXY_ENABLED),
      resolver.resolveResource) {
      override def destroy(): Unit = {
        try super.destroy()
        finally resolver.close()
      }
    }
  }
}
