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

package org.apache.kyuubi.events.handler

import java.net.URI
import java.security.cert.X509Certificate
import java.time.Duration

import org.apache.commons.lang3.StringUtils
import org.apache.http.{Header, HttpHeaders}
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods.{CloseableHttpResponse, HttpPost}
import org.apache.http.conn.ssl.{NoopHostnameVerifier, SSLConnectionSocketFactory, TrustStrategy}
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.{CloseableHttpClient, HttpClientBuilder}
import org.apache.http.message.BasicHeader
import org.apache.http.ssl.SSLContexts

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.{ConfigEntry, KyuubiConf}
import org.apache.kyuubi.events.KyuubiEvent

class HttpLoggingEventHandler(
    loggers: ConfigEntry[Seq[String]],
    kyuubiConf: KyuubiConf)
  extends EventHandler[KyuubiEvent] with Logging {

  private var ingestUri: URI = _
  private var retryCount: Integer = 0
  private var httpClient: CloseableHttpClient = _
  private var httpHeaders: List[Header] = _

  initialize(loggers, kyuubiConf) // init

  override def apply(event: KyuubiEvent): Unit = {
    val request = new HttpPost(ingestUri)
    if (httpHeaders.nonEmpty) request.setHeaders(httpHeaders.toArray)
    request.setHeader(HttpHeaders.CONTENT_TYPE, "application/json")
    request.setHeader(HttpHeaders.CONTENT_ENCODING, "UTF-8")
    request.setEntity(new StringEntity(event.toJson))

    attemptToSend(request, 0, event.eventType)
  }

  private def attemptToSend(request: HttpPost, attempt: Integer, eventType: String): Unit = {
    try {
      val response = httpClient.execute(request)
      if (shouldRetry(response)) {
        if (attempt < retryCount) {
          warn(
            s"Ingest server responded with code ${response.getStatusLine.getStatusCode}, " +
              s"will retry: [Event: $eventType, Attempt=${attempt + 1}/${retryCount + 1}, " +
              s"URL=${request.getURI.toString}]")
          attemptToSend(request, attempt + 1, eventType)
        } else {
          error(
            s"Ingest server responded with code ${response.getStatusLine.getStatusCode}, " +
              s"fatal error: [Event: $eventType, Attempt=${attempt + 1}/${retryCount + 1}, " +
              s"URL=${request.getURI.toString}]")
        }
      } else {
        debug(s"Event delivered successfully:" +
          s"[Event: $eventType, Attempt=${attempt + 1}/${retryCount + 1}, " +
          s"URL=${request.getURI.toString}]")
      }
    } catch {
      case e: Exception =>
        if (attempt < retryCount) {
          warn(
            s"Sending event caused an exception, will retry: [Event: $eventType, " +
              s"Attempt=${attempt + 1}/${retryCount + 1}, URL=${request.getURI.toString}]",
            e)
          attemptToSend(request, attempt + 1, eventType)
        } else {
          error(
            s"Error sending Http request: [Event: $eventType, " +
              s"Attempt=${attempt + 1}/${retryCount + 1}, URL=${request.getURI.toString}]",
            e)
        }
    }
  }

  private def shouldRetry(response: CloseableHttpResponse): Boolean = {
    val statusCode = response.getStatusLine.getStatusCode
    // 1XX Information, requests can't be split
    if (statusCode < 200) return false
    // 2XX - OK
    if (200 <= statusCode && statusCode < 300) return false
    // 3XX Redirects, not following redirects
    if (300 <= statusCode && statusCode <= 400) return false
    // 4XX - client error, no retry except 408 Request Timeout and 429 Too Many Requests
    if (400 <= statusCode && statusCode < 500 && statusCode != 408 && statusCode != 429) {
      return false
    }
    true
  }

  override def close(): Unit = {
    httpClient.close();
  }

  private def initialize(loggers: ConfigEntry[Seq[String]], kyuubiConf: KyuubiConf): Unit = {
    val loggersPrefix = StringUtils.substringBeforeLast(loggers.key, ".loggers")
    ingestUri = new URI(kyuubiConf.getOption(loggersPrefix + ".http.ingest.uri").get)
    retryCount =
      Integer.valueOf(kyuubiConf.getOption(loggersPrefix + ".http.retry.count").get)

    val connectTimeoutMills =
      Duration.parse(
        kyuubiConf.getOption(loggersPrefix + ".http.connect.timeout").get).toMillis.toInt
    val socketTimeoutMills =
      Duration.parse(
        kyuubiConf.getOption(loggersPrefix + ".http.socket.timeout").get).toMillis.toInt
    httpClient = buildHttpClient(connectTimeoutMills, socketTimeoutMills)

    val headers = kyuubiConf.getOption(loggersPrefix + ".http.headers")
    if (headers.isDefined) httpHeaders = StringUtils.split(headers.get, ",").map {
      header =>
        new BasicHeader(
          StringUtils.split(header, ":")(0).trim,
          StringUtils.split(header, ":")(1).trim)
    }.toList
  }

  private def buildHttpClient(
      connectTimeoutMills: Integer,
      socketTimeoutMills: Integer): CloseableHttpClient = {
    val requestConfig = RequestConfig.custom
      .setConnectTimeout(connectTimeoutMills)
      .setSocketTimeout(socketTimeoutMills)
      .build
    val acceptingTrustStrategy: TrustStrategy =
      (cert: Array[X509Certificate], authType: String) => true
    val sslContext = SSLContexts.custom.loadTrustMaterial(null, acceptingTrustStrategy).build
    val sslSocketFactory = new SSLConnectionSocketFactory(sslContext, NoopHostnameVerifier.INSTANCE)

    HttpClientBuilder.create
      .setDefaultRequestConfig(requestConfig)
      .setSSLSocketFactory(sslSocketFactory)
      .build
  }
}

object HttpLoggingEventHandler {
  def apply(loggers: ConfigEntry[Seq[String]], kyuubiConf: KyuubiConf): HttpLoggingEventHandler = {
    new HttpLoggingEventHandler(loggers, kyuubiConf)
  }
}
