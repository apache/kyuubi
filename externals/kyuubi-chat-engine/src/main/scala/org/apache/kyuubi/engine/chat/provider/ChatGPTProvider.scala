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

package org.apache.kyuubi.engine.chat.provider

import java.util
import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._

import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}
import org.apache.http.{HttpHeaders, HttpHost, HttpStatus}
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.{ContentType, StringEntity}
import org.apache.http.impl.client.{CloseableHttpClient, HttpClientBuilder}
import org.apache.http.message.BasicHeader
import org.apache.http.util.EntityUtils

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.engine.chat.provider.ChatProvider.mapper

class ChatGPTProvider(conf: KyuubiConf) extends ChatProvider {

  private val gptApiKey = conf.get(KyuubiConf.ENGINE_CHAT_GPT_API_KEY).getOrElse {
    throw new IllegalArgumentException(
      s"'${KyuubiConf.ENGINE_CHAT_GPT_API_KEY.key}' must be configured, " +
        s"which could be got at https://platform.openai.com/account/api-keys")
  }

  private val httpClient: CloseableHttpClient = {
    HttpClientBuilder.create()
      .setDefaultHeaders(List(
        new BasicHeader(HttpHeaders.AUTHORIZATION, s"Bearer $gptApiKey")).asJava)
      .build()
  }

  private val requestConfig: RequestConfig = {
    val connectTimeout = conf.get(KyuubiConf.ENGINE_CHAT_GPT_HTTP_CONNECT_TIMEOUT).intValue()
    val socketTimeout = conf.get(KyuubiConf.ENGINE_CHAT_GPT_HTTP_SOCKET_TIMEOUT).intValue()
    val builder: RequestConfig.Builder = RequestConfig.custom()
      .setConnectTimeout(connectTimeout)
      .setSocketTimeout(socketTimeout)
    conf.get(KyuubiConf.ENGINE_CHAT_GPT_HTTP_PROXY).foreach { url =>
      builder.setProxy(HttpHost.create(url))
    }
    builder.build()
  }

  private val chatHistory: LoadingCache[String, util.ArrayDeque[Message]] =
    CacheBuilder.newBuilder()
      .expireAfterWrite(10, TimeUnit.MINUTES)
      .build(new CacheLoader[String, util.ArrayDeque[Message]] {
        override def load(sessionId: String): util.ArrayDeque[Message] =
          new util.ArrayDeque[Message]
      })

  override def open(sessionId: String): Unit = {
    chatHistory.getIfPresent(sessionId)
  }

  override def ask(sessionId: String, q: String): String = {
    val messages = chatHistory.get(sessionId)
    messages.addLast(Message("user", q))

    val request = new HttpPost("https://api.openai.com/v1/chat/completions")
    val req = Map(
      "messages" -> messages,
      "model" -> "gpt-3.5-turbo",
      "max_tokens" -> 200,
      "temperature" -> 0.5,
      "top_p" -> 1)
    val entity = new StringEntity(mapper.writeValueAsString(req), ContentType.APPLICATION_JSON)
    request.setEntity(entity)
    request.setConfig(requestConfig)
    val response = httpClient.execute(request)
    val respJson = mapper.readTree(EntityUtils.toString(response.getEntity))
    response.getStatusLine.getStatusCode match {
      case HttpStatus.SC_OK =>
        val replyMessage = mapper.treeToValue[Message](
          respJson.get("choices").get(0).get("message"))
        messages.addLast(replyMessage)
        replyMessage.content
      case errorStatusCode =>
        messages.removeLast()
        s"Chat failed. Status: $errorStatusCode. ${respJson.get("error").get("message").asText}"
    }
  }

  override def close(sessionId: String): Unit = {
    chatHistory.invalidate(sessionId)
  }
}
