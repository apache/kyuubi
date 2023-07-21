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

import java.net.{InetSocketAddress, Proxy, URL}
import java.time.Duration
import java.util
import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._

import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}
import com.theokanning.openai.OpenAiApi
import com.theokanning.openai.completion.chat.{ChatCompletionRequest, ChatMessage, ChatMessageRole}
import com.theokanning.openai.service.OpenAiService
import com.theokanning.openai.service.OpenAiService.{defaultClient, defaultObjectMapper, defaultRetrofit}

import org.apache.kyuubi.config.KyuubiConf

class ChatGPTProvider(conf: KyuubiConf) extends ChatProvider {

  private val gptApiKey = conf.get(KyuubiConf.ENGINE_CHAT_GPT_API_KEY).getOrElse {
    throw new IllegalArgumentException(
      s"'${KyuubiConf.ENGINE_CHAT_GPT_API_KEY.key}' must be configured, " +
        s"which could be got at https://platform.openai.com/account/api-keys")
  }

  private val openAiService: OpenAiService = {
    val builder = defaultClient(
      gptApiKey,
      Duration.ofMillis(conf.get(KyuubiConf.ENGINE_CHAT_GPT_HTTP_SOCKET_TIMEOUT)))
      .newBuilder
      .connectTimeout(Duration.ofMillis(conf.get(KyuubiConf.ENGINE_CHAT_GPT_HTTP_CONNECT_TIMEOUT)))

    conf.get(KyuubiConf.ENGINE_CHAT_GPT_HTTP_PROXY) match {
      case Some(httpProxyUrl) =>
        val url = new URL(httpProxyUrl)
        val proxy = new Proxy(Proxy.Type.HTTP, new InetSocketAddress(url.getHost, url.getPort))
        builder.proxy(proxy)
      case _ =>
    }

    val retrofit = defaultRetrofit(builder.build(), defaultObjectMapper)
    val api = retrofit.create(classOf[OpenAiApi])
    new OpenAiService(api)
  }

  private var sessionUser: Option[String] = None

  private val chatHistory: LoadingCache[String, util.ArrayDeque[ChatMessage]] =
    CacheBuilder.newBuilder()
      .expireAfterWrite(10, TimeUnit.MINUTES)
      .build(new CacheLoader[String, util.ArrayDeque[ChatMessage]] {
        override def load(sessionId: String): util.ArrayDeque[ChatMessage] =
          new util.ArrayDeque[ChatMessage]
      })

  override def open(sessionId: String, user: Option[String]): Unit = {
    sessionUser = user
    chatHistory.getIfPresent(sessionId)
  }

  override def ask(sessionId: String, q: String): String = {
    val messages = chatHistory.get(sessionId)
    try {
      messages.addLast(new ChatMessage(ChatMessageRole.USER.value(), q))
      val completionRequest = ChatCompletionRequest.builder()
        .model(conf.get(KyuubiConf.ENGINE_CHAT_GPT_MODEL))
        .messages(messages.asScala.toList.asJava)
        .user(sessionUser.orNull)
        .n(1)
        .build()
      val responseText = openAiService.createChatCompletion(completionRequest)
        .getChoices.get(0).getMessage.getContent
      responseText
    } catch {
      case e: Throwable =>
        messages.removeLast()
        s"Chat failed. Error: ${e.getMessage}"
    }
  }

  override def close(sessionId: String): Unit = {
    chatHistory.invalidate(sessionId)
  }
}
