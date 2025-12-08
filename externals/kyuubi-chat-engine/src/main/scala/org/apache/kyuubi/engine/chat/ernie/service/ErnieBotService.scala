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

package org.apache.kyuubi.engine.chat.ernie.service

import java.io.IOException
import java.time.Duration
import java.util.concurrent.TimeUnit

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper, PropertyNamingStrategies}
import io.reactivex.Single
import okhttp3.{ConnectionPool, OkHttpClient}
import retrofit2.{HttpException, Retrofit}
import retrofit2.adapter.rxjava2.RxJava2CallAdapterFactory
import retrofit2.converter.jackson.JacksonConverterFactory

import org.apache.kyuubi.engine.chat.api.{ApiHttpException, ErnieBotApi}
import org.apache.kyuubi.engine.chat.ernie.bean.{ChatCompletionRequest, ChatCompletionResult}

class ErnieBotService(api: ErnieBotApi) {

  def execute[T](apiCall: Single[T]): T = {
    try apiCall.blockingGet
    catch {
      case httpException: HttpException =>
        try if (httpException.response != null && httpException.response.errorBody != null) {
            val errorBody: String = httpException.response.errorBody.string
            val statusCode: Int = httpException.response.code
            throw new ApiHttpException(statusCode, errorBody, httpException)
          } else {
            throw httpException
          }
        catch {
          case ioException: IOException =>
            throw httpException
        }
    }
  }

  def createChatCompletion(
      request: ChatCompletionRequest,
      model: String,
      accessToken: String): ChatCompletionResult = {
    execute(this.api.createChatCompletion(model, accessToken, request))
  }
}

object ErnieBotService {
  final private val BASE_URL = "https://aip.baidubce.com/"

  def apply(api: ErnieBotApi): ErnieBotService = new ErnieBotService(api)

  def defaultObjectMapper: ObjectMapper = {
    val mapper: ObjectMapper = new ObjectMapper
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL)
    mapper.setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE)
    mapper
  }

  def defaultClient(timeout: Duration): OkHttpClient = {
    new OkHttpClient.Builder()
      .connectionPool(new ConnectionPool(5, 1, TimeUnit.SECONDS))
      .readTimeout(timeout.toMillis, TimeUnit.MILLISECONDS)
      .build
  }

  def defaultRetrofit(client: OkHttpClient, mapper: ObjectMapper): Retrofit = {
    new Retrofit.Builder().baseUrl(BASE_URL).client(client)
      .addConverterFactory(JacksonConverterFactory.create(mapper))
      .addCallAdapterFactory(RxJava2CallAdapterFactory.create)
      .build
  }

}
