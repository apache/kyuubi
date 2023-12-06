/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kyuubi.engine.chat.ernie.service;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.theokanning.openai.OpenAiHttpException;
import io.reactivex.Single;
import okhttp3.ConnectionPool;
import okhttp3.OkHttpClient;
import org.apache.kyuubi.engine.chat.ernie.api.*;
import org.apache.kyuubi.engine.chat.ernie.bean.ChatCompletionRequest;
import org.apache.kyuubi.engine.chat.ernie.bean.ChatCompletionResult;
import org.apache.kyuubi.engine.chat.ernie.constants.Model;
import retrofit2.HttpException;
import retrofit2.Retrofit;
import retrofit2.adapter.rxjava2.RxJava2CallAdapterFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class ErnieBotService {
    private static final String BASE_URL = "https://api.baidu.com/";
    private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(10L);
    private static final ObjectMapper mapper = defaultObjectMapper();
    private final ErnieBotApi api;

    public ErnieBotService(String token) {
        this(token, DEFAULT_TIMEOUT);
    }
    public ErnieBotService(String token, Duration timeout) {
        Objects.requireNonNull(token, "Token required");
        ObjectMapper mapper = defaultObjectMapper();
        OkHttpClient client = defaultClient(token, timeout);
        Retrofit retrofit = defaultRetrofit(client, mapper);

        this.api = retrofit.create(ErnieBotApi.class);
    }

    public ErnieBotService(ErnieBotApi api) {
        this.api = api;
    }


    public static <T> T execute(Single<T> apiCall) {
        try {
            return apiCall.blockingGet();
        } catch (HttpException var5) {
            HttpException e = var5;

            try {
                if (e.response() != null && e.response().errorBody() != null) {
                    String errorBody = e.response().errorBody().string();
                    ApiError error = mapper.readValue(errorBody, ApiError.class);
                    throw new ApiHttpException(error, e, e.code());
                } else {
                    throw e;
                }
            } catch (IOException var4) {
                throw var5;
            }
        }
    }

    public static ObjectMapper defaultObjectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        mapper.setPropertyNamingStrategy(PropertyNamingStrategy.SNAKE_CASE);
        return mapper;
    }

    public static OkHttpClient defaultClient(String token, Duration timeout) {
        return new OkHttpClient.Builder()
                .addInterceptor(new AuthenticationInterceptor(token))
                .connectionPool(new ConnectionPool(5, 1, TimeUnit.SECONDS))
                .readTimeout(timeout.toMillis(), TimeUnit.MILLISECONDS)
                .build();
    }

    public static Retrofit defaultRetrofit(OkHttpClient client, ObjectMapper mapper) {
        return new Retrofit.Builder()
                .baseUrl(BASE_URL)
                .client(client)
                .addConverterFactory(JacksonConverterFactory.create(mapper))
                .addCallAdapterFactory(RxJava2CallAdapterFactory.create())
                .build();
    }

    public ChatCompletionResult createChatCompletion(ChatCompletionRequest request, String model) {
        if (model.equals(Model.ERNIE_BOT_8k.value())) {
            return execute(this.api.createChatCompletion8k(request));
        }

        if (model.equals(Model.ERNIE_BOT_4.value())) {
            return execute(this.api.createChatCompletionPro(request));
        }

        if (model.equals(Model.ERNIE_BOT_TURBO.value())){
            return execute(this.api.createChatCompletionTurbo(request));
        }

        return execute(this.api.createChatCompletion(request));
    }

}
