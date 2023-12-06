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

package org.apache.kyuubi.engine.chat.ernie.api;


import io.reactivex.Single;
import org.apache.kyuubi.engine.chat.ernie.bean.ChatCompletionRequest;
import org.apache.kyuubi.engine.chat.ernie.bean.ChatCompletionResult;
import retrofit2.http.Body;
import retrofit2.http.POST;

public interface ErnieBotApi {

    @POST("/rpc/2.0/ai_custom/v1/wenxinworkshop/chat/completions_pro")

    Single<ChatCompletionResult> createChatCompletionPro(@Body ChatCompletionRequest var1);

    @POST("/rpc/2.0/ai_custom/v1/wenxinworkshop/chat/ernie_bot_8k")
    Single<ChatCompletionResult> createChatCompletion8k(@Body ChatCompletionRequest var1);
    @POST("/rpc/2.0/ai_custom/v1/wenxinworkshop/chat/completions")
    Single<ChatCompletionResult> createChatCompletion(@Body ChatCompletionRequest var1);

    @POST("/rpc/2.0/ai_custom/v1/wenxinworkshop/chat/eb-instant")
    Single<ChatCompletionResult> createChatCompletionTurbo(@Body ChatCompletionRequest var1);

}
