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

package org.apache.kyuubi.engine.chat.ernie.bean

import java.lang.{Long => JLong}

import com.fasterxml.jackson.annotation.JsonProperty

case class ChatCompletionResult(
    @JsonProperty("id") id: String,
    @JsonProperty("object") obj: String,
    @JsonProperty("created") created: JLong,
    @JsonProperty("sentence_id") sentenceId: JLong,
    @JsonProperty("is_end") isEnd: Boolean,
    @JsonProperty("is_truncated") isTruncated: Boolean,
    @JsonProperty("finish_reason") finishReason: String,
    @JsonProperty("search_info") searchInfo: SearchInfo,
    @JsonProperty("result") result: String,
    @JsonProperty("need_clear_history") needClearHistory: Boolean,
    @JsonProperty("ban_round") banRound: JLong,
    @JsonProperty("usage") usage: Usage,
    @JsonProperty("function_call") functionCall: FunctionCall,
    @JsonProperty("error_msg") errorMsg: String,
    @JsonProperty("error_code") errorCode: JLong)
