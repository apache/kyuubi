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

import scala.beans.BeanProperty

import com.fasterxml.jackson.annotation.JsonProperty

case class ChatCompletionResult(
    @BeanProperty @JsonProperty("id") id: String,
    @BeanProperty @JsonProperty("object") `object`: String,
    @BeanProperty @JsonProperty("created") created: java.lang.Long,
    @BeanProperty @JsonProperty("sentence_id") sentenceId: java.lang.Long,
    @BeanProperty @JsonProperty("is_end") isEnd: Boolean,
    @BeanProperty @JsonProperty("is_truncated") isTruncated: Boolean,
    @BeanProperty @JsonProperty("finish_reason") finishReason: String,
    @BeanProperty @JsonProperty("search_info") searchInfo: SearchInfo,
    @BeanProperty @JsonProperty("result") result: String,
    @BeanProperty @JsonProperty("need_clear_history") needClearHistory: Boolean,
    @BeanProperty @JsonProperty("ban_round") banRound: java.lang.Long,
    @BeanProperty @JsonProperty("usage") usage: Usage,
    @BeanProperty @JsonProperty("function_call") functionCall: FunctionCall,
    @BeanProperty @JsonProperty("error_msg") errorMsg: String,
    @BeanProperty @JsonProperty("error_code") errorCode: java.lang.Long)
